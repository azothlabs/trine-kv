#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::sync::Weak;
use std::{
    collections::{BTreeMap, BTreeSet, hash_map::RandomState},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, RwLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

// Foreground writes may hand pressure relief to a background worker when that
// worker already owns the maintenance guard. Give real filesystem work a
// bounded completion window; a scheduler-scale timeout (the former 2 ms) turns
// healthy flush latency into spurious RuntimeBusy errors.
const BACKGROUND_MAINTENANCE_PROGRESS_WAIT: Duration = Duration::from_secs(5);

use crate::{
    blob::{self, ValueRef},
    bucket::{Bucket, BucketName, BucketReader, DEFAULT_BUCKET_NAME},
    cache, compaction,
    error::{Error, Result},
    iterator::{Direction, Iter, LazyIter, ScanSelector, ScanSourceInput},
    lsm::{
        AsyncPointReadIo, CompactionInput as LsmCompactionInput,
        CompactionOutput as LsmCompactionOutput,
        CompactionTablePayload as LsmCompactionTablePayload, FlushInput as LsmFlushInput,
        LsmPointReadSnapshot, LsmTree,
    },
    manifest::{self, ManifestState, ManifestStore},
    object_store::{ObjectClient, ObjectStoreBackend, verify_object_client_contract_for_open},
    options::{
        BlobLevelMergePolicy, BucketOptions, DbOptions, DurabilityMode, FailOnCorruptionPolicy,
        FilterPolicy, HostStorageBackend, ObjectClientTrustMode, PrefixFilterPolicy, StorageMode,
        WriteOptions,
    },
    point_value::PointValue,
    recovery,
    runtime::{self, CancellationToken, Runtime, RuntimeTask},
    snapshot::{Snapshot, SnapshotTracker},
    stats::{
        BlobReadMetrics, CompactionLevelStats, CompactionSkip, CompactionSkipStats,
        CompactionTrigger, CompactionTriggerStats, DbStats, FatalWriteStopStats, FilterStats,
        LevelFilterStats, LevelStats, ScanWasteMetrics,
    },
    storage::{
        BlockingStorageDirectoryCreateBackend, BlockingStorageDirectoryListBackend,
        BlockingStorageDirectorySyncBackend, BlockingStorageObjectDeleteBackend,
        BlockingStorageReadBackend, BlockingStorageReadObject, NativeFileBackend,
        StorageCapability, StorageDirectoryCreateBackend, StorageDirectoryFile, StorageDirectoryId,
        StorageDirectoryListBackend, StorageDirectorySyncBackend, StorageManifestReadBackend,
        StorageObjectDeleteBackend, StorageObjectId, StorageObjectKind, StorageObjectListBackend,
        StorageObjectReadBackend, StorageReadBackend,
    },
    substrate::{
        DurabilitySubstrate, FilesystemSubstrate, ObjectLeaseState, ObjectStoreSubstrate,
        ObjectWriterLease,
    },
    table::{self, Table},
    transaction::{Transaction, TransactionOptions},
    types::{CommitInfo, KeyRange, ReadVersion, Sequence, Value},
    wal::{self, WalBatch, WalFrontDoor},
    write_batch::BatchOperation,
};

mod async_api;
mod commit;
mod content;
mod engine;
mod open_helpers;
mod storage_backend;

use open_helpers::{
    cleanup_pending_obsolete_blob_files, cleanup_pending_obsolete_table_files, lock_poisoned,
};
use storage_backend::DatabaseStorageRef;

#[cfg(test)]
mod tests;

/// Database handle for reads, writes, snapshots, buckets, and maintenance.
#[derive(Debug)]
pub struct Db {
    inner: Arc<DbInner>,
    counts_as_user_handle: bool,
}

/// Converts common open inputs into `DbOptions`.
pub trait IntoOpenOptions {
    /// Converts this value into database open options.
    fn into_open_options(self) -> DbOptions;
}

impl IntoOpenOptions for DbOptions {
    fn into_open_options(self) -> DbOptions {
        self
    }
}

impl<P> IntoOpenOptions for P
where
    P: AsRef<Path>,
{
    fn into_open_options(self) -> DbOptions {
        DbOptions::new(self.as_ref())
    }
}

/// Cooperative maintenance work budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceBudget {
    max_flush_inputs: usize,
    max_compaction_inputs: usize,
}

impl MaintenanceBudget {
    /// Default number of immutable memtables to flush per maintenance call.
    pub const DEFAULT_MAX_FLUSH_INPUTS: usize = 1;
    /// Default number of compaction inputs to process per maintenance call.
    pub const DEFAULT_MAX_COMPACTION_INPUTS: usize = 1;

    /// Creates a maintenance budget, treating zero limits as one.
    #[must_use]
    pub const fn new(max_flush_inputs: usize, max_compaction_inputs: usize) -> Self {
        let max_flush_inputs = if max_flush_inputs == 0 {
            1
        } else {
            max_flush_inputs
        };
        let max_compaction_inputs = if max_compaction_inputs == 0 {
            1
        } else {
            max_compaction_inputs
        };
        Self {
            max_flush_inputs,
            max_compaction_inputs,
        }
    }

    /// Creates the default one-unit maintenance budget.
    #[must_use]
    pub const fn single_unit() -> Self {
        Self::new(
            Self::DEFAULT_MAX_FLUSH_INPUTS,
            Self::DEFAULT_MAX_COMPACTION_INPUTS,
        )
    }

    /// Creates a budget that does not intentionally limit maintenance inputs.
    #[must_use]
    pub const fn unbounded() -> Self {
        Self::new(usize::MAX, usize::MAX)
    }

    /// Returns the maximum number of flush inputs.
    #[must_use]
    pub const fn max_flush_inputs(self) -> usize {
        self.max_flush_inputs
    }

    /// Returns the maximum number of compaction inputs.
    #[must_use]
    pub const fn max_compaction_inputs(self) -> usize {
        self.max_compaction_inputs
    }

    fn flush_input_limit(self) -> usize {
        self.max_flush_inputs.max(1)
    }

    fn compaction_input_limit(self) -> usize {
        self.max_compaction_inputs.max(1)
    }
}

impl Default for MaintenanceBudget {
    fn default() -> Self {
        Self::single_unit()
    }
}

/// Result of a cooperative maintenance call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceOutcome {
    /// Number of flushes completed.
    pub flushes: usize,
    /// Number of compactions completed.
    pub compactions: usize,
    /// Whether the supplied budget stopped more available work.
    pub budget_exhausted: bool,
    /// Whether maintenance was already running elsewhere.
    pub busy: bool,
}

mod commit_tracker;
mod lifecycle;
mod maintenance_state;
mod state;

use commit_tracker::{
    CommitSlot, CommitTracker, PublishActivityGuard, PublishBarrier, PublishSequenceGuard,
};
use lifecycle::release_browser_writer_lease;
#[cfg(test)]
use maintenance_state::compaction_reservations_conflict;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use maintenance_state::record_maintenance_success;
use maintenance_state::{
    BlobGcCandidate, BlobGcRewritePlan, BlobGcRewriteRecord, BlobGcRewriteTable,
    CompactionReservation, MaintenanceCompactionGuard, MaintenanceCoordinator, MaintenanceRequest,
    NamedCompactionInput, NamedCompactionOutput, NamedFlushInput, PendingCompactionOutputs,
    WritePressure, shutdown_background_workers,
};
pub(crate) use state::DbInner;
pub(in crate::db) use state::FatalWriteStopReason;
use state::PersistentOpenParts;

impl MaintenanceOutcome {
    /// Returns `true` when at least one flush or compaction completed.
    #[must_use]
    pub const fn made_progress(self) -> bool {
        self.flushes != 0 || self.compactions != 0
    }

    /// Returns whether the supplied budget stopped more available work.
    #[must_use]
    pub const fn budget_exhausted(self) -> bool {
        self.budget_exhausted
    }

    /// Returns whether maintenance was already running elsewhere.
    #[must_use]
    pub const fn busy(self) -> bool {
        self.busy
    }

    fn busy_outcome() -> Self {
        Self {
            busy: true,
            ..Self::default()
        }
    }

    fn permits_follow_up_compaction(self) -> bool {
        !self.busy && !self.budget_exhausted
    }

    fn add_assign(&mut self, other: Self) {
        self.flushes = self.flushes.saturating_add(other.flushes);
        self.compactions = self.compactions.saturating_add(other.compactions);
        self.budget_exhausted |= other.budget_exhausted;
        self.busy |= other.busy;
    }
}
