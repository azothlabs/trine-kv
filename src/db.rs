use std::{
    collections::{BTreeMap, BTreeSet, hash_map::RandomState},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[cfg(any(not(target_os = "wasi"), test))]
use std::task::{Context, Poll, Waker};

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
        CompactionTrigger, CompactionTriggerStats, DbStats, FilterStats, LevelFilterStats,
        LevelStats, ScanWasteMetrics,
    },
    storage::{
        BlockingStorageDirectoryCreateBackend, BlockingStorageDirectoryListBackend,
        BlockingStorageDirectorySyncBackend, BlockingStorageObjectDeleteBackend,
        BlockingStorageReadBackend, BlockingStorageReadObject, MemoryStorageBackend,
        NativeFileBackend, StorageCapability, StorageDirectoryCreateBackend, StorageDirectoryFile,
        StorageDirectoryId, StorageDirectoryListBackend, StorageDirectorySyncBackend,
        StorageManifestReadBackend, StorageObjectDeleteBackend, StorageObjectId, StorageObjectKind,
        StorageObjectListBackend, StorageObjectReadBackend, StorageReadBackend,
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

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::{
    storage::{BrowserStorageBackend, BrowserWriterLease},
    wal::BrowserWalFrontDoor,
};

mod async_api;
mod commit;
mod content;
mod open_helpers;
mod sync_api;

use open_helpers::{
    cleanup_pending_obsolete_blob_files, cleanup_pending_obsolete_table_files, lock_poisoned,
    persistent_path_from_options,
};

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

#[derive(Debug)]
pub(crate) struct DbInner {
    options: DbOptions,
    user_handles: AtomicUsize,
    commit_tracker: CommitTracker,
    closed: AtomicBool,
    publish_barrier: PublishBarrier,
    /// Serializes object-store commit-slot reservation with the corresponding
    /// remote-WAL handoff. The remote WAL represents gaps as empty commits, so
    /// accepting N+1 before N would make N unrecoverable. This lock is never
    /// held across an `.await`.
    object_wal_commit_order: Mutex<()>,
    memtable_publish_lock: Mutex<()>,
    buckets: RwLock<BTreeMap<String, Arc<LsmTree>>>,
    snapshots: Arc<SnapshotTracker>,
    checkpoints: Mutex<BTreeMap<String, Sequence>>,
    // Obsolete table handles awaiting file deletion. Holding the `Arc<Table>`
    // (not just the id) lets cleanup delete a file only once no in-flight reader
    // still pins it (`Arc::strong_count == 1`), instead of blocking all deletion
    // while any snapshot is open. See `.phrase/protocol/snapshot-version-pinning.md`.
    pending_obsolete_tables: Mutex<Vec<Arc<Table>>>,
    manifest: Option<Mutex<ManifestStore>>,
    // The write-ahead log + single-writer lease behind the Band 3 durability
    // substrate (see `src/substrate.rs`). The native/WASI persistent path holds a
    // filesystem WAL + LOCK lease here; in-memory and the browser path hold an
    // inert substrate (browser durability still rides the `browser_*` fields).
    substrate: DurabilitySubstrate,
    block_cache: Arc<cache::BlockCache>,
    compaction_runs: AtomicU64,
    compaction_input_tables: AtomicU64,
    compaction_output_tables: AtomicU64,
    compaction_input_bytes: AtomicU64,
    compaction_output_bytes: AtomicU64,
    compaction_level_stats: Mutex<BTreeMap<u32, CompactionLevelStats>>,
    compaction_trigger_stats: Mutex<BTreeMap<CompactionTrigger, CompactionTriggerStats>>,
    compaction_skip_stats: Mutex<BTreeMap<CompactionSkip, CompactionSkipStats>>,
    blob_gc_runs: AtomicU64,
    blob_gc_input_bytes: AtomicU64,
    blob_gc_output_bytes: AtomicU64,
    blob_gc_discarded_bytes: AtomicU64,
    blob_reads: Arc<BlobReadMetrics>,
    scan_waste: Arc<ScanWasteMetrics>,
    maintenance_cooperative_yields: AtomicU64,
    maintenance_budget_exhaustions: AtomicU64,
    native_storage: NativeFileBackend,
    /// Private object registry used only by `ContentObject` chunks and
    /// descriptors in an in-memory database.
    content_memory: MemoryStorageBackend,
    /// Serializes descriptor publication and same-ContentId deduplication.
    content_seal_lock: futures::lock::Mutex<()>,
    /// Serializes the irreversible leased-only barrier transition in one writer.
    content_access_lock: futures::lock::Mutex<()>,
    /// Per-database keyed hash state for bounded content lock sharding.
    content_lock_hasher: RandomState,
    /// Serializes physical-quota counter transitions per `StorageDomain` shard.
    content_quota_locks: [futures::lock::Mutex<()>; 256],
    /// Bounded lock shards serialize state transitions for one `UploadId` without
    /// retaining a lock object for every historical upload.
    content_upload_locks: [futures::lock::Mutex<()>; 256],
    /// Object-storage byte backend for object-store databases (async-only),
    /// mirroring `browser_storage`. `None` for every other backend; when set,
    /// `native_storage` is an unused default and `substrate` is `ObjectStore`.
    object_storage: Option<ObjectStoreBackend>,
    /// Optional low-latency object backend for the object-store writer lease and
    /// remote WAL. Defaults to `object_storage` when callers do not provide a
    /// separate WAL tier.
    object_wal_storage: Option<ObjectStoreBackend>,
    /// Key prefix for an object-store database, used as the `db_path` for all of
    /// its object keys. For split-tier opens the same prefix is used in both
    /// clients: storage-tier keys include `<prefix>/MANIFEST` and tables, while
    /// WAL-tier keys include `<prefix>/LOCK` and remote WAL objects. Empty
    /// (bucket root) for the default open and for every other backend.
    object_storage_prefix: PathBuf,
    /// Serializes object-store manifest publishes so the CAS clone -> commit ->
    /// write-back stays atomic without holding the (std) manifest mutex across
    /// the await. Mirrors `browser_manifest_async_lock`; unused by other backends.
    object_manifest_async_lock: futures::lock::Mutex<()>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    browser_storage: Option<BrowserStorageBackend>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    #[allow(dead_code)]
    browser_writer_lease: Mutex<Option<BrowserWriterLease>>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    browser_wal: Option<BrowserWalFrontDoor>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    browser_manifest_async_lock: futures::lock::Mutex<()>,
    runtime: Runtime,
    runtime_shutdown: CancellationToken,
    maintenance: Arc<MaintenanceCoordinator>,
    background_workers: Mutex<Vec<RuntimeTask>>,
}

#[derive(Debug)]
struct PersistentOpenParts {
    options: DbOptions,
    runtime: Runtime,
    native_storage: NativeFileBackend,
    process_lock: Option<recovery::ProcessLock>,
    buckets: BTreeMap<String, Arc<LsmTree>>,
    manifest: ManifestStore,
    wal: Option<WalFrontDoor>,
    batches: Vec<WalBatch>,
    replay_floor: Sequence,
    db_path_for_cleanup: PathBuf,
}

#[derive(Debug)]
pub(super) struct CommitTracker {
    last_reserved_sequence: AtomicU64,
    visible_sequence: AtomicU64,
    skipped_slots: AtomicU64,
    slots: Mutex<BTreeMap<u64, CommitSlotState>>,
    visible_changed: Condvar,
    #[cfg(any(not(target_os = "wasi"), test))]
    visible_wakers: Mutex<Vec<Waker>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CommitSlot {
    sequence: Sequence,
}

#[derive(Debug)]
pub(super) struct PublishBarrier {
    sequence_lock: Mutex<()>,
    activity: Mutex<PublishBarrierActivity>,
    idle: Condvar,
}

#[derive(Debug)]
pub(super) struct PublishBarrierGuard<'barrier> {
    _activity: PublishActivityGuard<'barrier>,
    _sequence: PublishSequenceGuard<'barrier>,
}

#[derive(Debug)]
pub(crate) struct PublishActivityGuard<'barrier> {
    barrier: &'barrier PublishBarrier,
}

#[derive(Debug)]
pub(super) struct PublishSequenceGuard<'barrier> {
    _guard: MutexGuard<'barrier, ()>,
}

#[derive(Debug, Default)]
struct PublishBarrierActivity {
    active: usize,
    closing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitSlotState {
    Open,
    Visible,
    Skipped,
}

impl CommitTracker {
    fn new(visible_sequence: Sequence) -> Self {
        Self {
            last_reserved_sequence: AtomicU64::new(visible_sequence.get()),
            visible_sequence: AtomicU64::new(visible_sequence.get()),
            skipped_slots: AtomicU64::new(0),
            slots: Mutex::new(BTreeMap::new()),
            visible_changed: Condvar::new(),
            #[cfg(any(not(target_os = "wasi"), test))]
            visible_wakers: Mutex::new(Vec::new()),
        }
    }

    #[must_use]
    fn visible_sequence(&self) -> Sequence {
        Sequence::new(self.visible_sequence.load(Ordering::Acquire))
    }

    fn reset_visible_boundary(&self, visible_sequence: Sequence) -> Result<()> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        slots.clear();
        self.visible_sequence
            .store(visible_sequence.get(), Ordering::Release);
        self.last_reserved_sequence
            .store(visible_sequence.get(), Ordering::Release);
        self.skipped_slots.store(0, Ordering::Release);
        Ok(())
    }

    fn last_reserved_sequence(&self) -> Sequence {
        Sequence::new(self.last_reserved_sequence.load(Ordering::Acquire))
    }

    fn open_slot_count(&self) -> usize {
        self.slots.lock().map_or(0, |slots| {
            slots
                .values()
                .filter(|state| **state == CommitSlotState::Open)
                .count()
        })
    }

    fn skipped_slot_count(&self) -> u64 {
        self.skipped_slots.load(Ordering::Acquire)
    }

    pub(super) fn reserve_slot(&self) -> Result<CommitSlot> {
        let reserved = self
            .last_reserved_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| Error::Corruption {
                message: "sequence counter overflow".to_owned(),
            })?
            .checked_add(1)
            .ok_or_else(|| Error::Corruption {
                message: "sequence counter overflow".to_owned(),
            })?;
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        if slots.insert(reserved, CommitSlotState::Open).is_some() {
            return Err(Error::Corruption {
                message: format!("commit slot {reserved} was reserved twice"),
            });
        }
        Ok(CommitSlot {
            sequence: Sequence::new(reserved),
        })
    }

    pub(super) fn mark_visible(&self, slot: CommitSlot) -> Result<()> {
        self.mark_terminal(slot, CommitSlotState::Visible)
    }

    pub(super) fn mark_skipped(&self, slot: CommitSlot) -> Result<()> {
        self.mark_terminal(slot, CommitSlotState::Skipped)
    }

    fn mark_terminal(&self, slot: CommitSlot, terminal_state: CommitSlotState) -> Result<()> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        let state = slots
            .get_mut(&slot.sequence.get())
            .ok_or_else(|| Error::Corruption {
                message: format!("commit slot {} is missing", slot.sequence.get()),
            })?;
        match *state {
            CommitSlotState::Open => {
                *state = terminal_state;
                if terminal_state == CommitSlotState::Skipped {
                    self.skipped_slots.fetch_add(1, Ordering::AcqRel);
                }
                let advanced = self.advance_visible_sequence(&mut slots);
                drop(slots);
                if advanced {
                    self.notify_visible_waiters();
                }
                Ok(())
            }
            CommitSlotState::Visible | CommitSlotState::Skipped => Err(Error::Corruption {
                message: format!("commit slot {} is already terminal", slot.sequence.get()),
            }),
        }
    }

    fn advance_visible_sequence(&self, slots: &mut BTreeMap<u64, CommitSlotState>) -> bool {
        let mut visible = self.visible_sequence.load(Ordering::Acquire);
        let previous = visible;
        while let Some(next) = visible.checked_add(1) {
            match slots.get(&next).copied() {
                Some(CommitSlotState::Visible | CommitSlotState::Skipped) => {
                    slots.remove(&next);
                    visible = next;
                    self.visible_sequence.store(visible, Ordering::Release);
                }
                Some(CommitSlotState::Open) | None => break,
            }
        }
        visible != previous
    }

    fn wait_until_visible(&self, sequence: Sequence) -> Result<()> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        while self.visible_sequence().get() < sequence.get() {
            slots = self
                .visible_changed
                .wait(slots)
                .map_err(|_| lock_poisoned("commit tracker visible wait"))?;
        }
        Ok(())
    }

    #[cfg(any(not(target_os = "wasi"), test))]
    async fn wait_until_visible_async(&self, sequence: Sequence) -> Result<()> {
        std::future::poll_fn(|context| self.poll_until_visible(sequence, context)).await
    }

    #[cfg(any(not(target_os = "wasi"), test))]
    fn poll_until_visible(&self, sequence: Sequence, context: &Context<'_>) -> Poll<Result<()>> {
        if self.visible_sequence().get() >= sequence.get() {
            return Poll::Ready(Ok(()));
        }

        let mut wakers = self
            .visible_wakers
            .lock()
            .map_err(|_| lock_poisoned("commit tracker visible wakers"))?;
        if self.visible_sequence().get() >= sequence.get() {
            return Poll::Ready(Ok(()));
        }
        if !wakers
            .iter()
            .any(|registered| registered.will_wake(context.waker()))
        {
            wakers.push(context.waker().clone());
        }
        Poll::Pending
    }

    fn notify_visible_waiters(&self) {
        self.visible_changed.notify_all();
        #[cfg(any(not(target_os = "wasi"), test))]
        {
            let wakers = {
                let mut wakers = self
                    .visible_wakers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                std::mem::take(&mut *wakers)
            };
            for waker in wakers {
                waker.wake();
            }
        }
    }
}

impl PublishBarrier {
    fn new() -> Self {
        Self {
            sequence_lock: Mutex::new(()),
            activity: Mutex::new(PublishBarrierActivity::default()),
            idle: Condvar::new(),
        }
    }

    pub(super) fn enter(&self) -> Result<PublishBarrierGuard<'_>> {
        let activity = self.begin_activity()?;
        match self.enter_sequence() {
            Ok(sequence) => Ok(PublishBarrierGuard {
                _activity: activity,
                _sequence: sequence,
            }),
            Err(error) => {
                drop(activity);
                Err(error)
            }
        }
    }

    pub(super) fn begin_activity(&self) -> Result<PublishActivityGuard<'_>> {
        let mut activity = self
            .activity
            .lock()
            .map_err(|_| lock_poisoned("publish activity"))?;
        if activity.closing {
            return Err(Error::Closed);
        }
        activity.active = activity
            .active
            .checked_add(1)
            .ok_or_else(|| Error::Corruption {
                message: "publish activity counter overflow".to_owned(),
            })?;
        Ok(PublishActivityGuard { barrier: self })
    }

    pub(super) fn enter_sequence(&self) -> Result<PublishSequenceGuard<'_>> {
        self.sequence_lock
            .lock()
            .map(|guard| PublishSequenceGuard { _guard: guard })
            .map_err(|_| lock_poisoned("publish sequence barrier"))
    }

    fn close(&self) -> Result<()> {
        let mut activity = self
            .activity
            .lock()
            .map_err(|_| lock_poisoned("publish activity"))?;
        activity.closing = true;
        while activity.active != 0 {
            activity = self
                .idle
                .wait(activity)
                .map_err(|_| lock_poisoned("publish activity"))?;
        }
        Ok(())
    }
}

impl Drop for PublishActivityGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut activity) = self.barrier.activity.lock() {
            if activity.active == 0 {
                debug_assert!(false, "publish activity guard count underflow");
                return;
            }
            activity.active -= 1;
            if activity.active == 0 {
                self.barrier.idle.notify_all();
            }
        }
    }
}

impl CommitSlot {
    #[must_use]
    pub(super) const fn sequence(self) -> Sequence {
        self.sequence
    }
}

struct NamedFlushInput {
    bucket: String,
    tree: Arc<LsmTree>,
    input: LsmFlushInput,
}

struct NamedCompactionInput {
    bucket: String,
    tree: Arc<LsmTree>,
    input: LsmCompactionInput,
}

struct NamedCompactionOutput {
    bucket: String,
    trigger: Option<CompactionTrigger>,
    output: LsmCompactionOutput,
}

struct PendingCompactionOutputs {
    outputs: Vec<NamedCompactionOutput>,
    written_table_ids: Vec<table::TableId>,
}

struct BlobGcCandidate {
    file_id: u64,
    total_bytes: u64,
    live_bytes: u64,
}

struct BlobGcRewriteTable {
    bucket: String,
    input_table_id: table::TableId,
    output_table_id: table::TableId,
    level: table::TableLevel,
    options: table::TableWriteOptions,
    point_records: Vec<table::TablePointRecord>,
    range_tombstones: Vec<table::TableRangeTombstone>,
}

struct BlobGcRewriteRecord {
    internal_key: crate::internal_key::InternalKey,
    value: Vec<u8>,
    compression: crate::codec::CodecId,
    table_index: usize,
    record_index: usize,
}

struct BlobGcRewritePlan {
    candidates: Vec<BlobGcCandidate>,
    new_blob_file_id: u64,
    tables: Vec<BlobGcRewriteTable>,
    records: Vec<BlobGcRewriteRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MaintenanceRequest {
    flush: bool,
    compaction: bool,
}

impl MaintenanceRequest {
    #[must_use]
    const fn any(self) -> bool {
        self.flush || self.compaction
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WritePressure {
    flush: bool,
    compaction: bool,
}

impl WritePressure {
    #[must_use]
    const fn none(self) -> bool {
        !self.flush && !self.compaction
    }

    #[must_use]
    const fn request(self) -> MaintenanceRequest {
        MaintenanceRequest {
            flush: self.flush,
            compaction: self.compaction,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactionReservation {
    bucket: String,
    range: KeyRange,
}

#[derive(Debug)]
struct MaintenanceCoordinator {
    state: Mutex<MaintenanceState>,
    wake: Condvar,
}

#[derive(Debug, Default)]
struct MaintenanceState {
    flush_requests: usize,
    compaction_requests: usize,
    active_flushes: usize,
    active_compactions: Vec<CompactionReservation>,
    progress: u64,
    shutdown: bool,
    last_error: Option<Error>,
}

#[derive(Debug)]
struct MaintenanceFlushGuard {
    coordinator: Arc<MaintenanceCoordinator>,
}

#[derive(Debug)]
struct MaintenanceCompactionGuard {
    coordinator: Arc<MaintenanceCoordinator>,
    reservations: Vec<CompactionReservation>,
}

impl MaintenanceCoordinator {
    fn new() -> Self {
        Self {
            state: Mutex::new(MaintenanceState::default()),
            wake: Condvar::new(),
        }
    }

    fn request(&self, request: MaintenanceRequest) {
        if !request.any() {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            if request.flush {
                state.flush_requests = state.flush_requests.saturating_add(1);
            }
            if request.compaction {
                state.compaction_requests = state.compaction_requests.saturating_add(1);
            }
            self.wake.notify_all();
        }
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    fn wait_for_request(&self) -> Option<MaintenanceRequest> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        while state.flush_requests == 0 && state.compaction_requests == 0 && !state.shutdown {
            let Ok(next_state) = self.wake.wait(state) else {
                return None;
            };
            state = next_state;
        }
        if state.shutdown {
            return None;
        }
        let request = MaintenanceRequest {
            flush: state.flush_requests != 0,
            compaction: state.compaction_requests != 0,
        };
        state.flush_requests = 0;
        state.compaction_requests = 0;
        self.wake.notify_all();
        Some(request)
    }

    fn progress(&self) -> u64 {
        self.state.lock().map_or(0, |state| state.progress)
    }

    fn wait_for_progress(&self, observed_progress: u64, timeout: Duration) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        while state.progress == observed_progress && !state.shutdown && state.last_error.is_none() {
            let Ok((next_state, wait_result)) = self.wake.wait_timeout(state, timeout) else {
                return false;
            };
            state = next_state;
            if wait_result.timed_out() {
                break;
            }
        }
        state.progress != observed_progress || state.shutdown || state.last_error.is_some()
    }

    fn wait_until_idle(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while state.active_flushes != 0 || !state.active_compactions.is_empty() {
            let Ok(next_state) = self.wake.wait(state) else {
                return;
            };
            state = next_state;
        }
    }

    fn wait_until_flush_idle(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while (state.flush_requests != 0 || state.active_flushes != 0)
            && !state.shutdown
            && state.last_error.is_none()
        {
            let Ok(next_state) = self.wake.wait(state) else {
                return;
            };
            state = next_state;
        }
    }

    fn wait_until_compaction_idle(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while (state.compaction_requests != 0 || !state.active_compactions.is_empty())
            && !state.shutdown
            && state.last_error.is_none()
        {
            let Ok(next_state) = self.wake.wait(state) else {
                return;
            };
            state = next_state;
        }
    }

    fn has_pending_compaction(&self) -> bool {
        self.state.lock().is_ok_and(|state| {
            state.compaction_requests != 0 || !state.active_compactions.is_empty()
        })
    }

    fn try_start_flush(self: &Arc<Self>) -> Option<MaintenanceFlushGuard> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        if state.shutdown || state.active_flushes != 0 || !state.active_compactions.is_empty() {
            return None;
        }
        state.active_flushes = 1;
        Some(MaintenanceFlushGuard {
            coordinator: Arc::clone(self),
        })
    }

    fn reserve_compactions(
        self: &Arc<Self>,
        candidates: Vec<CompactionReservation>,
    ) -> Option<MaintenanceCompactionGuard> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        if state.shutdown || state.active_flushes != 0 {
            return None;
        }

        let mut reservations = Vec::new();
        for candidate in candidates {
            if state
                .active_compactions
                .iter()
                .any(|active| compaction_reservations_conflict(active, &candidate))
            {
                continue;
            }
            state.active_compactions.push(candidate.clone());
            reservations.push(candidate);
        }

        if reservations.is_empty() {
            return None;
        }

        Some(MaintenanceCompactionGuard {
            coordinator: Arc::clone(self),
            reservations,
        })
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    fn record_error(&self, error: Error) {
        if let Ok(mut state) = self.state.lock() {
            state.last_error = Some(error);
            state.progress = state.progress.saturating_add(1);
            self.wake.notify_all();
        }
    }

    fn take_error(&self) -> Option<Error> {
        self.state
            .lock()
            .ok()
            .and_then(|mut state| state.last_error.take())
    }

    fn shutdown(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.shutdown = true;
            self.wake.notify_all();
        }
    }

    fn finish_flush(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.active_flushes = state.active_flushes.saturating_sub(1);
            state.progress = state.progress.saturating_add(1);
            self.wake.notify_all();
        }
    }

    fn finish_compactions(&self, reservations: &[CompactionReservation]) {
        if let Ok(mut state) = self.state.lock() {
            state
                .active_compactions
                .retain(|active| !reservations.iter().any(|finished| finished == active));
            state.progress = state.progress.saturating_add(1);
            self.wake.notify_all();
        }
    }
}

impl Drop for MaintenanceFlushGuard {
    fn drop(&mut self) {
        self.coordinator.finish_flush();
    }
}

impl Drop for MaintenanceCompactionGuard {
    fn drop(&mut self) {
        self.coordinator.finish_compactions(&self.reservations);
    }
}

impl MaintenanceCompactionGuard {
    fn contains(&self, bucket: &str, range: &KeyRange) -> bool {
        self.reservations
            .iter()
            .any(|reservation| reservation.bucket == bucket && reservation.range == *range)
    }
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
fn record_maintenance_success(_maintenance: &MaintenanceCoordinator) {
    // A later successful maintenance pass must not hide a failure that no
    // caller has observed yet. `take_error` is the only path that clears it.
}

fn compaction_reservations_conflict(
    left: &CompactionReservation,
    right: &CompactionReservation,
) -> bool {
    // Every compaction and blob-GC rewrite replaces tables in one bucket's
    // current LSM version. Serializing that replacement boundary prevents two
    // independently planned outputs from installing incompatible views even
    // when their requested key ranges appeared disjoint before either publish.
    left.bucket == right.bucket
}

fn shutdown_background_workers(
    maintenance: &Arc<MaintenanceCoordinator>,
    runtime_shutdown: &CancellationToken,
    workers: &Mutex<Vec<RuntimeTask>>,
) {
    runtime_shutdown.cancel();
    maintenance.shutdown();
    let workers = workers
        .lock()
        .map(|mut workers| std::mem::take(&mut *workers))
        .unwrap_or_default();

    for worker in workers {
        if worker.is_current_thread() {
            continue;
        }
        let _ = worker.join();
    }
    maintenance.wait_until_idle();
}

fn release_browser_writer_lease(inner: &DbInner) {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        let Ok(mut lease) = inner.browser_writer_lease.lock() else {
            return;
        };
        let _ = lease.take();
    }
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    let _ = inner;
}

impl Drop for DbInner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        shutdown_background_workers(
            &self.maintenance,
            &self.runtime_shutdown,
            &self.background_workers,
        );
        let _ = cleanup_pending_obsolete_table_files(
            &self.native_storage,
            persistent_path_from_options(&self.options),
            &self.pending_obsolete_tables,
        );
        let _ = cleanup_pending_obsolete_blob_files(
            &self.native_storage,
            persistent_path_from_options(&self.options),
            &self.snapshots,
            self.manifest.as_ref(),
        );
        release_browser_writer_lease(self);
    }
}

impl Clone for Db {
    fn clone(&self) -> Self {
        if self.counts_as_user_handle {
            self.inner.user_handles.fetch_add(1, Ordering::AcqRel);
        }
        Self {
            inner: Arc::clone(&self.inner),
            counts_as_user_handle: self.counts_as_user_handle,
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.counts_as_user_handle {
            return;
        }
        if self.inner.user_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.closed.store(true, Ordering::Release);
            shutdown_background_workers(
                &self.inner.maintenance,
                &self.inner.runtime_shutdown,
                &self.inner.background_workers,
            );
            let _ = self.inner.publish_barrier.close();
            release_browser_writer_lease(&self.inner);
            self.inner.substrate.release_writer_lease();
        }
    }
}
