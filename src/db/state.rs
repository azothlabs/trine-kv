//! Database-owned shared state and open-time construction parts.

use super::{
    Arc, AtomicBool, AtomicU8, AtomicU64, AtomicUsize, BTreeMap, BlobReadMetrics,
    CancellationToken, CommitTracker, CompactionLevelStats, CompactionSkip, CompactionSkipStats,
    CompactionTrigger, CompactionTriggerStats, DbOptions, DurabilitySubstrate, FatalWriteStopStats,
    LsmTree, MaintenanceCoordinator, ManifestStore, Mutex, NativeFileBackend, PathBuf,
    PublishBarrier, RandomState, Runtime, RuntimeTask, RwLock, ScanWasteMetrics, Sequence,
    SnapshotTracker, Table, WalBatch, WalFrontDoor, cache, recovery, storage_backend,
};

#[derive(Debug)]
pub(crate) struct DbInner {
    pub(super) options: DbOptions,
    pub(super) user_handles: AtomicUsize,
    pub(super) commit_tracker: CommitTracker,
    pub(super) closed: AtomicBool,
    pub(super) fatal_write_stop_reason: AtomicU8,
    pub(super) publish_barrier: PublishBarrier,
    /// Serializes object-store commit-slot reservation with the corresponding
    /// remote-WAL handoff. The remote WAL represents gaps as empty commits, so
    /// accepting N+1 before N would make N unrecoverable. This lock is never
    /// held across an `.await`.
    pub(super) object_wal_commit_order: Mutex<()>,
    pub(super) memtable_publish_lock: Mutex<()>,
    pub(super) buckets: RwLock<BTreeMap<String, Arc<LsmTree>>>,
    pub(super) snapshots: Arc<SnapshotTracker>,
    pub(super) checkpoints: Mutex<BTreeMap<String, Sequence>>,
    // Obsolete table handles awaiting file deletion. Holding the `Arc<Table>`
    // (not just the id) lets cleanup delete a file only once no in-flight reader
    // still pins it (`Arc::strong_count == 1`), instead of blocking all deletion
    // while any snapshot is open. See `.phrase/protocol/snapshot-version-pinning.md`.
    pub(super) pending_obsolete_tables: Mutex<Vec<Arc<Table>>>,
    pub(super) manifest: Option<Mutex<ManifestStore>>,
    // The write-ahead log + single-writer lease behind the Band 3 durability
    // substrate (see `src/substrate.rs`). The native/WASI persistent path holds a
    // filesystem WAL + LOCK lease here; in-memory and the browser path hold an
    // inert substrate (browser durability still rides the `browser_*` fields).
    pub(super) substrate: DurabilitySubstrate,
    pub(super) block_cache: Arc<cache::BlockCache>,
    pub(super) compaction_runs: AtomicU64,
    pub(super) compaction_input_tables: AtomicU64,
    pub(super) compaction_output_tables: AtomicU64,
    pub(super) compaction_input_bytes: AtomicU64,
    pub(super) compaction_output_bytes: AtomicU64,
    pub(super) compaction_level_stats: Mutex<BTreeMap<u32, CompactionLevelStats>>,
    pub(super) compaction_trigger_stats: Mutex<BTreeMap<CompactionTrigger, CompactionTriggerStats>>,
    pub(super) compaction_skip_stats: Mutex<BTreeMap<CompactionSkip, CompactionSkipStats>>,
    pub(super) blob_gc_runs: AtomicU64,
    pub(super) blob_gc_input_bytes: AtomicU64,
    pub(super) blob_gc_output_bytes: AtomicU64,
    pub(super) blob_gc_discarded_bytes: AtomicU64,
    pub(super) blob_reads: Arc<BlobReadMetrics>,
    pub(super) scan_waste: Arc<ScanWasteMetrics>,
    pub(super) maintenance_cooperative_yields: AtomicU64,
    pub(super) maintenance_budget_exhaustions: AtomicU64,
    /// Accepted host-manifest transitions running on the runtime sync adapter.
    ///
    /// This is separate from native-storage fallback accounting so diagnostics
    /// can prove that an async operation did not adapt the whole operation.
    pub(super) manifest_sync_adapter_tasks: AtomicU64,
    /// The one valid storage state selected when this database was opened.
    pub(super) storage: storage_backend::DatabaseStorage,
    /// Serializes descriptor publication and same-ContentId deduplication.
    pub(super) content_seal_lock: futures::lock::Mutex<()>,
    /// Serializes the irreversible leased-only barrier transition in one writer.
    pub(super) content_access_lock: futures::lock::Mutex<()>,
    /// Per-database keyed hash state for bounded content lock sharding.
    pub(super) content_lock_hasher: RandomState,
    /// Serializes physical-quota counter transitions per `StorageDomain` shard.
    pub(super) content_quota_locks: [futures::lock::Mutex<()>; 256],
    /// Bounded lock shards serialize state transitions for one `UploadId` without
    /// retaining a lock object for every historical upload.
    pub(super) content_upload_locks: [futures::lock::Mutex<()>; 256],
    /// Serializes object-store manifest publishes so the CAS clone -> commit ->
    /// write-back stays atomic without holding the (std) manifest mutex across
    /// the await. Mirrors `browser_manifest_async_lock`; unused by other backends.
    pub(super) object_manifest_async_lock: futures::lock::Mutex<()>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(super) browser_manifest_async_lock: futures::lock::Mutex<()>,
    pub(super) runtime: Runtime,
    pub(super) runtime_shutdown: CancellationToken,
    pub(super) maintenance: Arc<MaintenanceCoordinator>,
    pub(super) background_workers: Mutex<Vec<RuntimeTask>>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::db) enum FatalWriteStopReason {
    Corruption = 1,
    Fenced = 2,
    OutcomeUnknown = 3,
}

impl FatalWriteStopReason {
    pub(super) const NONE: u8 = 0;

    pub(super) const fn code(self) -> u8 {
        self as u8
    }

    pub(super) fn stats(code: u8) -> FatalWriteStopStats {
        FatalWriteStopStats {
            stopped: code != Self::NONE,
            total: u64::from(code != Self::NONE),
            corruption: u64::from(code == Self::Corruption as u8),
            fencing: u64::from(code == Self::Fenced as u8),
            outcome_unknown: u64::from(code == Self::OutcomeUnknown as u8),
        }
    }
}

#[derive(Debug)]
pub(super) struct PersistentOpenParts {
    pub(super) options: DbOptions,
    pub(super) runtime: Runtime,
    pub(super) native_storage: NativeFileBackend,
    pub(super) process_lock: Option<recovery::ProcessLock>,
    pub(super) buckets: BTreeMap<String, Arc<LsmTree>>,
    pub(super) manifest: ManifestStore,
    pub(super) wal: Option<WalFrontDoor>,
    pub(super) batches: Vec<WalBatch>,
    pub(super) replay_floor: Sequence,
    pub(super) db_path_for_cleanup: PathBuf,
}
