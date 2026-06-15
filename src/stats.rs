use std::sync::atomic::{AtomicU64, Ordering};

/// Snapshot of live database, storage, maintenance, cache, and read-path stats.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbStats {
    /// Number of bucket states currently loaded.
    pub live_buckets: usize,
    /// Number of pinned snapshots.
    pub active_snapshots: usize,
    /// Oldest read sequence still pinned by an active snapshot (or the current
    /// visible sequence when none are pinned).
    pub oldest_snapshot_seq: u64,
    /// Version-debt held by the oldest snapshot: visible sequence minus
    /// `oldest_snapshot_seq`. Larger values keep more obsolete versions alive.
    pub oldest_snapshot_lag: u64,
    /// Internal version records merged by range/prefix scans since open.
    pub scan_internal_records: u64,
    /// User keys returned by range/prefix scans since open. The scan
    /// read-amplification ratio is `scan_internal_records / scan_user_keys`.
    pub scan_user_keys: u64,
    /// User-key groups hidden by a point or range delete during scans.
    pub scan_tombstone_hidden_keys: u64,
    /// Approximate bytes held by active memtables.
    pub memtable_bytes: u64,
    /// Number of immutable memtables waiting for flush.
    pub immutable_memtables: usize,
    /// Number of level-0 table files.
    pub l0_tables: usize,
    /// Total number of table files across all levels.
    pub total_tables: usize,
    /// Per-level table counts and byte sizes.
    pub level_tables: Vec<LevelStats>,
    /// Per-level filter counters summed over each level's table files, for
    /// layered (Monkey-style) filter allocation analysis.
    pub level_filters: Vec<LevelFilterStats>,
    /// Total bytes held by table files.
    pub table_bytes: u64,
    /// WAL bytes accepted but not yet synced according to the selected durability.
    pub wal_bytes_pending_sync: u64,
    /// Number of blob files referenced by live records.
    pub live_blob_files: usize,
    /// Bytes in blob files referenced by live records.
    pub live_blob_bytes: u64,
    /// Number of blob files with discardable bytes.
    pub stale_blob_files: usize,
    /// Discardable bytes in stale blob files.
    pub stale_blob_bytes: u64,
    /// Number of blob files no longer referenced by live records.
    pub obsolete_blob_files: usize,
    /// Bytes in obsolete blob files.
    pub obsolete_blob_bytes: u64,
    /// Number of blob garbage-collection runs.
    pub blob_gc_runs: u64,
    /// Input bytes scanned by blob garbage collection.
    pub blob_gc_input_bytes: u64,
    /// Output bytes written by blob garbage collection.
    pub blob_gc_output_bytes: u64,
    /// Bytes discarded by blob garbage collection.
    pub blob_gc_discarded_bytes: u64,
    /// Number of blob value reads.
    pub blob_read_count: u64,
    /// Bytes returned by blob value reads.
    pub blob_read_bytes: u64,
    /// Number of compaction runs.
    pub compaction_runs: u64,
    /// Table files read by compaction.
    pub compaction_input_tables: u64,
    /// Table files written by compaction.
    pub compaction_output_tables: u64,
    /// Table bytes read by compaction.
    pub compaction_input_bytes: u64,
    /// Table bytes written by compaction.
    pub compaction_output_bytes: u64,
    /// Per-level table and byte counts read and written by compaction.
    pub compaction_levels: Vec<CompactionLevelStats>,
    /// Per-trigger table and byte counts read and written by compaction.
    pub compaction_triggers: Vec<CompactionTriggerStats>,
    /// Per-reason counts for compactions the picker deliberately did not run.
    pub compaction_skips: Vec<CompactionSkipStats>,
    /// Commit sequences allocated by writers.
    pub commit_sequences_allocated: u64,
    /// Highest commit sequence visible to readers.
    pub commit_visible_sequence: u64,
    /// Commit slots allocated but not yet published or skipped.
    pub commit_open_slots: usize,
    /// Commit slots skipped after failed publication.
    pub commit_skipped_slots: u64,
    /// Number of WAL shards configured for the database.
    pub wal_shards: usize,
    /// Number of WAL shards currently open.
    pub wal_open_shards: usize,
    /// Per-shard WAL queue capacity.
    pub wal_queue_capacity: usize,
    /// WAL records accepted by the writer path.
    pub wal_records_accepted: u64,
    /// WAL bytes accepted by the writer path.
    pub wal_bytes_accepted: u64,
    /// Whether storage work is using the runtime's sync adapter.
    pub storage_uses_sync_adapter: bool,
    /// Whether native storage work is routed through the platform I/O driver.
    ///
    /// This reports the selected storage driver. It can be `true` on targets
    /// whose current operations are thread-pool managed; use
    /// [`Self::storage_uses_platform_async_io`] and the platform task counters
    /// to distinguish native async work from managed async work.
    pub storage_uses_platform_io_driver: bool,
    /// Whether storage work is using platform-io asynchronous completion.
    pub storage_uses_platform_async_io: bool,
    /// Blocking storage tasks accepted by the sync adapter.
    pub storage_sync_adapter_tasks: u64,
    /// Sync-adapter queue capacity.
    pub storage_sync_adapter_queue_capacity: usize,
    /// Sync-adapter tasks currently queued.
    pub storage_sync_adapter_queued_tasks: usize,
    /// Sync-adapter tasks submitted.
    pub storage_sync_adapter_submitted_tasks: u64,
    /// Sync-adapter tasks completed.
    pub storage_sync_adapter_completed_tasks: u64,
    /// Sync-adapter tasks rejected because the queue was full or unavailable.
    pub storage_sync_adapter_rejected_tasks: u64,
    /// Total runtime spent by sync-adapter tasks.
    pub storage_sync_adapter_total_runtime_micros: u64,
    /// Storage tasks completed through true or partial native platform async I/O.
    pub storage_platform_async_io_tasks: u64,
    /// Storage tasks completed by platform-io's managed thread-pool path.
    pub storage_platform_thread_pool_managed_async_tasks: u64,
    /// Storage tasks that used a synchronous fallback path.
    pub storage_platform_sync_fallback_tasks: u64,
    /// Per-operation platform I/O capability-class counters.
    ///
    /// These counters are filled only when native storage work is routed
    /// through the `platform-io` driver. They explain how each Trine storage
    /// operation completed at the platform boundary. For example, a target can
    /// report true platform async reads while directory listing reports
    /// thread-pool managed async.
    pub storage_platform_io_operations: PlatformIoOperationStats,
    /// Storage tasks completed inline.
    pub storage_inline_tasks: u64,
    /// Per-operation storage request counters and latency totals.
    pub storage_operations: StorageOperationStats,
    /// Cooperative maintenance yields.
    pub maintenance_cooperative_yields: u64,
    /// Maintenance runs stopped after exhausting their budget.
    pub maintenance_budget_exhaustions: u64,
    /// Block-cache hits.
    pub block_cache_hits: u64,
    /// Block-cache misses.
    pub block_cache_misses: u64,
    /// Point-read path counters.
    pub read_path: ReadPathStats,
    /// Filter hit, miss, and false-positive counters.
    pub filters: FilterStats,
}

/// Request count and total latency for one storage operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageOperationMetric {
    /// Number of operation requests.
    pub requests: u64,
    /// Total operation latency in microseconds.
    pub total_latency_micros: u64,
}

/// Per-operation storage metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageOperationStats {
    /// Opens of readable storage objects.
    pub open_read: StorageOperationMetric,
    /// Length queries for readable storage objects.
    pub len: StorageOperationMetric,
    /// Borrowed-buffer positioned reads.
    pub read_exact_at: StorageOperationMetric,
    /// Owned-buffer positioned reads.
    pub read_exact_at_owned: StorageOperationMetric,
    /// Whole-object byte reads.
    pub read_object_bytes: StorageOperationMetric,
    /// Opens of appendable storage objects.
    pub open_append: StorageOperationMetric,
    /// Appends to storage objects.
    pub append: StorageOperationMetric,
    /// Persistence requests for storage objects.
    pub persist: StorageOperationMetric,
    /// WAL rewrite operations.
    pub rewrite_wal: StorageOperationMetric,
    /// Writer-lease acquisition requests.
    pub acquire_writer_lease: StorageOperationMetric,
    /// Directory creation requests.
    pub create_directory_all: StorageOperationMetric,
    /// Directory file-list requests.
    pub list_directory_files: StorageOperationMetric,
    /// Directory sync requests after atomic renames.
    pub sync_directory_after_renames: StorageOperationMetric,
    /// Reads of the current manifest pointer.
    pub read_current_manifest: StorageOperationMetric,
    /// Manifest publish operations.
    pub publish_manifest: StorageOperationMetric,
    /// Whole-object writes.
    pub write_object: StorageOperationMetric,
    /// Object delete requests.
    pub delete_object: StorageOperationMetric,
    /// Object list requests.
    pub list_objects: StorageOperationMetric,
}

/// Platform I/O capability-class counters for one Trine storage operation.
///
/// Each field is a task count. A single operation request increments exactly one
/// field when it goes through the `platform-io` driver. Operations that use the
/// normal bounded sync adapter or inline native file path do not increment this
/// structure. A zero value usually means the operation has not run through the
/// platform driver during the stats interval, not that the target lacks support.
///
/// Use [`Self::total`] to count all platform-driver completions for the
/// operation, and [`Self::non_true_platform_async_total`] to count completions
/// that were not end-to-end true platform async.
///
/// # Examples
///
/// ```
/// use trine_kv::PlatformIoClassCounters;
///
/// let counters = PlatformIoClassCounters {
///     true_platform_async: 2,
///     thread_pool_managed_async: 1,
///     ..PlatformIoClassCounters::default()
/// };
///
/// assert_eq!(counters.total(), 3);
/// assert_eq!(counters.non_true_platform_async_total(), 1);
/// assert!(counters.uses_true_platform_async());
/// assert!(counters.uses_non_true_platform_async());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlatformIoClassCounters {
    /// Completions that used a real platform async completion mechanism.
    ///
    /// These are operation requests whose selected backend class is true
    /// platform async for the whole Trine storage operation. On the current
    /// backend matrix this is expected on Linux for the operations audited as
    /// true async.
    pub true_platform_async: u64,
    /// Completions that used native async primitives but still needed
    /// non-native work.
    ///
    /// This class means the platform has audited native async evidence for part
    /// of the operation, but at least one user-visible step such as open,
    /// metadata, sync, rename, delete, directory work, or lease handling is not
    /// yet true platform async.
    pub platform_native_async_but_partial: u64,
    /// Completions handled by platform-io's managed thread-pool path.
    ///
    /// This class is used when the target or operation has no complete native
    /// async file completion, but platform-io still returns an asynchronous
    /// completion by running the blocking work on its managed thread pool. WASM
    /// and other targets without native threads must not use this class.
    pub thread_pool_managed_async: u64,
    /// Completions handled by an explicit platform-driver blocking path.
    ///
    /// This class is reserved for operations that cannot currently be
    /// completed by native async primitives or platform-io's managed
    /// thread-pool path. Native targets should prefer
    /// [`Self::thread_pool_managed_async`] when platform-io owns the blocking
    /// work and returns an asynchronous completion to the caller.
    pub blocking_fallback: u64,
    /// Completions rejected because the target cannot support the operation.
    pub unsupported: u64,
}

impl PlatformIoClassCounters {
    /// Returns the total number of platform-driver completions in these class
    /// counters.
    ///
    /// The result is a task count. It is the saturating sum of all class fields,
    /// so it stays at `u64::MAX` instead of wrapping if counters are combined
    /// after a very long process lifetime.
    #[must_use]
    pub fn total(self) -> u64 {
        self.true_platform_async
            .saturating_add(self.platform_native_async_but_partial)
            .saturating_add(self.thread_pool_managed_async)
            .saturating_add(self.blocking_fallback)
            .saturating_add(self.unsupported)
    }

    /// Returns whether no platform-driver completion has been recorded.
    ///
    /// A zero result means this counter set is empty for the observed stats
    /// snapshot. It does not by itself prove that the operation is unsupported;
    /// use [`Self::has_unsupported`] after the operation has been attempted to
    /// check unsupported completions.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.total() == 0
    }

    /// Returns the number of platform-driver completions that were not true
    /// platform async for the whole Trine operation.
    ///
    /// This includes partial native async, thread-pool managed async, blocking
    /// fallback, and unsupported completions. It excludes
    /// [`Self::true_platform_async`].
    #[must_use]
    pub fn non_true_platform_async_total(self) -> u64 {
        self.platform_native_async_but_partial
            .saturating_add(self.thread_pool_managed_async)
            .saturating_add(self.blocking_fallback)
            .saturating_add(self.unsupported)
    }

    /// Returns the number of platform-driver completions that were not true
    /// platform async for the whole Trine operation.
    ///
    /// Prefer [`Self::non_true_platform_async_total`] in new code; this helper
    /// remains as a compatibility alias.
    #[must_use]
    pub fn fallback_total(self) -> u64 {
        self.non_true_platform_async_total()
    }

    /// Returns whether at least one completion was true platform async for the
    /// whole Trine operation.
    #[must_use]
    pub fn uses_true_platform_async(self) -> bool {
        self.true_platform_async > 0
    }

    /// Returns whether at least one completion was not true platform async for
    /// the whole Trine operation.
    #[must_use]
    pub fn uses_non_true_platform_async(self) -> bool {
        self.non_true_platform_async_total() > 0
    }

    /// Returns whether at least one completion was not true platform async for
    /// the whole Trine operation.
    ///
    /// Prefer [`Self::uses_non_true_platform_async`] in new code; this helper
    /// remains as a compatibility alias.
    #[must_use]
    pub fn uses_fallback(self) -> bool {
        self.uses_non_true_platform_async()
    }

    /// Returns whether at least one completion reached an unsupported platform
    /// operation.
    #[must_use]
    pub fn has_unsupported(self) -> bool {
        self.unsupported > 0
    }

    fn saturating_add_assign(&mut self, other: Self) {
        self.true_platform_async = self
            .true_platform_async
            .saturating_add(other.true_platform_async);
        self.platform_native_async_but_partial = self
            .platform_native_async_but_partial
            .saturating_add(other.platform_native_async_but_partial);
        self.thread_pool_managed_async = self
            .thread_pool_managed_async
            .saturating_add(other.thread_pool_managed_async);
        self.blocking_fallback = self
            .blocking_fallback
            .saturating_add(other.blocking_fallback);
        self.unsupported = self.unsupported.saturating_add(other.unsupported);
    }
}

/// Per-operation platform I/O capability-class counters.
///
/// This table uses Trine operation names instead of OS API names. It lets
/// diagnostics distinguish, for example, random reads from directory listing
/// even when both are routed through the same selected platform driver.
///
/// Use [`Self::total`] to summarize all operations into one class counter set
/// for dashboards, health checks, or tests that only need to know whether any
/// platform I/O work used true async, partial native async, thread-pool
/// managed async, blocking fallback, or unsupported classes.
///
/// # Examples
///
/// ```
/// use trine_kv::{PlatformIoClassCounters, PlatformIoOperationStats};
///
/// let stats = PlatformIoOperationStats {
///     random_read: PlatformIoClassCounters {
///         true_platform_async: 4,
///         ..PlatformIoClassCounters::default()
///     },
///     directory_listing: PlatformIoClassCounters {
///         thread_pool_managed_async: 1,
///         ..PlatformIoClassCounters::default()
///     },
///     ..PlatformIoOperationStats::default()
/// };
///
/// let total = stats.total();
/// assert_eq!(total.total(), 5);
/// assert!(total.uses_true_platform_async());
/// assert!(total.uses_non_true_platform_async());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlatformIoOperationStats {
    /// Length lookups for readable native-file objects.
    pub length_lookup: PlatformIoClassCounters,
    /// Owned-buffer positioned reads.
    pub random_read: PlatformIoClassCounters,
    /// Whole-object reads, including optional reads for manifests and tables.
    pub whole_object_read: PlatformIoClassCounters,
    /// Temporary-file writes followed by rename publish.
    pub temp_write_rename_publish: PlatformIoClassCounters,
    /// Opens of appendable WAL objects.
    pub append_open: PlatformIoClassCounters,
    /// Appends to WAL or appendable storage objects.
    pub append: PlatformIoClassCounters,
    /// Persistence requests such as flush, data sync, or full sync.
    pub persist: PlatformIoClassCounters,
    /// WAL rewrite operations that publish a replacement WAL file.
    pub wal_rewrite: PlatformIoClassCounters,
    /// Object delete requests.
    pub delete: PlatformIoClassCounters,
    /// Directory creation requests.
    pub directory_create: PlatformIoClassCounters,
    /// Directory sync requests after rename publication.
    pub directory_sync: PlatformIoClassCounters,
    /// Directory or object listing requests.
    pub directory_listing: PlatformIoClassCounters,
    /// Writer-lease acquisition requests.
    pub writer_lease: PlatformIoClassCounters,
}

impl PlatformIoOperationStats {
    /// Returns class counters summed across every Trine platform I/O operation.
    ///
    /// Each field in the returned value is a task count. The sums saturate at
    /// `u64::MAX`, matching [`PlatformIoClassCounters::total`], so callers can
    /// safely aggregate snapshots from long-lived processes without wrapping.
    #[must_use]
    pub fn total(self) -> PlatformIoClassCounters {
        let mut total = PlatformIoClassCounters::default();
        total.saturating_add_assign(self.length_lookup);
        total.saturating_add_assign(self.random_read);
        total.saturating_add_assign(self.whole_object_read);
        total.saturating_add_assign(self.temp_write_rename_publish);
        total.saturating_add_assign(self.append_open);
        total.saturating_add_assign(self.append);
        total.saturating_add_assign(self.persist);
        total.saturating_add_assign(self.wal_rewrite);
        total.saturating_add_assign(self.delete);
        total.saturating_add_assign(self.directory_create);
        total.saturating_add_assign(self.directory_sync);
        total.saturating_add_assign(self.directory_listing);
        total.saturating_add_assign(self.writer_lease);
        total
    }
}

#[derive(Debug, Default)]
pub(crate) struct BlobReadMetrics {
    count: AtomicU64,
    bytes: AtomicU64,
}

impl BlobReadMetrics {
    pub(crate) fn record(&self, bytes: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> (u64, u64) {
        (
            self.count.load(Ordering::Acquire),
            self.bytes.load(Ordering::Acquire),
        )
    }
}

/// Range/prefix scan GC-waste counters: how many internal version records the
/// merge had to handle relative to the user keys it returned, and how many
/// user-key groups were hidden by a delete. The north-star read-amplification
/// ratio is `internal_records / user_keys`.
#[derive(Debug, Default)]
pub(crate) struct ScanWasteMetrics {
    internal_records: AtomicU64,
    user_keys: AtomicU64,
    tombstone_hidden_keys: AtomicU64,
}

impl ScanWasteMetrics {
    /// Records one resolved user-key version group: `group_records` versions
    /// merged, and the outcome (a visible row, a delete-hidden row, or nothing).
    pub(crate) fn record_group(&self, group_records: u64, outcome: ScanGroupOutcome) {
        self.internal_records
            .fetch_add(group_records, Ordering::Relaxed);
        match outcome {
            ScanGroupOutcome::Visible => {
                self.user_keys.fetch_add(1, Ordering::Relaxed);
            }
            ScanGroupOutcome::HiddenByDelete => {
                self.tombstone_hidden_keys.fetch_add(1, Ordering::Relaxed);
            }
            ScanGroupOutcome::NoVisibleVersion => {}
        }
    }

    pub(crate) fn snapshot(&self) -> ScanWasteSnapshot {
        ScanWasteSnapshot {
            internal_records: self.internal_records.load(Ordering::Acquire),
            user_keys: self.user_keys.load(Ordering::Acquire),
            tombstone_hidden_keys: self.tombstone_hidden_keys.load(Ordering::Acquire),
        }
    }
}

/// Outcome of resolving one user-key version group during a scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanGroupOutcome {
    /// A visible row was returned for the user key.
    Visible,
    /// The user key existed but was hidden by a point or range delete.
    HiddenByDelete,
    /// No version of the user key was visible to the read sequence.
    NoVisibleVersion,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ScanWasteSnapshot {
    pub(crate) internal_records: u64,
    pub(crate) user_keys: u64,
    pub(crate) tombstone_hidden_keys: u64,
}

/// Table count and byte size for one LSM level.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LevelStats {
    /// LSM level number.
    pub level: u32,
    /// Number of table files in the level.
    pub tables: usize,
    /// Total bytes in the level's table files.
    pub bytes: u64,
}

/// Per-level table and byte totals read and written by compaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionLevelStats {
    /// LSM level number.
    pub level: u32,
    /// Number of input table files read from this level.
    pub input_tables: u64,
    /// Number of output table files written to this level.
    pub output_tables: u64,
    /// Input table bytes read from this level.
    pub input_bytes: u64,
    /// Output table bytes written to this level.
    pub output_bytes: u64,
}

/// Reason the compaction picker selected a compaction input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompactionTrigger {
    /// Level-0 tables overlapped each other or the next level and needed to be
    /// closed into a safe output range.
    L0Overlap,
    /// A non-level-0 table level was above its configured byte target.
    LevelSize,
    /// No higher-priority trigger existed, but a shallow non-level-0 level had
    /// multiple tables in the requested range that could be merged downward.
    MultiTableLevel,
}

/// Per-trigger table and byte totals read and written by compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionTriggerStats {
    /// Picker reason shared by the compaction inputs counted in this row.
    pub trigger: CompactionTrigger,
    /// Number of compaction inputs selected for this reason.
    pub runs: u64,
    /// Number of input table files read for this reason.
    pub input_tables: u64,
    /// Number of output table files written for this reason.
    pub output_tables: u64,
    /// Input table bytes read for this reason.
    pub input_bytes: u64,
    /// Output table bytes written for this reason.
    pub output_bytes: u64,
}

/// Reason the compaction picker deliberately left a level un-compacted.
///
/// This is the "did not run" complement to [`CompactionTrigger`]: it explains a
/// non-uniform per-level policy decision rather than a compaction that happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompactionSkip {
    /// A deeper non-level-0 level had enough non-overlapping tables that a
    /// uniform picker would merge one downward, but the non-uniform policy left
    /// it lazy. The level was within its depth-scaled file budget and no size,
    /// tombstone, or blob trigger justified rewriting it. Because non-level-0
    /// levels are non-overlapping, the extra tables add no point-read candidate
    /// depth, so leaving them avoids write amplification without regressing
    /// reads.
    LowerLevelLazy,
}

/// Per-reason counts for compactions the picker deliberately did not run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionSkipStats {
    /// Policy reason shared by the skipped compaction decisions in this row.
    pub skip: CompactionSkip,
    /// Number of times the picker chose this lazy decision.
    pub occurrences: u64,
}

/// Filter counters for table-level and block-level filters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FilterStats {
    /// Table key-filter positive results for point reads.
    pub table_point_hits: u64,
    /// Table key-filter negative results for point reads.
    pub table_point_misses: u64,
    /// Table key-filter positives that did not contain the key.
    pub table_point_false_positives: u64,
    /// Table prefix-filter positive results for prefix reads.
    pub table_prefix_hits: u64,
    /// Table prefix-filter negative results for prefix reads.
    pub table_prefix_misses: u64,
    /// Table prefix-filter positives that did not contain the prefix.
    pub table_prefix_false_positives: u64,
    /// Block key-filter positive results for point reads.
    pub block_point_hits: u64,
    /// Block key-filter negative results for point reads.
    pub block_point_misses: u64,
    /// Block key-filter positives that did not contain the key.
    pub block_point_false_positives: u64,
    /// Block prefix-filter positive results for prefix reads.
    pub block_prefix_hits: u64,
    /// Block prefix-filter negative results for prefix reads.
    pub block_prefix_misses: u64,
    /// Block prefix-filter positives that did not contain the prefix.
    pub block_prefix_false_positives: u64,
}

impl FilterStats {
    /// Observed point false-positive rate: filter-allowed probes that turned out
    /// not to contain the key, over all filter-allowed absent probes (false
    /// positives plus correct negatives). Returns `None` when no absent probe
    /// exercised the table point filter, so callers do not divide by zero.
    #[must_use]
    pub fn table_point_false_positive_rate(&self) -> Option<f64> {
        let allowed_absent = self
            .table_point_false_positives
            .saturating_add(self.table_point_misses);
        if allowed_absent == 0 {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        Some(self.table_point_false_positives as f64 / allowed_absent as f64)
    }
}

/// Per-level aggregation of table filter counters.
///
/// Filter counters are recorded per table; this rolls them up by LSM level so
/// layered (Monkey-style) filter allocation can see where false positives
/// actually concentrate before any per-level `bits_per_key` change.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LevelFilterStats {
    /// LSM level these filter counters belong to.
    pub level: u32,
    /// Number of table files on this level that contributed counters.
    pub tables: usize,
    /// Table and block filter counters summed over this level's tables.
    pub filters: FilterStats,
    /// Resident Bloom filter bytes held in memory for this level's tables
    /// (table-level plus resident data-block filters). This is the layered
    /// (Monkey-style) allocation memory metric; deeper levels with a lower
    /// per-key budget hold fewer filter bytes per key.
    pub filter_resident_bytes: u64,
}

/// Read-path counters that describe how far reads travel through table metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadPathStats {
    /// Table files considered by point reads.
    pub point_table_probes: u64,
    /// Level-0 table files considered by point reads.
    pub point_l0_table_probes: u64,
    /// Non-level-0 table files considered by point reads.
    pub point_non_l0_table_probes: u64,
    /// Point lookup keys that considered at least one level-0 table.
    pub point_l0_lookup_keys: u64,
    /// Extra level-0 table probes after the first level-0 probe for a point key.
    pub point_l0_overlap_extra_table_probes: u64,
    /// Input keys accepted by grouped point-batch reads.
    pub batch_point_input_keys: u64,
    /// Unique keys planned by grouped point-batch reads.
    pub batch_point_unique_keys: u64,
    /// Table groups visited by grouped point-batch reads.
    pub batch_point_table_groups: u64,
    /// Unique grouped point-batch keys that considered at least one level-0 table.
    pub batch_point_l0_lookup_keys: u64,
    /// Extra level-0 table probes after the first probe for grouped point-batch keys.
    pub batch_point_l0_overlap_extra_table_probes: u64,
    /// Index partitions considered by point reads.
    pub point_index_partition_probes: u64,
    /// Data-block metadata entries considered by point reads.
    pub point_block_metadata_probes: u64,
    /// Data blocks read by point reads.
    pub point_data_block_reads: u64,
    /// Point reads skipped because filters ruled out a table or block.
    pub point_filter_misses: u64,
    /// Table files considered by range scans.
    pub range_table_probes: u64,
    /// Level-0 table files considered by range scans.
    pub range_l0_table_probes: u64,
    /// Non-level-0 table files considered by range scans.
    pub range_non_l0_table_probes: u64,
    /// Table files inspected for range tombstones during range scans.
    pub range_tombstone_table_probes: u64,
    /// Table files considered by prefix scans.
    pub prefix_table_probes: u64,
    /// Table files inspected for range tombstones during prefix scans.
    pub prefix_tombstone_table_probes: u64,
    /// Data-block metadata entries considered by prefix scans.
    pub prefix_block_metadata_probes: u64,
    /// Data blocks read by prefix scans.
    pub prefix_data_block_reads: u64,
    /// Prefix scan work skipped because filters ruled out a table or block.
    pub prefix_filter_misses: u64,
}

impl ReadPathStats {
    pub(crate) fn saturating_add_assign(&mut self, other: Self) {
        self.point_table_probes = self
            .point_table_probes
            .saturating_add(other.point_table_probes);
        self.point_l0_table_probes = self
            .point_l0_table_probes
            .saturating_add(other.point_l0_table_probes);
        self.point_non_l0_table_probes = self
            .point_non_l0_table_probes
            .saturating_add(other.point_non_l0_table_probes);
        self.point_l0_lookup_keys = self
            .point_l0_lookup_keys
            .saturating_add(other.point_l0_lookup_keys);
        self.point_l0_overlap_extra_table_probes = self
            .point_l0_overlap_extra_table_probes
            .saturating_add(other.point_l0_overlap_extra_table_probes);
        self.batch_point_input_keys = self
            .batch_point_input_keys
            .saturating_add(other.batch_point_input_keys);
        self.batch_point_unique_keys = self
            .batch_point_unique_keys
            .saturating_add(other.batch_point_unique_keys);
        self.batch_point_table_groups = self
            .batch_point_table_groups
            .saturating_add(other.batch_point_table_groups);
        self.batch_point_l0_lookup_keys = self
            .batch_point_l0_lookup_keys
            .saturating_add(other.batch_point_l0_lookup_keys);
        self.batch_point_l0_overlap_extra_table_probes = self
            .batch_point_l0_overlap_extra_table_probes
            .saturating_add(other.batch_point_l0_overlap_extra_table_probes);
        self.point_index_partition_probes = self
            .point_index_partition_probes
            .saturating_add(other.point_index_partition_probes);
        self.point_block_metadata_probes = self
            .point_block_metadata_probes
            .saturating_add(other.point_block_metadata_probes);
        self.point_data_block_reads = self
            .point_data_block_reads
            .saturating_add(other.point_data_block_reads);
        self.point_filter_misses = self
            .point_filter_misses
            .saturating_add(other.point_filter_misses);
        self.range_table_probes = self
            .range_table_probes
            .saturating_add(other.range_table_probes);
        self.range_l0_table_probes = self
            .range_l0_table_probes
            .saturating_add(other.range_l0_table_probes);
        self.range_non_l0_table_probes = self
            .range_non_l0_table_probes
            .saturating_add(other.range_non_l0_table_probes);
        self.range_tombstone_table_probes = self
            .range_tombstone_table_probes
            .saturating_add(other.range_tombstone_table_probes);
        self.prefix_table_probes = self
            .prefix_table_probes
            .saturating_add(other.prefix_table_probes);
        self.prefix_tombstone_table_probes = self
            .prefix_tombstone_table_probes
            .saturating_add(other.prefix_tombstone_table_probes);
        self.prefix_block_metadata_probes = self
            .prefix_block_metadata_probes
            .saturating_add(other.prefix_block_metadata_probes);
        self.prefix_data_block_reads = self
            .prefix_data_block_reads
            .saturating_add(other.prefix_data_block_reads);
        self.prefix_filter_misses = self
            .prefix_filter_misses
            .saturating_add(other.prefix_filter_misses);
    }
}

impl FilterStats {
    pub(crate) fn saturating_add_assign(&mut self, other: Self) {
        self.table_point_hits = self.table_point_hits.saturating_add(other.table_point_hits);
        self.table_point_misses = self
            .table_point_misses
            .saturating_add(other.table_point_misses);
        self.table_point_false_positives = self
            .table_point_false_positives
            .saturating_add(other.table_point_false_positives);
        self.table_prefix_hits = self
            .table_prefix_hits
            .saturating_add(other.table_prefix_hits);
        self.table_prefix_misses = self
            .table_prefix_misses
            .saturating_add(other.table_prefix_misses);
        self.table_prefix_false_positives = self
            .table_prefix_false_positives
            .saturating_add(other.table_prefix_false_positives);
        self.block_point_hits = self.block_point_hits.saturating_add(other.block_point_hits);
        self.block_point_misses = self
            .block_point_misses
            .saturating_add(other.block_point_misses);
        self.block_point_false_positives = self
            .block_point_false_positives
            .saturating_add(other.block_point_false_positives);
        self.block_prefix_hits = self
            .block_prefix_hits
            .saturating_add(other.block_prefix_hits);
        self.block_prefix_misses = self
            .block_prefix_misses
            .saturating_add(other.block_prefix_misses);
        self.block_prefix_false_positives = self
            .block_prefix_false_positives
            .saturating_add(other.block_prefix_false_positives);
    }
}

#[cfg(test)]
mod tests {
    use super::{FilterStats, PlatformIoClassCounters, PlatformIoOperationStats};

    #[test]
    fn table_point_false_positive_rate_uses_allowed_absent_probes() {
        // 1 false positive out of 1 false positive + 3 correct negatives = 0.25.
        let stats = FilterStats {
            table_point_false_positives: 1,
            table_point_misses: 3,
            ..FilterStats::default()
        };
        assert_eq!(stats.table_point_false_positive_rate(), Some(0.25));
    }

    #[test]
    fn table_point_false_positive_rate_is_none_without_absent_probes() {
        let stats = FilterStats {
            table_point_hits: 10,
            ..FilterStats::default()
        };
        assert_eq!(stats.table_point_false_positive_rate(), None);
    }

    #[test]
    fn platform_io_class_counter_helpers_summarize_classes() {
        let counters = PlatformIoClassCounters {
            true_platform_async: 2,
            platform_native_async_but_partial: 3,
            thread_pool_managed_async: 5,
            blocking_fallback: 7,
            unsupported: 11,
        };

        assert_eq!(counters.total(), 28);
        assert_eq!(counters.non_true_platform_async_total(), 26);
        assert_eq!(counters.fallback_total(), 26);
        assert!(!counters.is_empty());
        assert!(counters.uses_true_platform_async());
        assert!(counters.uses_non_true_platform_async());
        assert!(counters.uses_fallback());
        assert!(counters.has_unsupported());
        assert!(PlatformIoClassCounters::default().is_empty());
    }

    #[test]
    fn platform_io_operation_stats_total_saturates_by_class() {
        let stats = PlatformIoOperationStats {
            length_lookup: PlatformIoClassCounters {
                true_platform_async: u64::MAX,
                thread_pool_managed_async: 1,
                ..PlatformIoClassCounters::default()
            },
            random_read: PlatformIoClassCounters {
                true_platform_async: 1,
                platform_native_async_but_partial: 2,
                blocking_fallback: 3,
                unsupported: 4,
                ..PlatformIoClassCounters::default()
            },
            directory_listing: PlatformIoClassCounters {
                blocking_fallback: 5,
                ..PlatformIoClassCounters::default()
            },
            ..PlatformIoOperationStats::default()
        };

        let total = stats.total();
        assert_eq!(total.true_platform_async, u64::MAX);
        assert_eq!(total.platform_native_async_but_partial, 2);
        assert_eq!(total.thread_pool_managed_async, 1);
        assert_eq!(total.blocking_fallback, 8);
        assert_eq!(total.unsupported, 4);
    }
}
