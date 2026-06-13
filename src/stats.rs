use std::sync::atomic::{AtomicU64, Ordering};

/// Snapshot of live database, storage, maintenance, cache, and read-path stats.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbStats {
    /// Number of bucket states currently loaded.
    pub live_buckets: usize,
    /// Number of pinned snapshots.
    pub active_snapshots: usize,
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
    /// whose current operations are fallback-classified; use
    /// [`Self::storage_uses_platform_async_io`] and the platform task counters
    /// to distinguish true async work from fallback work.
    pub storage_uses_platform_io_driver: bool,
    /// Whether storage work is using platform async I/O.
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
    /// Storage tasks that used a backend fallback path.
    pub storage_platform_backend_fallback_tasks: u64,
    /// Storage tasks that used a synchronous fallback path.
    pub storage_platform_sync_fallback_tasks: u64,
    /// Per-operation platform I/O capability-class counters.
    ///
    /// These counters are filled only when native storage work is routed
    /// through the `platform-io` driver. They explain how each Trine storage
    /// operation completed at the platform boundary. For example, a target can
    /// report true platform async reads while directory listing still reports a
    /// blocking fallback.
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
/// operation, and [`Self::fallback_total`] to count completions that were not
/// end-to-end true platform async.
///
/// # Examples
///
/// ```
/// use trine_kv::PlatformIoClassCounters;
///
/// let counters = PlatformIoClassCounters {
///     true_platform_async: 2,
///     platform_managed_fallback: 1,
///     ..PlatformIoClassCounters::default()
/// };
///
/// assert_eq!(counters.total(), 3);
/// assert_eq!(counters.fallback_total(), 1);
/// assert!(counters.uses_true_platform_async());
/// assert!(counters.uses_fallback());
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
    /// Completions that used native async primitives but still needed fallback
    /// work.
    ///
    /// This class means the platform has audited native async evidence for part
    /// of the operation, but at least one user-visible step such as open,
    /// metadata, sync, rename, delete, directory work, or lease handling is not
    /// yet true platform async.
    pub platform_native_async_but_partial: u64,
    /// Completions handled by a fallback managed inside the platform driver.
    ///
    /// The operation still crossed the platform-io boundary, but the selected
    /// backend did not provide true async behavior for this Trine operation.
    pub platform_managed_fallback: u64,
    /// Completions handled by an explicit platform-driver blocking fallback.
    ///
    /// This is used for operations such as directory listing when the selected
    /// backend has no real async enumeration path.
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
            .saturating_add(self.platform_managed_fallback)
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
    /// This includes partial native async, platform-managed fallback, blocking
    /// fallback, and unsupported completions. It excludes
    /// [`Self::true_platform_async`].
    #[must_use]
    pub fn fallback_total(self) -> u64 {
        self.platform_native_async_but_partial
            .saturating_add(self.platform_managed_fallback)
            .saturating_add(self.blocking_fallback)
            .saturating_add(self.unsupported)
    }

    /// Returns whether at least one completion was true platform async for the
    /// whole Trine operation.
    #[must_use]
    pub fn uses_true_platform_async(self) -> bool {
        self.true_platform_async > 0
    }

    /// Returns whether at least one completion used a partial, managed,
    /// blocking, or unsupported fallback class.
    #[must_use]
    pub fn uses_fallback(self) -> bool {
        self.fallback_total() > 0
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
        self.platform_managed_fallback = self
            .platform_managed_fallback
            .saturating_add(other.platform_managed_fallback);
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
/// platform I/O work used true async, fallback, or unsupported classes.
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
///         blocking_fallback: 1,
///         ..PlatformIoClassCounters::default()
///     },
///     ..PlatformIoOperationStats::default()
/// };
///
/// let total = stats.total();
/// assert_eq!(total.total(), 5);
/// assert!(total.uses_true_platform_async());
/// assert!(total.uses_fallback());
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

/// Read-path counters that describe how far reads travel through table metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadPathStats {
    /// Table files considered by point reads.
    pub point_table_probes: u64,
    /// Index partitions considered by point reads.
    pub point_index_partition_probes: u64,
    /// Data-block metadata entries considered by point reads.
    pub point_block_metadata_probes: u64,
    /// Data blocks read by point reads.
    pub point_data_block_reads: u64,
    /// Point reads skipped because filters ruled out a table or block.
    pub point_filter_misses: u64,
    /// Table files considered by prefix scans.
    pub prefix_table_probes: u64,
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
        self.prefix_table_probes = self
            .prefix_table_probes
            .saturating_add(other.prefix_table_probes);
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
    use super::{PlatformIoClassCounters, PlatformIoOperationStats};

    #[test]
    fn platform_io_class_counter_helpers_summarize_classes() {
        let counters = PlatformIoClassCounters {
            true_platform_async: 2,
            platform_native_async_but_partial: 3,
            platform_managed_fallback: 5,
            blocking_fallback: 7,
            unsupported: 11,
        };

        assert_eq!(counters.total(), 28);
        assert_eq!(counters.fallback_total(), 26);
        assert!(!counters.is_empty());
        assert!(counters.uses_true_platform_async());
        assert!(counters.uses_fallback());
        assert!(counters.has_unsupported());
        assert!(PlatformIoClassCounters::default().is_empty());
    }

    #[test]
    fn platform_io_operation_stats_total_saturates_by_class() {
        let stats = PlatformIoOperationStats {
            length_lookup: PlatformIoClassCounters {
                true_platform_async: u64::MAX,
                platform_managed_fallback: 1,
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
        assert_eq!(total.platform_managed_fallback, 1);
        assert_eq!(total.blocking_fallback, 8);
        assert_eq!(total.unsupported, 4);
    }
}
