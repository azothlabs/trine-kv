use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbStats {
    pub live_buckets: usize,
    pub active_snapshots: usize,
    pub memtable_bytes: u64,
    pub immutable_memtables: usize,
    pub l0_tables: usize,
    pub total_tables: usize,
    pub level_tables: Vec<LevelStats>,
    pub table_bytes: u64,
    pub wal_bytes_pending_sync: u64,
    pub live_blob_files: usize,
    pub live_blob_bytes: u64,
    pub stale_blob_files: usize,
    pub stale_blob_bytes: u64,
    pub obsolete_blob_files: usize,
    pub obsolete_blob_bytes: u64,
    pub blob_gc_runs: u64,
    pub blob_gc_input_bytes: u64,
    pub blob_gc_output_bytes: u64,
    pub blob_gc_discarded_bytes: u64,
    pub blob_read_count: u64,
    pub blob_read_bytes: u64,
    pub compaction_runs: u64,
    pub compaction_input_tables: u64,
    pub compaction_output_tables: u64,
    pub compaction_input_bytes: u64,
    pub compaction_output_bytes: u64,
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    pub filters: FilterStats,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LevelStats {
    pub level: u32,
    pub tables: usize,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FilterStats {
    pub table_point_hits: u64,
    pub table_point_misses: u64,
    pub table_point_false_positives: u64,
    pub table_prefix_hits: u64,
    pub table_prefix_misses: u64,
    pub table_prefix_false_positives: u64,
    pub block_point_hits: u64,
    pub block_point_misses: u64,
    pub block_point_false_positives: u64,
    pub block_prefix_hits: u64,
    pub block_prefix_misses: u64,
    pub block_prefix_false_positives: u64,
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
