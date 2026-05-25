#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbStats {
    pub live_keyspaces: usize,
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
    pub obsolete_blob_files: usize,
    pub obsolete_blob_bytes: u64,
    pub compaction_runs: u64,
    pub compaction_input_tables: u64,
    pub compaction_output_tables: u64,
    pub compaction_input_bytes: u64,
    pub compaction_output_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LevelStats {
    pub level: u32,
    pub tables: usize,
    pub bytes: u64,
}
