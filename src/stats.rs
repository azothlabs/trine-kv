#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbStats {
    pub live_keyspaces: usize,
    pub active_snapshots: usize,
    pub memtable_bytes: u64,
    pub immutable_memtables: usize,
    pub l0_tables: usize,
    pub wal_bytes_pending_sync: u64,
}
