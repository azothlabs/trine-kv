use std::{
    fs,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use trine_kv::{
    BlobGcRatio, BlobLevelMergePolicy, BucketOptions, CompactionSkip, CompactionTrigger,
    CompressionProfile, Db,
    DbOptions, DurabilityMode, Error, FailOnCorruptionPolicy, FilterDepthCurve, FilterPolicy,
    IndexSearchPolicy, KeyRange, MaintenanceBudget, PrefixExtractor, PrefixFilterPolicy,
    TransactionOptions,
    WriteBatch, WriteOptions, blob, codec::CodecId, manifest, recovery, table, wal,
    write_batch::BatchOperation,
};

use crate::types::Sequence;

fn temp_db_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("trine-kv-{name}-{}-{nonce}", std::process::id()))
}

fn flushed_default_table_path(path: &std::path::Path, options: &DbOptions) -> PathBuf {
    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write a");
        db.flush_sync().expect("flush table");
    }

    let manifest_state =
        manifest::read_manifest(&manifest::manifest_path(path)).expect("manifest reads");
    let table_id = manifest_state
        .tables()
        .get("default")
        .and_then(|tables| tables.first())
        .expect("default table exists")
        .id;
    table::table_path(path, table_id)
}

fn corrupt_first_data_block_payload(table_path: &std::path::Path) {
    let mut bytes = fs::read(table_path).expect("read table");
    let encoded_byte_offset = 14 + 13;
    let byte = bytes
        .get_mut(encoded_byte_offset)
        .expect("table has a first data block payload byte");
    *byte ^= 0xff;
    fs::write(table_path, bytes).expect("write corrupted table");
}

fn collect_rows(iter: trine_kv::Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|item| {
        let item = item.expect("iterator item reads");
        (item.key, item.value)
    })
    .collect()
}

fn blob_file_paths(path: &std::path::Path) -> Vec<PathBuf> {
    let mut paths = fs::read_dir(path)
        .expect("read test db directory")
        .map(|entry| entry.expect("read directory entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("blob-"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn table_file_paths(path: &std::path::Path) -> Vec<PathBuf> {
    fs::read_dir(path)
        .expect("read test db directory")
        .map(|entry| entry.expect("read directory entry").path())
        .filter(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension == table::TABLE_FILE_EXTENSION)
        })
        .collect()
}

fn default_table_levels(path: &std::path::Path) -> Vec<u32> {
    let manifest_state =
        manifest::read_manifest(&manifest::manifest_path(path)).expect("manifest reads");
    let mut levels = manifest_state
        .tables()
        .get("default")
        .expect("default table list")
        .iter()
        .map(|properties| properties.level.get())
        .collect::<Vec<_>>();
    levels.sort_unstable();
    levels
}

fn default_table_ids(path: &std::path::Path) -> Vec<u64> {
    let manifest_state =
        manifest::read_manifest(&manifest::manifest_path(path)).expect("manifest reads");
    let mut ids = manifest_state
        .tables()
        .get("default")
        .expect("default table list")
        .iter()
        .map(|properties| properties.id.get())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn level_table_count(stats: &trine_kv::DbStats, level: u32) -> usize {
    stats
        .level_tables
        .iter()
        .find(|level_stats| level_stats.level == level)
        .map_or(0, |level_stats| level_stats.tables)
}

fn level_table_bytes(stats: &trine_kv::DbStats, level: u32) -> u64 {
    stats
        .level_tables
        .iter()
        .find(|level_stats| level_stats.level == level)
        .map_or(0, |level_stats| level_stats.bytes)
}

fn compaction_trigger_runs(stats: &trine_kv::DbStats, trigger: CompactionTrigger) -> u64 {
    stats
        .compaction_triggers
        .iter()
        .find(|trigger_stats| trigger_stats.trigger == trigger)
        .map_or(0, |trigger_stats| trigger_stats.runs)
}

fn write_file(path: &std::path::Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent directory");
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("open test file");
    file.write_all(bytes).expect("write test file");
}

fn wait_until(label: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..100 {
        if condition() {
            return;
        }
        thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!("timed out waiting for {label}");
}

fn corruption_message(error: Error) -> String {
    match error {
        Error::Corruption { message } => message,
        other => panic!("expected corruption error, got {other:?}"),
    }
}

#[path = "persistent_wal/recovery.rs"]
mod recovery_tests;
#[path = "persistent_wal/flush_memtable.rs"]
mod flush_memtable;
#[path = "persistent_wal/compaction.rs"]
mod compaction;
#[path = "persistent_wal/read_stats.rs"]
mod read_stats;
#[path = "persistent_wal/blob_gc.rs"]
mod blob_gc;
#[path = "persistent_wal/corruption.rs"]
mod corruption;
#[path = "persistent_wal/durability.rs"]
mod durability;
#[path = "persistent_wal/destructive.rs"]
mod destructive;
