use super::{
    BucketOptions, CompactionTrigger, Db, DbOptions, FilterPolicy, KeyRange, LARGE_VALUE_BYTES,
    Path, PathBuf, PrefixExtractor, PrefixFilterPolicy, SystemTime, UNIX_EPOCH,
    benchmark_persistent_options, fs,
};

pub(super) fn populated_memory_db(rows: usize) -> Db {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    db
}

pub(super) fn populated_delta_memory_db(rows: usize) -> Db {
    let mut options = DbOptions::memory();
    options.write_buffer_bytes = 1;
    let db = Db::open_sync(options).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    assert_delta_backed_memory_stats(&db);
    db
}

pub(super) fn populated_active_memtable_db(
    name: &str,
    rows: usize,
) -> (PathBuf, Db, trine_kv::Bucket) {
    let dir = temp_dir(name);
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = 0;
    options.write_buffer_bytes = 64 * 1024 * 1024;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    assert_active_memtable_stats(&db);
    (dir, db, bucket)
}

pub(super) fn assert_delta_backed_memory_stats(db: &Db) {
    let stats = db.stats();
    assert!(
        stats.memtable_bytes > 0,
        "delta-backed benchmark must keep recent write bytes in memory stats"
    );
    assert_eq!(
        stats.immutable_memtables, 0,
        "delta-backed benchmark must not use immutable memtable queues"
    );
    assert_eq!(
        stats.total_tables, 0,
        "delta-backed benchmark must stay in memory"
    );
}

pub(super) fn assert_active_memtable_stats(db: &Db) {
    let stats = db.stats();
    assert!(
        stats.memtable_bytes > 0,
        "active memtable benchmark must keep recent write bytes in memory stats"
    );
    assert_eq!(
        stats.immutable_memtables, 0,
        "active memtable benchmark must avoid freeze/flush work"
    );
    assert_eq!(
        stats.total_tables, 0,
        "active memtable benchmark must avoid table reads"
    );
}

pub(super) fn populated_prefix_db(rows: usize, filters: bool) -> Db {
    let mut options = DbOptions::memory();
    options.default_bucket_options = prefix_options(filters);
    let db = Db::open_sync(options).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(prefix_key(index), value(index))
            .expect("put succeeds");
    }
    db
}

pub(super) fn random_get_checksum(
    bucket: &trine_kv::Bucket,
    rows: usize,
    ops: usize,
    mut seed: u64,
) -> u64 {
    let mut checksum = 0;
    for _ in 0..ops {
        seed = xorshift(seed);
        let index = seed_index(seed, rows);
        checksum += bucket
            .get_sync(&key(index))
            .expect("get succeeds")
            .map_or(0, |value| value.len() as u64);
    }
    checksum
}

pub(super) fn sequential_point_batch_checksum(bucket: &trine_kv::Bucket, keys: &[Vec<u8>]) -> u64 {
    let mut checksum = 0;
    for key in keys {
        checksum += bucket
            .get_sync(key)
            .expect("sequential batch point read succeeds")
            .map_or(0, |value| value.len() as u64);
    }
    checksum
}

pub(super) fn batched_point_read_checksum(
    bucket: &trine_kv::Bucket,
    keys: &[Vec<u8>],
    batch_size: usize,
) -> u64 {
    let mut checksum = 0;
    for batch in keys.chunks(batch_size) {
        checksum += bucket
            .get_many_sync(batch)
            .expect("batched point read succeeds")
            .into_iter()
            .map(|value| value.map_or(0, |value| value.len() as u64))
            .sum::<u64>();
    }
    checksum
}

pub(super) fn point_read_keys(rows: usize, ops: usize, mut seed: u64) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(ops);
    for _ in 0..ops {
        seed = xorshift(seed);
        keys.push(key(seed_index(seed, rows)));
    }
    keys
}

pub(super) fn localized_point_read_keys(rows: usize, ops: usize) -> Vec<Vec<u8>> {
    (0..ops).map(|index| key(index % rows)).collect()
}

pub(super) fn missing_point_read_keys(ops: usize) -> Vec<Vec<u8>> {
    (0..ops)
        .map(|index| format!("missing-{index:04}").into_bytes())
        .collect()
}

pub(super) fn bounded_missing_point_read_keys(rows: usize, ops: usize) -> Vec<Vec<u8>> {
    assert!(
        rows > 1,
        "bounded missing keys need at least two table keys"
    );
    (0..ops)
        .map(|index| {
            let key_index = index % (rows - 1);
            format!("key-{key_index:08}!missing").into_bytes()
        })
        .collect()
}

pub(super) fn range_scan_checksum(bucket: &trine_kv::Bucket, scans: usize) -> u64 {
    let mut checksum = 0;
    for start in 0..scans {
        let end = start + 32;
        let iter = bucket
            .range_sync(&KeyRange::half_open(key(start), key(end)))
            .expect("range succeeds");
        checksum += iter
            .map(|item| item.expect("range item").value.len() as u64)
            .sum::<u64>();
    }
    checksum
}

pub(super) fn prefix_scan_checksum(bucket: &trine_kv::Bucket, scans: usize, missing: bool) -> u64 {
    let mut checksum = 0;
    for tenant in 0..scans {
        let prefix = if missing {
            format!("missing:{tenant:02}:")
        } else {
            format!("tenant:{:02}:", tenant % 16)
        };
        let iter = bucket
            .prefix_sync(prefix.as_bytes())
            .expect("prefix succeeds");
        checksum += iter
            .map(|item| item.expect("prefix item").value.len() as u64)
            .sum::<u64>();
    }
    checksum
}

pub(super) fn flushed_persistent_db(
    name: &str,
    rows: usize,
    bucket_options: BucketOptions,
) -> (PathBuf, Db, trine_kv::Bucket) {
    let dir = temp_dir(name);
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = bucket_options;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");
    (dir, db, bucket)
}

pub(super) fn large_blob_db(name: &str, rows: usize) -> (PathBuf, Db, trine_kv::Bucket) {
    let dir = temp_dir(name);
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = large_blob_options();
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), large_value(index))
            .expect("large put succeeds");
    }
    db.flush_sync().expect("large flush succeeds");
    (dir, db, bucket)
}

pub(super) fn large_blob_options() -> BucketOptions {
    BucketOptions {
        blob_threshold_bytes: 4 * 1024,
        ..BucketOptions::default()
    }
}

pub(super) fn prefix_options(filters: bool) -> BucketOptions {
    BucketOptions {
        prefix_extractor: PrefixExtractor::Separator(b':'),
        prefix_filter_policy: if filters {
            PrefixFilterPolicy::Bloom { bits_per_prefix: 8 }
        } else {
            PrefixFilterPolicy::Disabled
        },
        filter_policy: if filters {
            FilterPolicy::Bloom { bits_per_key: 10 }
        } else {
            FilterPolicy::Disabled
        },
        ..BucketOptions::default()
    }
}

pub(super) fn key(index: usize) -> Vec<u8> {
    format!("key-{index:08}").into_bytes()
}

pub(super) fn prefix_key(index: usize) -> Vec<u8> {
    format!("tenant:{:02}:key-{index:08}", index % 16).into_bytes()
}

pub(super) fn long_shared_prefix_key(index: usize) -> Vec<u8> {
    format!("tenant:analytics:region:us-west-2:dataset:events:shard:000000:key-{index:08}")
        .into_bytes()
}

pub(super) fn value(index: usize) -> Vec<u8> {
    format!("value-{index:08}-{}", index.wrapping_mul(31)).into_bytes()
}

pub(super) fn large_value(index: usize) -> Vec<u8> {
    let mut seed = (index as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(0x1234_5678_9abc_def0);
    let mut bytes = Vec::with_capacity(LARGE_VALUE_BYTES);
    while bytes.len() < LARGE_VALUE_BYTES {
        seed = xorshift(seed);
        bytes.extend_from_slice(&seed.to_le_bytes());
    }
    bytes.truncate(LARGE_VALUE_BYTES);
    bytes
}

pub(super) fn repeated_bytes(prefix: &[u8], len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        bytes.extend_from_slice(prefix);
    }
    bytes.truncate(len);
    bytes
}

pub(super) fn xorshift(mut value: u64) -> u64 {
    value ^= value << 13;
    value ^= value >> 7;
    value ^ (value << 17)
}

pub(super) fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "trine-kv-bench-{name}-{}-{nonce}",
        std::process::id()
    ))
}

pub(super) fn seed_index(seed: u64, len: usize) -> usize {
    let len = u64::try_from(len).expect("length fits in u64");
    usize::try_from(seed % len).expect("seed modulo length fits in usize")
}

pub(super) fn cleanup_dir(dir: &Path) {
    if let Err(error) = fs::remove_dir_all(dir) {
        if error.kind() != std::io::ErrorKind::NotFound {
            eprintln!("failed to remove {}: {error}", dir.display());
        }
    }
}

pub(super) fn labelled(name: &'static str, label: &'static str) -> &'static str {
    Box::leak(format!("{name} {label}").into_boxed_str())
}

pub(super) fn labelled3(
    name: &'static str,
    first: &'static str,
    second: &'static str,
) -> &'static str {
    Box::leak(format!("{name} {first} {second}").into_boxed_str())
}

pub(super) fn labelled_level(name: &'static str, level: u32, label: &'static str) -> &'static str {
    Box::leak(format!("{name} level {level} {label}").into_boxed_str())
}

pub(super) fn labelled_trigger(
    name: &'static str,
    trigger: CompactionTrigger,
    label: &'static str,
) -> &'static str {
    Box::leak(
        format!(
            "{name} trigger {} {label}",
            compaction_trigger_label(trigger)
        )
        .into_boxed_str(),
    )
}

pub(super) const fn compaction_trigger_label(trigger: CompactionTrigger) -> &'static str {
    match trigger {
        CompactionTrigger::L0Overlap => "l0-overlap",
        CompactionTrigger::LevelSize => "level-size",
        CompactionTrigger::MultiTableLevel => "multi-table-level",
        CompactionTrigger::TombstoneDebt => "tombstone-debt",
    }
}
