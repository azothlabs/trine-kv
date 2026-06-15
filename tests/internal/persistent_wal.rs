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

#[test]
fn persistent_api_helpers_cover_open_options_and_bucket_writes() {
    let path = temp_db_path("api-helpers");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    let bucket_options =
        BucketOptions::default().with_prefix_extractor(PrefixExtractor::Separator(b':'));
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        let put_info = bucket
            .put_with_options_sync(b"user:001", b"Ada", WriteOptions::sync_all())
            .expect("put with options commits");
        assert_eq!(put_info.sequence(), Sequence::new(1));

        bucket
            .put_with_options_sync(b"user:002", b"Lin", WriteOptions::flush())
            .expect("second put commits");
        bucket
            .delete_with_options_sync(b"user:002", WriteOptions::sync_data())
            .expect("delete with options commits");
        bucket
            .delete_range_with_options_sync(
                KeyRange::half_open(b"unused:000", b"unused:999"),
                WriteOptions::buffered(),
            )
            .expect("range delete with options commits");

        db.flush_sync().expect("flush helper writes table");
    }

    {
        let db = Db::open_sync(DbOptions::persistent_read_only(&path)).expect("read-only db opens");
        let bucket = db.default_bucket_sync().expect("read-only bucket opens");
        assert_eq!(
            bucket.get_sync(b"user:001").expect("user reads"),
            Some(b"Ada".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"user:002").expect("deleted user reads"),
            None
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_replays_point_and_range_batches() {
    let path = temp_db_path("wal-replay");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a");
        bucket.put_sync(b"b", b"b1").expect("write b");
        bucket.put_sync(b"c", b"c1").expect("write c");
        bucket.delete_sync(b"b").expect("delete b");
        bucket
            .delete_range_sync(KeyRange::half_open(b"c", b"d"))
            .expect("range delete c");
        db.persist_sync(DurabilityMode::Flush).expect("flush WAL");
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(db.stats().live_buckets, 1);
        assert_eq!(
            bucket.get_sync(b"a").expect("a replays"),
            Some(b"a1".to_vec())
        );
        assert_eq!(bucket.get_sync(b"b").expect("b delete replays"), None);
        assert_eq!(bucket.get_sync(b"c").expect("range delete replays"), None);

        let mut batch = WriteBatch::new();
        batch.put(b"d", b"d1");
        let info = db
            .write_sync(
                batch,
                WriteOptions {
                    durability: DurabilityMode::Flush,
                },
            )
            .expect("post-replay write commits");
        assert_eq!(info.sequence().get(), 6);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_replays_front_door_accepted_record_without_memory_publish() {
    let path = temp_db_path("front-door-recovery");
    let options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db creates manifest");
        assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
    }

    let mut writer = wal::WalWriter::open_append(&wal::wal_path(&path)).expect("WAL opens");
    writer
        .append_batch(
            Sequence::new(1),
            &[BatchOperation::Put {
                bucket: "default".to_owned(),
                key: b"accepted".to_vec(),
                value: b"from-wal".to_vec(),
            }],
            DurabilityMode::Flush,
        )
        .expect("front-door accepted record is written");

    {
        let db = Db::open_sync(options).expect("persistent db replays WAL-only record");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket
                .get_sync(b"accepted")
                .expect("accepted record replays"),
            Some(b"from-wal".to_vec())
        );
        assert_eq!(db.last_committed_sequence(), Sequence::new(1));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_merges_records_from_wal_shards() {
    let path = temp_db_path("wal-shard-recovery");
    let options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db creates manifest");
        assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
    }

    let mut legacy_writer =
        wal::WalWriter::open_append(&wal::wal_path(&path)).expect("legacy WAL opens");
    legacy_writer
        .append_batch(
            Sequence::new(1),
            &[BatchOperation::Put {
                bucket: "default".to_owned(),
                key: b"a".to_vec(),
                value: b"a1".to_vec(),
            }],
            DurabilityMode::Flush,
        )
        .expect("legacy record writes");
    let mut shard_writer =
        wal::WalWriter::open_append(&wal::wal_shard_path(&path, 1)).expect("WAL shard opens");
    shard_writer
        .append_batch(
            Sequence::new(2),
            &[BatchOperation::Put {
                bucket: "default".to_owned(),
                key: b"b".to_vec(),
                value: b"b1".to_vec(),
            }],
            DurabilityMode::Flush,
        )
        .expect("shard record writes");

    {
        let db = Db::open_sync(options).expect("persistent db replays WAL shards");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket.get_sync(b"a").expect("a replays"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("b replays"),
            Some(b"b1".to_vec())
        );
        assert_eq!(db.last_committed_sequence(), Sequence::new(2));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_writes_route_across_wal_shards_and_recover() {
    let path = temp_db_path("wal-shard-routing");
    let options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        for index in 0..4 {
            db.put_with_options_sync(
                format!("k{index}").as_bytes(),
                format!("v{index}").as_bytes(),
                WriteOptions::flush(),
            )
            .expect("write commits");
        }
        let stats = db.stats();
        assert_eq!(stats.commit_sequences_allocated, 4);
        assert_eq!(stats.commit_visible_sequence, 4);
        assert_eq!(stats.commit_open_slots, 0);
        assert_eq!(stats.wal_shards, wal::DEFAULT_WAL_SHARD_COUNT);
        assert_eq!(stats.wal_open_shards, wal::DEFAULT_WAL_SHARD_COUNT);
        assert!(stats.wal_queue_capacity > 0);
        assert_eq!(stats.wal_records_accepted, 4);
        assert!(stats.wal_bytes_accepted > 0);
        assert!(wal::wal_shard_path(&path, 1).exists());
        assert_eq!(
            wal::read_all_batches(&path)
                .expect("WAL batches read")
                .into_iter()
                .map(|batch| batch.sequence)
                .collect::<Vec<_>>(),
            vec![
                Sequence::new(1),
                Sequence::new(2),
                Sequence::new(3),
                Sequence::new(4)
            ]
        );
    }

    {
        let db = Db::open_sync(options).expect("persistent db replays sharded WAL");
        for index in 0..4 {
            assert_eq!(
                db.get_sync(format!("k{index}").as_bytes())
                    .expect("value replays"),
                Some(format!("v{index}").into_bytes())
            );
        }
        assert_eq!(db.last_committed_sequence(), Sequence::new(4));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_replays_cross_bucket_batch() {
    let path = temp_db_path("cross-bucket");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        db.bucket_sync("users").expect("users bucket opens");
        db.bucket_sync("posts").expect("posts bucket opens");

        let mut batch = WriteBatch::new();
        batch
            .put_bucket("users", b"1", b"ada")
            .expect("stage users write");
        batch
            .put_bucket("posts", b"1", b"hello")
            .expect("stage posts write");
        db.write_sync(
            batch,
            WriteOptions {
                durability: DurabilityMode::Flush,
            },
        )
        .expect("cross-bucket batch commits");
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let users = db.bucket_sync("users").expect("users bucket reopens");
        let posts = db.bucket_sync("posts").expect("posts bucket reopens");

        assert_eq!(
            users.get_sync(b"1").expect("users replay"),
            Some(b"ada".to_vec())
        );
        assert_eq!(
            posts.get_sync(b"1").expect("posts replay"),
            Some(b"hello".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_manifest_keeps_bucket_options_across_reopen() {
    let path = temp_db_path("manifest-bucket-options");
    let options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        allow_empty_keys: false,
        compression: CompressionProfile::Fast,
        block_bytes: 4096,
        filter_policy: FilterPolicy::Bloom { bits_per_key: 12 },
        prefix_extractor: PrefixExtractor::Separator(b':'),
        prefix_filter_policy: PrefixFilterPolicy::Bloom { bits_per_prefix: 8 },
        index_search_policy: IndexSearchPolicy::Binary,
        blob_threshold_bytes: 128 * 1024,
        blob_level_merge_policy: BlobLevelMergePolicy::Always,
        filter_depth_curve: FilterDepthCurve::Auto,
    };

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db
            .bucket_with_options_sync("users", bucket_options.clone())
            .expect("bucket opens");

        bucket.put_sync(b"user:1", b"ada").expect("write user row");
        db.persist_sync(DurabilityMode::Flush).expect("flush WAL");
    }

    let manifest_state =
        manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
    assert_eq!(manifest_state.wal_replay_floor(), Sequence::ZERO);
    assert_eq!(manifest_state.buckets().get("users"), Some(&bucket_options));

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        assert_eq!(db.stats().live_buckets, 2);

        let bucket = db
            .bucket_with_options_sync("users", bucket_options)
            .expect("bucket reopens with manifest options");
        assert_eq!(
            bucket.get_sync(b"user:1").expect("user row replays"),
            Some(b"ada".to_vec())
        );

        let error = db
            .bucket_sync("users")
            .expect_err("wrong bucket options are rejected");
        assert!(matches!(error, Error::InvalidOptions { .. }));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_writer_open_fails_when_directory_lock_is_held() {
    let path = temp_db_path("writer-lock-held");
    let options = DbOptions::persistent(&path);
    let lock_path = path.join("LOCK");

    let db = Db::open_sync(options.clone()).expect("first writer opens");
    assert!(lock_path.exists());

    let message = corruption_message(
        Db::open_sync(options.clone()).expect_err("second writer must fail closed"),
    );
    assert!(message.contains("database lock is already held"));
    assert!(
        lock_path.exists(),
        "failed writer open should leave the owner lock untouched"
    );

    db.close_sync();
    assert!(
        !lock_path.exists(),
        "close should release the writer directory lock"
    );

    let reopened = Db::open_sync(options).expect("writer reopens after close");
    drop(reopened);
    assert!(
        !lock_path.exists(),
        "dropping the final writer handle should release the directory lock"
    );

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_writer_open_fails_closed_on_existing_lock_file() {
    let path = temp_db_path("writer-lock-stale");
    let options = DbOptions::persistent(&path);
    let lock_path = path.join("LOCK");
    write_file(&lock_path, b"pid=stale\n");

    let message = corruption_message(
        Db::open_sync(options).expect_err("existing lock file must fail closed"),
    );
    assert!(message.contains("database lock is already held"));
    assert_eq!(
        fs::read(&lock_path).expect("stale lock remains readable"),
        b"pid=stale\n"
    );
    assert!(!recovery::recovery_report_path(&path).exists());

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_read_only_open_does_not_take_writer_lock() {
    let path = temp_db_path("read-only-no-writer-lock");
    let options = DbOptions::persistent(&path);
    let lock_path = path.join("LOCK");

    {
        let db = Db::open_sync(options.clone()).expect("writer opens");
        db.put_sync(b"a", b"a1").expect("write row");
        db.persist_sync(DurabilityMode::Flush).expect("flush WAL");
    }

    let mut read_only_options = options.clone();
    read_only_options.read_only = true;
    read_only_options.create_if_missing = false;
    let read_only_db = Db::open_sync(read_only_options).expect("read-only open succeeds");
    assert!(
        !lock_path.exists(),
        "read-only open should not take the writer directory lock"
    );

    let writer = Db::open_sync(options).expect("writer opens while read-only handle exists");
    assert!(lock_path.exists());

    assert_eq!(
        read_only_db.get_sync(b"a").expect("read-only row reads"),
        Some(b"a1".to_vec())
    );

    drop(writer);
    drop(read_only_db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_fails_closed_on_safe_temporary_files_by_default() {
    let path = temp_db_path("recovery-temp-fail-closed");
    let options = DbOptions::persistent(&path);
    let manifest_tmp = manifest::manifest_path(&path).with_extension("tmp");
    write_file(&manifest_tmp, b"partial manifest publish");

    let error = Db::open_sync(options).expect_err("temporary files require explicit repair");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(
        manifest_tmp.exists(),
        "fail-closed recovery should leave evidence untouched"
    );
    assert!(!recovery::recovery_report_path(&path).exists());

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_fails_closed_on_wal_shard_temporary_file_by_default() {
    let path = temp_db_path("recovery-wal-shard-temp-fail-closed");
    let options = DbOptions::persistent(&path);
    let wal_shard_tmp = path.join("trine.wal.shard-0001.tmp");
    write_file(&wal_shard_tmp, b"partial shard WAL rewrite");

    let error = Db::open_sync(options).expect_err("WAL shard temporary file requires repair");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(
        wal_shard_tmp.exists(),
        "fail-closed recovery should leave shard rewrite evidence untouched"
    );
    assert!(!recovery::recovery_report_path(&path).exists());

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_repairs_safe_temporary_files_and_writes_report() {
    let path = temp_db_path("recovery-temp-repair");
    let mut options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write row");
        db.flush_sync().expect("flush table");
    }

    let manifest_tmp = manifest::manifest_path(&path).with_extension("tmp");
    let wal_tmp = path.join(wal::WAL_REWRITE_TMP_FILE_NAME);
    let wal_shard_tmp = path.join("trine.wal.shard-0001.tmp");
    let blob_tmp = path.join("blob-00000000000000000999.tmp");
    let table_tmp = table::table_path(&path, table::TableId(999)).with_extension("tmp");
    write_file(&manifest_tmp, b"partial manifest publish");
    write_file(&wal_tmp, b"partial WAL rewrite");
    write_file(&wal_shard_tmp, b"partial shard WAL rewrite");
    write_file(&blob_tmp, b"partial blob file");
    write_file(&table_tmp, b"partial table file");

    options.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;
    {
        let db = Db::open_sync(options).expect("repair recovery opens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket.get_sync(b"a").expect("row survives repair"),
            Some(b"a1".to_vec())
        );
    }

    assert!(!manifest_tmp.exists());
    assert!(!wal_tmp.exists());
    assert!(!wal_shard_tmp.exists());
    assert!(!blob_tmp.exists());
    assert!(!table_tmp.exists());
    let report = recovery::read_recovery_report(&path).expect("recovery report reads");
    assert_eq!(
        report.repaired_temporary_files(),
        &[
            "MANIFEST.tmp".to_owned(),
            "blob-00000000000000000999.tmp".to_owned(),
            "table-00000000000000000999.tmp".to_owned(),
            "trine.wal.shard-0001.tmp".to_owned(),
            "trine.wal.tmp".to_owned(),
        ]
    );

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_fails_closed_on_unreferenced_table_file() {
    let path = temp_db_path("recovery-unreferenced-table");
    let options = DbOptions::persistent(&path);
    let unreferenced_table_path;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write row");
        db.flush_sync().expect("flush table");

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        let table_id = manifest_state
            .tables()
            .get("default")
            .and_then(|tables| tables.first())
            .expect("default table exists")
            .id;
        unreferenced_table_path = table::table_path(&path, table::TableId(999));
        fs::copy(table::table_path(&path, table_id), &unreferenced_table_path)
            .expect("copy table file");
    }

    let message = corruption_message(
        Db::open_sync(options).expect_err("unreferenced table file must fail closed"),
    );
    assert!(message.contains("unreferenced table/blob files"));
    assert!(message.contains("table-00000000000000000999.trinet"));
    assert!(
        unreferenced_table_path.exists(),
        "startup should leave unreferenced table files for operator review"
    );

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_fails_closed_on_unreferenced_blob_file_even_with_temp_repair_policy() {
    let path = temp_db_path("recovery-unreferenced-blob");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket
            .put_sync(b"a", b"large-value-a-large-value-a".to_vec())
            .expect("write blob value");
        db.flush_sync().expect("flush blob table");
    }

    let unreferenced_blob_path = blob::blob_path(&path, 999);
    write_file(&unreferenced_blob_path, b"unreferenced blob bytes");

    options.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;
    let message = corruption_message(
        Db::open_sync(options).expect_err("unreferenced blob file must fail closed"),
    );
    assert!(message.contains("unreferenced table/blob files"));
    assert!(message.contains("blob-00000000000000000999.trineb"));
    assert!(
        unreferenced_blob_path.exists(),
        "startup should not repair formal blob files automatically"
    );
    assert!(!recovery::recovery_report_path(&path).exists());

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_cleans_manifest_pending_blob_deletion() {
    let path = temp_db_path("recovery-pending-blob-deletion");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        db.default_bucket_sync().expect("bucket opens");
    }

    let pending_blob_path = blob::blob_path(&path, 999);
    write_file(&pending_blob_path, b"pending obsolete blob bytes");
    let mut manifest_store =
        manifest::ManifestStore::open_or_create(manifest::manifest_path(&path), false)
            .expect("manifest opens");
    manifest_store
        .replace_tables_batch_and_mark_blob_deletions(Vec::new(), vec![999], Sequence::new(7))
        .expect("pending blob deletion is published");

    {
        let _db = Db::open_sync(options.clone()).expect("pending blob deletion is cleaned on open");
        assert!(
            !pending_blob_path.exists(),
            "pending obsolete blob file should be removed during writable open"
        );
        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        assert!(
            manifest_state.pending_blob_deletions().is_empty(),
            "pending deletion metadata should be cleared after cleanup"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_does_not_delete_referenced_pending_blob_deletion() {
    let path = temp_db_path("recovery-referenced-pending-blob-deletion");
    let mut options = DbOptions::persistent(&path);
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        db.put_sync(b"a", b"large-value-large-value")
            .expect("write large value");
        db.flush_sync().expect("flush blob-backed table");
    }

    let manifest_state =
        manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
    let blob_id = manifest_state
        .tables()
        .get("default")
        .and_then(|tables| tables.first())
        .and_then(|table| table.blob_file_ids().first())
        .copied()
        .expect("table references blob file");
    let blob_path = blob::blob_path(&path, blob_id);

    let mut manifest_store =
        manifest::ManifestStore::open_or_create(manifest::manifest_path(&path), false)
            .expect("manifest opens");
    manifest_store
        .replace_tables_batch_and_mark_blob_deletions(Vec::new(), vec![blob_id], Sequence::new(7))
        .expect("conflicting pending blob deletion is published");

    {
        let db = Db::open_sync(options).expect("db opens despite conflicting pending deletion");
        assert_eq!(
            db.get_sync(b"a").expect("referenced blob still reads"),
            Some(b"large-value-large-value".to_vec())
        );
        assert!(
            blob_path.exists(),
            "referenced pending blob file must remain on disk"
        );
        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        assert!(
            manifest_state
                .pending_blob_deletions()
                .contains_key(&blob_id),
            "conflicting pending deletion should remain for later repair"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_fault_injection_matrix_fails_closed() {
    #[derive(Debug, Clone, Copy)]
    enum Fault {
        ManifestTempPublish,
        MissingReferencedTable,
        MissingReferencedBlob,
        CorruptReferencedBlob,
        UnreferencedFormalBlob,
    }

    for fault in [
        Fault::ManifestTempPublish,
        Fault::MissingReferencedTable,
        Fault::MissingReferencedBlob,
        Fault::CorruptReferencedBlob,
        Fault::UnreferencedFormalBlob,
    ] {
        let path = temp_db_path(&format!("recovery-fault-{fault:?}"));
        let mut options = DbOptions::persistent(&path);
        options.default_bucket_options = BucketOptions {
            blob_threshold_bytes: 8,
            ..BucketOptions::default()
        };

        {
            let db = Db::open_sync(options.clone()).expect("persistent db opens");
            db.put_sync(b"a", b"large-value-a-large-value-a".to_vec())
                .expect("write large value");
            db.flush_sync().expect("flush table and blob file");
        }

        match fault {
            Fault::ManifestTempPublish => {
                write_file(
                    &manifest::manifest_path(&path).with_extension("tmp"),
                    b"partial manifest publish",
                );
            }
            Fault::MissingReferencedTable => {
                let table_id = default_table_ids(&path)
                    .into_iter()
                    .next()
                    .expect("manifest table id exists");
                fs::remove_file(table::table_path(&path, table::TableId(table_id)))
                    .expect("remove referenced table");
            }
            Fault::MissingReferencedBlob => {
                let blob_path = blob_file_paths(&path)
                    .into_iter()
                    .next()
                    .expect("referenced blob exists");
                fs::remove_file(blob_path).expect("remove referenced blob");
            }
            Fault::CorruptReferencedBlob => {
                let blob_path = blob_file_paths(&path)
                    .into_iter()
                    .next()
                    .expect("referenced blob exists");
                let mut bytes = fs::read(&blob_path).expect("read referenced blob");
                bytes[8] ^= 0xff;
                fs::write(blob_path, bytes).expect("write corrupt referenced blob");
            }
            Fault::UnreferencedFormalBlob => {
                write_file(&blob::blob_path(&path, 999), b"unreferenced blob bytes");
            }
        }

        assert!(
            Db::open_sync(options).is_err(),
            "recovery fault {fault:?} should fail closed"
        );
        fs::remove_dir_all(path).expect("cleanup test db");
    }
}

#[test]
fn persistent_recovery_fails_closed_on_malformed_formal_storage_file_name() {
    let path = temp_db_path("recovery-malformed-storage-file");
    let options = DbOptions::persistent(&path);
    let malformed_table_path = path.join("table-not-a-number.trinet");

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        db.default_bucket_sync().expect("bucket opens");
    }

    write_file(&malformed_table_path, b"not a valid table file");

    let message = corruption_message(
        Db::open_sync(options).expect_err("malformed table file must fail closed"),
    );
    assert!(message.contains("invalid table file name"));
    assert!(
        malformed_table_path.exists(),
        "startup should leave malformed formal files for operator review"
    );

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_fails_closed_on_malformed_wal_shard_file_name() {
    let path = temp_db_path("recovery-malformed-wal-shard");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        db.put_sync(b"k", b"v").expect("write commits");
    }

    let malformed_wal_shard = path.join("trine.wal.shard-bad");
    write_file(&malformed_wal_shard, b"not a valid WAL shard file");

    let message = corruption_message(
        Db::open_sync(options).expect_err("malformed WAL shard file must fail closed"),
    );
    assert!(message.contains("malformed WAL shard file name"));
    assert!(
        malformed_wal_shard.exists(),
        "startup should leave malformed WAL shard files for operator review"
    );

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_rejects_bucket_missing_from_manifest() {
    let path = temp_db_path("wal-missing-manifest-bucket");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.bucket_sync("users").expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write a");
        db.persist_sync(DurabilityMode::Flush).expect("flush WAL");
    }

    fs::remove_file(manifest::manifest_path(&path)).expect("remove manifest");

    let error = Db::open_sync(options).expect_err("WAL cannot recreate a missing manifest bucket");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_writes_table_and_reopen_can_skip_wal() {
    let path = temp_db_path("flush-table");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a");
        bucket.put_sync(b"b", b"b1").expect("write b");
        bucket.put_sync(b"c", b"c1").expect("write c");
        bucket.delete_sync(b"b").expect("delete b");
        bucket
            .delete_range_sync(KeyRange::half_open(b"c", b"d"))
            .expect("range delete c");

        db.flush_sync().expect("flush memtable to table");
        assert_eq!(
            bucket.get_sync(b"a").expect("a reads from table"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("b delete reads from table"),
            None
        );
        assert_eq!(
            bucket
                .get_sync(b"c")
                .expect("range delete reads from table"),
            None
        );
    }

    let manifest_state =
        manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
    assert_eq!(manifest_state.wal_replay_floor(), Sequence::new(5));
    let tables = manifest_state
        .tables()
        .get("default")
        .expect("default table list");
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].level.get(), 0);
    assert!(table::table_path(&path, tables[0].id).exists());
    assert!(
        wal::read_all_batches(&path)
            .expect("WAL reads after checkpoint")
            .is_empty(),
        "flushed batches should not remain in the WAL"
    );

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens from table");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(
            bucket.get_sync(b"a").expect("a reads after reopen"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("b delete reads after reopen"),
            None
        );
        assert_eq!(
            bucket
                .get_sync(b"c")
                .expect("range delete reads after reopen"),
            None
        );

        let mut batch = WriteBatch::new();
        batch.put(b"d", b"d1");
        let info = db
            .write_sync(
                batch,
                WriteOptions {
                    durability: DurabilityMode::Flush,
                },
            )
            .expect("post-table write commits");
        assert_eq!(info.sequence(), Sequence::new(6));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_writes_blob_index_file_and_reopen_reads_large_values() {
    let path = temp_db_path("flush-blob-index");
    let mut options = DbOptions::persistent(&path);
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    let large_value = b"large-value-a-large-value-a".to_vec();

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket
            .put_sync(b"small", b"tiny")
            .expect("write small value");
        bucket
            .put_sync(b"large", large_value.clone())
            .expect("write large value");
        db.flush_sync().expect("flush table and blob file");

        let blob_paths = blob_file_paths(&path);
        assert_eq!(blob_paths.len(), 1);
        let blob_bytes = fs::read(&blob_paths[0]).expect("read blob file");
        let blob_file = blob::decode_blob_file(&blob_bytes).expect("blob file decodes");
        assert_eq!(blob_file.properties.record_count, 1);
        assert_eq!(
            blob_file.records[0].record.internal_key.user_key(),
            b"large"
        );
        assert_eq!(blob_file.records[0].record.value, large_value);

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        let table_properties = manifest_state
            .tables()
            .get("default")
            .and_then(|tables| tables.first())
            .expect("default table properties exist");
        assert_eq!(
            table_properties.blob_file_ids(),
            &[blob_file.header.file_id]
        );
        let references = table_properties.blob_references();
        assert_eq!(references.len(), 1);
        assert_eq!(references[0].file_id, blob_file.header.file_id);
        assert_eq!(references[0].referenced_record_count, 1);
        assert_eq!(references[0].referenced_bytes, large_value.len() as u64);

        assert_eq!(
            bucket.get_sync(b"large").expect("large reads after flush"),
            Some(large_value.clone())
        );
        let stats = db.stats();
        assert_eq!(stats.blob_read_count, 1);
        assert_eq!(stats.blob_read_bytes, large_value.len() as u64);
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket.get_sync(b"large").expect("large reads after reopen"),
            Some(large_value)
        );
        assert_eq!(
            bucket.get_sync(b"small").expect("small reads after reopen"),
            Some(b"tiny".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_reopen_fails_on_corrupt_referenced_blob_file() {
    let path = temp_db_path("corrupt-referenced-blob");
    let mut options = DbOptions::persistent(&path);
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket
            .put_sync(b"large", b"large-value-a-large-value-a".to_vec())
            .expect("write large value");
        db.flush_sync().expect("flush blob table");
    }

    let blob_path = blob_file_paths(&path)
        .into_iter()
        .next()
        .expect("blob file exists");
    let mut bytes = fs::read(&blob_path).expect("read blob file");
    let byte = bytes.get_mut(8).expect("blob file has header bytes");
    *byte ^= 0xff;
    fs::write(&blob_path, bytes).expect("write corrupted blob file");

    let error = Db::open_sync(options).expect_err("corrupt referenced blob must fail closed");
    assert!(matches!(
        error,
        Error::Corruption { .. } | Error::InvalidFormat { .. }
    ));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_write_buffer_freezes_active_memtable_and_reads_immutable() {
    let path = temp_db_path("write-buffer-freeze");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"user:1", b"ada").expect("write user");

        let stats = db.stats();
        assert_eq!(stats.immutable_memtables, 1);
        assert_eq!(stats.total_tables, 0);
        assert_eq!(
            bucket
                .get_sync(b"user:1")
                .expect("point read sees immutable"),
            Some(b"ada".to_vec())
        );
        assert_eq!(
            collect_rows(bucket.range_sync(&KeyRange::all()).expect("range reads")),
            vec![(b"user:1".to_vec(), b"ada".to_vec())]
        );
        assert_eq!(
            collect_rows(bucket.prefix_sync(b"user:").expect("prefix reads")),
            vec![(b"user:1".to_vec(), b"ada".to_vec())]
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_write_buffer_freezes_only_large_bucket() {
    let path = temp_db_path("write-buffer-bucket-local-freeze");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 40;
    options.max_immutable_memtables = 4;
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let cold = db.bucket_sync("cold").expect("cold bucket opens");
        let hot = db.bucket_sync("hot").expect("hot bucket opens");

        cold.put_sync(b"c", b"v").expect("cold write stays active");
        assert_eq!(db.stats().immutable_memtables, 0);

        hot.put_sync(b"h", vec![b'x'; 80])
            .expect("hot write freezes hot bucket");
        let stats = db.stats();
        assert_eq!(stats.immutable_memtables, 1);
        assert_eq!(stats.total_tables, 0);
        assert_eq!(
            cold.get_sync(b"c").expect("cold active row reads"),
            Some(b"v".to_vec())
        );
        assert_eq!(
            hot.get_sync(b"h").expect("hot immutable row reads"),
            Some(vec![b'x'; 80])
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_immutable_pressure_flushes_only_pressure_buckets() {
    let path = temp_db_path("immutable-pressure-bucket-local-flush");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 2;
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let cold = db.bucket_sync("cold").expect("cold bucket opens");
        let hot = db.bucket_sync("hot").expect("hot bucket opens");

        cold.put_sync(b"cold", b"c1")
            .expect("cold write freezes once");
        hot.put_sync(b"h1", b"v1").expect("hot write freezes once");
        hot.put_sync(b"h2", b"v2")
            .expect("hot reaches immutable pressure");
        assert_eq!(db.stats().immutable_memtables, 3);
        assert_eq!(db.stats().total_tables, 0);

        hot.put_sync(b"h3", b"v3")
            .expect("hot pressure flushes hot bucket first");
        let stats = db.stats();
        assert_eq!(
            stats.total_tables, 2,
            "only hot immutable memtables should have flushed"
        );
        assert_eq!(
            stats.immutable_memtables, 2,
            "cold immutable plus new hot immutable should remain queued"
        );
        assert_eq!(
            cold.get_sync(b"cold").expect("cold immutable row reads"),
            Some(b"c1".to_vec())
        );
        assert_eq!(
            hot.get_sync(b"h1").expect("flushed hot row reads"),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            hot.get_sync(b"h3").expect("new hot row reads"),
            Some(b"v3".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_immutable_range_tombstone_hides_point_records() {
    let path = temp_db_path("immutable-range-tombstone");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"k1", b"v1").expect("write k1");
        bucket
            .delete_range_sync(KeyRange::half_open(b"k", b"l"))
            .expect("range delete freezes");

        assert_eq!(
            bucket
                .get_sync(b"k1")
                .expect("point read checks immutable tombstone"),
            None
        );
        assert!(collect_rows(bucket.range_sync(&KeyRange::all()).expect("range reads")).is_empty());
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_immutable_pressure_flushes_before_next_write_and_keeps_new_wal_batch() {
    let path = temp_db_path("immutable-pressure-flush");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 1;
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        let first = bucket
            .put_with_options_sync(b"a", b"a1", WriteOptions::sync_all())
            .expect("first write freezes");
        assert_eq!(first.sequence(), Sequence::new(1));
        assert_eq!(db.stats().immutable_memtables, 1);
        assert_eq!(db.stats().total_tables, 0);

        let second = bucket
            .put_with_options_sync(b"b", b"b1", WriteOptions::sync_all())
            .expect("second write flushes pressure first");
        assert_eq!(second.sequence(), Sequence::new(2));

        let stats = db.stats();
        assert_eq!(stats.total_tables, 1);
        assert_eq!(stats.immutable_memtables, 1);
        assert_eq!(
            bucket.get_sync(b"a").expect("flushed row reads"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("new immutable row reads"),
            Some(b"b1".to_vec())
        );

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        assert_eq!(manifest_state.wal_replay_floor(), Sequence::new(1));
        let wal_batches = wal::read_all_batches(&path).expect("WAL reads");
        assert_eq!(
            wal_batches
                .iter()
                .map(|batch| batch.sequence)
                .collect::<Vec<_>>(),
            vec![Sequence::new(2)]
        );
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket.get_sync(b"a").expect("flushed row survives reopen"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("WAL row survives reopen"),
            Some(b"b1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_transaction_conflict_checks_immutable_memtables() {
    let path = temp_db_path("transaction-immutable-conflict");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write first value");

        let mut txn = db.transaction(TransactionOptions::default());
        assert_eq!(
            txn.get_sync(b"a").expect("transaction reads a"),
            Some(b"a1".to_vec())
        );

        bucket
            .put_sync(b"a", b"a2")
            .expect("write conflicting value");
        txn.put(b"b", b"b1");
        let error = txn
            .commit_sync()
            .expect_err("immutable memtable update should conflict");
        assert!(matches!(error, Error::Conflict { .. }));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_publish_failure_removes_unpublished_table_and_blob_files() {
    let path = temp_db_path("flush-publish-cleanup");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;
    let value = b"large-value-a-large-value-a".to_vec();

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket
            .put_sync(b"a", value.clone())
            .expect("write blob value");

        let manifest_tmp_dir = manifest::manifest_path(&path).with_extension("tmp");
        fs::create_dir(&manifest_tmp_dir).expect("block manifest tmp path");

        let error = db.flush_sync().expect_err("manifest publish should fail");
        assert!(matches!(error, Error::Io(_)));
        assert!(
            table_file_paths(&path).is_empty(),
            "failed flush should remove unpublished table files"
        );
        assert!(
            blob_file_paths(&path).is_empty(),
            "failed flush should remove unpublished blob files"
        );
        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("memtable row survives failed flush"),
            Some(value)
        );

        fs::remove_dir(&manifest_tmp_dir).expect("remove manifest tmp blocker");
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_levels_preserve_newer_l0_reads() {
    let path = temp_db_path("compaction-levels");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"old-a").expect("write old a");
        db.flush_sync().expect("flush first L0 table");
        bucket.put_sync(b"b", b"old-b").expect("write b");
        db.flush_sync().expect("flush second L0 table");
        assert_eq!(default_table_levels(&path), vec![0, 0]);

        db.compact_range_sync(KeyRange::all())
            .expect("compact L0 tables");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("compacted a reads"),
            Some(b"old-a".to_vec())
        );

        bucket.put_sync(b"a", b"new-a").expect("write newer L0 a");
        db.flush_sync().expect("flush newer L0 table");
        assert_eq!(default_table_levels(&path), vec![0, 1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("newer L0 a reads"),
            Some(b"new-a".to_vec())
        );

        db.compact_range_sync(KeyRange::all())
            .expect("compact L0 into L1");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("newer a survives second compaction"),
            Some(b"new-a".to_vec())
        );
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("newer L0 a reopens"),
            Some(b"new-a".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("compacted b reopens"),
            Some(b"old-b".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_single_l0_compaction_moves_table_without_rewrite() {
    let path = temp_db_path("single-l0-trivial-move");
    let options = DbOptions::persistent(&path);
    let before_table_ids;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a");
        db.flush_sync().expect("flush L0 table");
        before_table_ids = default_table_ids(&path);
        assert_eq!(default_table_levels(&path), vec![0]);
        assert_eq!(table_file_paths(&path).len(), 1);

        db.compact_range_sync(KeyRange::all())
            .expect("single L0 table moves down");
        assert_eq!(default_table_ids(&path), before_table_ids);
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(table_file_paths(&path).len(), 1);
        assert_eq!(
            bucket.get_sync(b"a").expect("moved table reads"),
            Some(b"a1".to_vec())
        );
        let stats = db.stats();
        assert_eq!(stats.compaction_runs, 1);
        assert_eq!(stats.compaction_input_tables, 1);
        assert_eq!(stats.compaction_output_tables, 1);
        assert!(stats.compaction_input_bytes > 0);
        assert!(stats.compaction_output_bytes > 0);
        assert_eq!(compaction_trigger_runs(&stats, CompactionTrigger::L0Overlap), 1);
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(default_table_ids(&path), before_table_ids);
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("moved table reopens"),
            Some(b"a1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_auto_compacts_when_l0_pressure_exceeds_limit() {
    let path = temp_db_path("auto-compact-l0");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 1;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a");
        db.flush_sync().expect("first flush stays L0");
        assert_eq!(default_table_levels(&path), vec![0]);

        bucket.put_sync(b"b", b"b1").expect("write b");
        db.flush_sync().expect("second flush triggers compaction");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("a reads after auto compaction"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket
                .get_sync(b"b")
                .expect("b reads after auto compaction"),
            Some(b"b1".to_vec())
        );

        bucket.put_sync(b"a", b"a2").expect("write newer a");
        db.flush_sync().expect("new L0 below pressure limit");
        assert_eq!(default_table_levels(&path), vec![0, 1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("newer a reads over L1"),
            Some(b"a2".to_vec())
        );
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(default_table_levels(&path), vec![0, 1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("newer a reopens"),
            Some(b"a2".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("b reopens"),
            Some(b"b1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_auto_compacts_overlapping_l0_below_file_limit() {
    let path = temp_db_path("auto-compact-overlapping-l0");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a1");
        db.flush_sync().expect("first flush stays L0");
        assert_eq!(default_table_levels(&path), vec![0]);

        bucket.put_sync(b"a", b"a2").expect("write a2");
        db.flush_sync()
            .expect("second overlapping flush triggers compaction");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("newer a reads after compaction"),
            Some(b"a2".to_vec())
        );
        assert!(db.stats().compaction_runs > 0);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_tombstone_only_table_hides_range_and_prefix_scans() {
    let path = temp_db_path("tombstone-only-scan-guard");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 64;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"user:1", b"old").expect("write user 1");
        bucket.put_sync(b"user:2", b"old").expect("write user 2");
        bucket.put_sync(b"order:1", b"keep").expect("write order");
        db.flush_sync().expect("flush point table");
        let snapshot = db.snapshot();

        bucket
            .delete_range_sync(KeyRange::all())
            .expect("delete all range");
        db.flush_sync().expect("flush tombstone-only table");

        assert_eq!(bucket.get_sync(b"user:1").expect("point hidden"), None);

        let before_range = db.stats();
        assert_eq!(
            collect_rows(bucket.range_sync(&KeyRange::all()).expect("range hidden")),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
        let after_range = db.stats();
        assert!(
            after_range
                .read_path
                .range_tombstone_table_probes
                .saturating_sub(before_range.read_path.range_tombstone_table_probes)
                > 0,
            "range scans must inspect candidate tombstone tables"
        );

        assert_eq!(
            collect_rows(
                bucket
                    .range_reverse_sync(&KeyRange::all())
                    .expect("reverse range hidden")
            ),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );

        let before_prefix = db.stats();
        assert_eq!(
            collect_rows(bucket.prefix_sync(b"user:").expect("prefix hidden")),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
        let after_prefix = db.stats();
        assert!(
            after_prefix
                .read_path
                .prefix_tombstone_table_probes
                .saturating_sub(before_prefix.read_path.prefix_tombstone_table_probes)
                > 0,
            "prefix scans must inspect candidate tombstone tables"
        );
        assert_eq!(
            collect_rows(
                bucket
                    .prefix_reverse_sync(b"user:")
                    .expect("reverse prefix hidden")
            ),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );

        assert_eq!(
            collect_rows(
                snapshot
                    .range_sync(&bucket, &KeyRange::all())
                    .expect("snapshot still sees old rows")
            ),
            vec![
                (b"order:1".to_vec(), b"keep".to_vec()),
                (b"user:1".to_vec(), b"old".to_vec()),
                (b"user:2".to_vec(), b"old".to_vec()),
            ]
        );
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            collect_rows(bucket.range_sync(&KeyRange::all()).expect("range hidden")),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
        assert_eq!(
            collect_rows(bucket.prefix_sync(b"user:").expect("prefix hidden")),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_budgeted_compaction_reports_exhaustion_and_resumes() {
    let path = temp_db_path("budgeted-compaction-resumes");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let default_bucket = db.default_bucket_sync().expect("default bucket opens");
        let other_bucket = db.bucket_sync("other").expect("other bucket opens");

        default_bucket
            .put_sync(b"a", b"a1")
            .expect("write default key");
        other_bucket.put_sync(b"b", b"b1").expect("write other key");
        db.flush_sync().expect("flush both buckets");
        assert_eq!(db.stats().l0_tables, 2);

        let first = db
            .compact_range_with_budget_sync(KeyRange::all(), MaintenanceBudget::default())
            .expect("first budgeted compaction runs");
        assert_eq!(first.compactions, 1);
        assert!(first.budget_exhausted());
        assert!(!first.busy());
        assert_eq!(db.stats().l0_tables, 1);
        assert_eq!(db.stats().maintenance_budget_exhaustions, 1);

        let second = db
            .compact_range_with_budget_sync(KeyRange::all(), MaintenanceBudget::default())
            .expect("second budgeted compaction resumes");
        assert_eq!(second.compactions, 1);
        assert!(!second.budget_exhausted());
        assert_eq!(db.stats().l0_tables, 0);
        assert_eq!(level_table_count(&db.stats(), 1), 2);
        assert_eq!(
            default_bucket.get_sync(b"a").expect("default key reads"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            other_bucket.get_sync(b"b").expect("other key reads"),
            Some(b"b1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_budgeted_maintenance_flushes_one_input_per_pass() {
    let path = temp_db_path("budgeted-maintenance-flushes");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let default_bucket = db.default_bucket_sync().expect("default bucket opens");
        let other_bucket = db.bucket_sync("other").expect("other bucket opens");

        default_bucket
            .put_sync(b"a", b"a1")
            .expect("write default key");
        other_bucket.put_sync(b"b", b"b1").expect("write other key");
        assert_eq!(db.stats().immutable_memtables, 2);

        let first = db
            .run_maintenance_with_budget_sync(MaintenanceBudget::default())
            .expect("first budgeted maintenance runs");
        assert_eq!(first.flushes, 1);
        assert!(first.budget_exhausted());
        assert_eq!(db.stats().immutable_memtables, 1);
        assert_eq!(db.stats().l0_tables, 1);

        let second = db
            .run_maintenance_with_budget_sync(MaintenanceBudget::default())
            .expect("second budgeted maintenance resumes");
        assert_eq!(second.flushes, 1);
        assert!(!second.budget_exhausted());
        assert_eq!(db.stats().immutable_memtables, 0);
        assert_eq!(db.stats().l0_tables, 2);
        assert_eq!(
            default_bucket.get_sync(b"a").expect("default key reads"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            other_bucket.get_sync(b"b").expect("other key reads"),
            Some(b"b1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_bucket_reader_keeps_memtable_source_after_flush() {
    let path = temp_db_path("bucket-reader-keeps-memtable-source");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write a1");

        let snapshot = db.snapshot();
        let reader = bucket.reader(&snapshot).expect("reader opens");
        db.flush_sync().expect("flush after reader creation");

        let value = reader
            .get_sync(b"a")
            .expect("reader value lookup")
            .expect("value is visible");
        assert_eq!(value.as_bytes(), b"a1");
        assert_eq!(
            reader
                .get_owned_sync(b"a")
                .expect("reader sees pre-flush memtable"),
            Some(b"a1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_background_workers_flush_and_compact_pressure() {
    let path = temp_db_path("background-maintenance");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;
    options.max_l0_files = 2;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a");
        bucket.put_sync(b"b", b"b1").expect("write b");
        bucket.put_sync(b"c", b"c1").expect("write c");
        wait_until("background batched flush of immutable memtables", || {
            let stats = db.stats();
            stats.immutable_memtables == 0 && (stats.l0_tables >= 3 || level_table_count(&stats, 1) >= 1)
        });

        wait_until("background compaction after L0 pressure", || {
            let stats = db.stats();
            stats.l0_tables <= 2 && level_table_count(&stats, 1) >= 1
        });

        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("a reads after background work"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket
                .get_sync(b"b")
                .expect("b reads after background work"),
            Some(b"b1".to_vec())
        );
        assert_eq!(
            bucket
                .get_sync(b"c")
                .expect("c reads after background work"),
            Some(b"c1".to_vec())
        );
        db.close_sync();
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let stats = db.stats();
        assert!(stats.l0_tables <= 2);
        assert!(level_table_count(&stats, 1) >= 1);
        assert_eq!(
            bucket.get_sync(b"a").expect("a reopens"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("b reopens"),
            Some(b"b1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"c").expect("c reopens"),
            Some(b"c1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_background_maintenance_error_surfaces_to_later_write() {
    let path = temp_db_path("background-maintenance-error");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;
    options.background_worker_count = 1;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        let manifest_tmp_dir = manifest::manifest_path(&path).with_extension("tmp");
        fs::create_dir(&manifest_tmp_dir).expect("block manifest tmp path");
        bucket.put_sync(b"a", b"a1").expect("write schedules flush");

        let mut surfaced = false;
        for index in 0..100 {
            thread::sleep(std::time::Duration::from_millis(20));
            let key = format!("probe-{index:03}").into_bytes();
            match bucket.put_sync(key, b"value") {
                Err(Error::Corruption { message })
                    if message.contains("background maintenance failed") =>
                {
                    surfaced = true;
                    break;
                }
                Ok(()) => {}
                Err(error) => panic!("unexpected write error: {error}"),
            }
        }
        assert!(
            surfaced,
            "background maintenance failure should reach a later write"
        );

        fs::remove_dir(&manifest_tmp_dir).expect("remove manifest tmp blocker");
        db.close_sync();
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_splits_outputs_and_moves_overfull_l1_down() {
    let path = temp_db_path("compaction-split-output");
    let mut options = DbOptions::persistent(&path);
    options.target_table_bytes = 240;
    options.level_size_multiplier = 2;
    let bucket_options = BucketOptions {
        compression: CompressionProfile::None,
        block_bytes: 256,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for index in 0..30 {
            let key = format!("key-{index:03}").into_bytes();
            let value = format!("value-{index:03}-{}", "x".repeat(48)).into_bytes();
            bucket.put_sync(key, value).expect("write first batch");
        }
        db.flush_sync().expect("flush first L0 table");
        for index in 30..60 {
            let key = format!("key-{index:03}").into_bytes();
            let value = format!("value-{index:03}-{}", "y".repeat(48)).into_bytes();
            bucket.put_sync(key, value).expect("write second batch");
        }
        db.flush_sync().expect("flush second L0 table");

        db.compact_range_sync(KeyRange::all())
            .expect("manual compaction splits L1 output");
        let levels = default_table_levels(&path);
        assert!(levels.len() > 1, "small target should split output tables");
        assert!(levels.iter().all(|level| *level == 1));

        db.compact_range_sync(KeyRange::all())
            .expect("overfull L1 compacts a narrow input into L2");
        let levels = default_table_levels(&path);
        assert!(
            levels.contains(&1),
            "narrow L1 compaction should leave unrelated L1 tables"
        );
        assert!(levels.contains(&2), "selected L1 input should move to L2");

        for index in [0, 17, 30, 59] {
            let key = format!("key-{index:03}").into_bytes();
            let expected_prefix = format!("value-{index:03}-").into_bytes();
            let value = bucket
                .get_sync(&key)
                .expect("value reads")
                .expect("key exists");
            assert!(value.starts_with(&expected_prefix));
        }
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let levels = default_table_levels(&path);
        assert!(levels.contains(&1));
        assert!(levels.contains(&2));
        assert_eq!(
            bucket.get_sync(b"key-059").expect("latest key reopens"),
            Some(format!("value-059-{}", "y".repeat(48)).into_bytes())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_multi_table_compaction_moves_one_guard_local_input() {
    let path = temp_db_path("multi-table-local-compaction");
    let mut options = DbOptions::persistent(&path);
    options.target_table_bytes = usize::MAX / 4;
    options.max_l0_files = 64;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for chunk in 0..3 {
            let key = format!("key-{chunk:03}").into_bytes();
            let value = format!("value-{chunk:03}").into_bytes();
            bucket.put_sync(key, value).expect("write chunk key");
            db.flush_sync().expect("flush L0 table");
            db.compact_range_sync(KeyRange::all())
                .expect("move flushed table to L1");
        }
        assert_eq!(default_table_levels(&path), vec![1, 1, 1]);

        let before = db.stats();
        db.compact_range_sync(KeyRange::all())
            .expect("multi-table fallback moves one guard-local input");
        let after = db.stats();

        assert_eq!(default_table_levels(&path), vec![1, 1, 2]);
        assert_eq!(
            after
                .compaction_input_tables
                .saturating_sub(before.compaction_input_tables),
            1,
            "guard-local fallback should avoid rewriting every same-level table"
        );
        assert_eq!(
            after
                .compaction_output_tables
                .saturating_sub(before.compaction_output_tables),
            1
        );
        assert_eq!(
            compaction_trigger_runs(&after, CompactionTrigger::MultiTableLevel).saturating_sub(
                compaction_trigger_runs(&before, CompactionTrigger::MultiTableLevel)
            ),
            1
        );

        for chunk in 0..3 {
            let key = format!("key-{chunk:03}").into_bytes();
            let value = bucket
                .get_sync(&key)
                .expect("guard-local compaction reads")
                .expect("key exists");
            assert_eq!(value, format!("value-{chunk:03}").into_bytes());
        }
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(default_table_levels(&path), vec![1, 1, 2]);
        assert_eq!(
            bucket.get_sync(b"key-002").expect("key reopens"),
            Some(b"value-002".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_deep_level_stays_lazy_and_reports_skip() {
    let path = temp_db_path("deep-level-lazy");
    let mut options = DbOptions::persistent(&path);
    // A huge target keeps every level under its byte target so no LevelSize
    // trigger fires and only the non-uniform no-pressure policy is exercised.
    options.target_table_bytes = usize::MAX / 4;
    options.max_l0_files = 64;

    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    // Build three disjoint single-key L1 tables, matching the guard-local setup.
    for chunk in 0..3 {
        let key = format!("key-{chunk:03}").into_bytes();
        let value = format!("value-{chunk:03}").into_bytes();
        bucket.put_sync(key, value).expect("write chunk key");
        db.flush_sync().expect("flush L0 table");
        db.compact_range_sync(KeyRange::all())
            .expect("move flushed table to L1");
    }
    assert_eq!(default_table_levels(&path), vec![1, 1, 1]);

    // Drive the picker until it stabilizes. Shallow L1 keeps merging one table
    // down to L2 (tight budget of 2); once L1 is under budget and L2 holds two
    // non-overlapping tables (deep budget of 3), the policy leaves L2 lazy and
    // records the skip instead of spawning an L3.
    let table_probes_before = point_read_table_probes(&db, &bucket);
    let mut guard = 0;
    loop {
        let before = db.stats();
        db.compact_range_sync(KeyRange::all())
            .expect("compaction step succeeds");
        let after = db.stats();
        let made_progress = after.compaction_output_tables > before.compaction_output_tables;
        guard += 1;
        if !made_progress || guard > 16 {
            break;
        }
    }

    let stats = db.stats();
    let lazy_skips = stats
        .compaction_skips
        .iter()
        .find(|row| row.skip == CompactionSkip::LowerLevelLazy)
        .map_or(0, |row| row.occurrences);
    assert!(
        lazy_skips >= 1,
        "the deep level should be left lazy at least once: {:?}",
        stats.compaction_skips
    );

    // Read amplification does not regress: the deep level stays non-overlapping,
    // so point reads still probe at most one table per level.
    let table_probes_after = point_read_table_probes(&db, &bucket);
    assert!(
        table_probes_after <= table_probes_before,
        "lazy deep level must not increase point table probes ({table_probes_before} -> {table_probes_after})"
    );

    // Data is still correct after the lazy policy stabilizes.
    for chunk in 0..3 {
        let key = format!("key-{chunk:03}").into_bytes();
        assert_eq!(
            bucket.get_sync(&key).expect("deep-level read"),
            Some(format!("value-{chunk:03}").into_bytes())
        );
    }

    drop(bucket);
    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

fn point_read_table_probes(db: &Db, bucket: &trine_kv::Bucket) -> u64 {
    let before = db.stats();
    for chunk in 0..3 {
        let key = format!("key-{chunk:03}").into_bytes();
        let _ = bucket.get_sync(&key).expect("probe read");
    }
    let after = db.stats();
    after
        .read_path
        .point_table_probes
        .saturating_sub(before.read_path.point_table_probes)
}

#[test]
fn scan_waste_and_snapshot_lag_metrics_report_gc_health() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    // Version bloat: three versions of four keys.
    for round in 0..3_u32 {
        for k in 0..4_u32 {
            bucket
                .put_sync(
                    format!("k{k}").into_bytes(),
                    format!("v{round}-{k}").into_bytes(),
                )
                .expect("put version");
        }
    }
    // Two keys hidden by a point delete.
    bucket.delete_sync(b"k0".to_vec()).expect("delete k0");
    bucket.delete_sync(b"k1".to_vec()).expect("delete k1");

    // Drain a full range scan.
    let mut returned = 0_u64;
    let mut iter = bucket.range_sync(&KeyRange::all()).expect("range scan opens");
    while let Some(row) = iter.next_sync() {
        row.expect("scan row");
        returned += 1;
    }
    drop(iter);

    let stats = db.stats();
    assert_eq!(returned, 2, "only k2 and k3 remain live");
    assert_eq!(stats.scan_user_keys, returned);
    assert!(
        stats.scan_internal_records > stats.scan_user_keys,
        "version bloat: internal {} should exceed user {}",
        stats.scan_internal_records,
        stats.scan_user_keys
    );
    assert!(
        stats.scan_tombstone_hidden_keys >= 2,
        "k0 and k1 are delete-hidden: {}",
        stats.scan_tombstone_hidden_keys
    );

    // Snapshot version-debt: holding a snapshot pins old versions.
    assert_eq!(db.stats().oldest_snapshot_lag, 0);
    let snapshot = db.snapshot();
    for k in 0..4_u32 {
        bucket
            .put_sync(format!("k{k}").into_bytes(), b"newer".to_vec())
            .expect("put after snapshot");
    }
    let during = db.stats();
    assert_eq!(during.active_snapshots, 1);
    assert!(
        during.oldest_snapshot_lag > 0,
        "held snapshot should hold version debt: {}",
        during.oldest_snapshot_lag
    );
    drop(snapshot);
    assert_eq!(
        db.stats().oldest_snapshot_lag,
        0,
        "dropping the snapshot clears version debt"
    );
}

#[test]
fn scan_skips_fully_covered_table_on_read_path() {
    let path = temp_db_path("scan-skip-covered");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    // Table 1: 50 "a*" keys. Table 2: 50 "z*" keys. Separate flushes.
    for i in 0..50_u32 {
        bucket
            .put_sync(format!("a{i:02}").into_bytes(), b"v".to_vec())
            .expect("put a");
    }
    db.flush_sync().expect("flush table a");
    for i in 0..50_u32 {
        bucket
            .put_sync(format!("z{i:02}").into_bytes(), b"v".to_vec())
            .expect("put z");
    }
    db.flush_sync().expect("flush table z");

    // Delete the whole "a*" table with one range tombstone (fresh, not compacted,
    // so the "a*" table is still physically present but fully hidden on read).
    db.delete_range_sync(KeyRange::half_open(b"a".to_vec(), b"b".to_vec()))
        .expect("range delete a*");
    db.flush_sync().expect("flush tombstone");

    let before = db.stats();
    let mut iter = bucket.range_sync(&KeyRange::all()).expect("range scan");
    let mut returned = 0_u64;
    while let Some(row) = iter.next_sync() {
        row.expect("scan row");
        returned += 1;
    }
    drop(iter);
    let after = db.stats();

    // Correctness: only the live "z*" keys come back.
    assert_eq!(returned, 50, "only z* keys are live");

    let internal = after
        .scan_internal_records
        .saturating_sub(before.scan_internal_records);
    let user = after.scan_user_keys.saturating_sub(before.scan_user_keys);
    let hidden = after
        .scan_tombstone_hidden_keys
        .saturating_sub(before.scan_tombstone_hidden_keys);
    assert_eq!(user, 50);
    // The covered "a*" table was skipped without reading: its 50 records are
    // neither merged (internal stays ~= user) nor counted as delete-hidden.
    assert!(
        internal <= user + 5,
        "covered table should be skipped, not merged: internal {internal} user {user}"
    );
    assert!(
        hidden <= 5,
        "skipped table keys must not be read as delete-hidden: {hidden}"
    );

    drop(bucket);
    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn bulk_range_delete_drops_covered_tables_by_file() {
    let path = temp_db_path("drop-covered-by-file");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    for index in 0..200_u32 {
        bucket
            .put_sync(format!("k{index:04}").into_bytes(), vec![b'x'; 64])
            .expect("put");
    }
    db.flush_sync().expect("flush data");
    db.compact_range_sync(KeyRange::all()).expect("settle into a level");
    let before = db.stats();
    assert!(before.total_tables >= 1, "data tables exist before the drop");

    // Bulk-delete everything, then compact. No active snapshots, so the
    // bucket-wide range tombstone is retention-safe and the fully covered data
    // tables are dropped by file instead of rewritten.
    db.delete_range_sync(KeyRange::all()).expect("range delete");
    db.flush_sync().expect("flush range tombstone");
    db.compact_range_sync(KeyRange::all())
        .expect("compaction drops covered tables");

    for index in 0..200_u32 {
        assert_eq!(
            bucket
                .get_sync(format!("k{index:04}").as_bytes())
                .expect("get after drop"),
            None,
            "covered key must be gone"
        );
    }
    let after = db.stats();
    assert!(
        after.table_bytes <= before.table_bytes,
        "covered data must not be rewritten: before {} after {}",
        before.table_bytes,
        after.table_bytes
    );

    // Reopen proves the drop is durable.
    drop(bucket);
    drop(db);
    let db = Db::open_sync(DbOptions::persistent(&path)).expect("reopen");
    let bucket = db.default_bucket_sync().expect("bucket reopens");
    assert_eq!(
        bucket.get_sync(b"k0000").expect("get after reopen"),
        None
    );
    drop(bucket);
    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_level_filter_stats_aggregate_by_level() {
    let path = temp_db_path("level-filter-stats");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 64;
    // Deterministic level layout: no background maintenance moves tables.
    options.background_worker_count = 0;

    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    let write_chunk = |chunk: usize| {
        for index in 0..64 {
            let row = chunk * 64 + index;
            let key = format!("key-{row:05}").into_bytes();
            bucket.put_sync(key, b"v").expect("write key");
        }
    };

    // First two chunks flush to L0 then compact down to L1.
    for chunk in 0..2 {
        write_chunk(chunk);
        db.flush_sync().expect("flush L0 table");
    }
    db.compact_range_sync(KeyRange::all())
        .expect("compact L0 into L1");
    // Next two chunks flush to L0 and stay there (no compaction).
    for chunk in 2..4 {
        write_chunk(chunk);
        db.flush_sync().expect("flush L0 table");
    }
    assert!(
        db.stats().l0_tables >= 1,
        "workload must leave tables on L0"
    );

    // In-range missing-key lookups exercise the table point filter (the keys sit
    // inside existing tables' key bounds but were never written).
    for row in 0..256 {
        let key = format!("key-{row:05}-absent").into_bytes();
        assert_eq!(bucket.get_sync(&key).expect("missing read"), None);
    }

    let stats = db.stats();
    assert!(
        !stats.level_filters.is_empty(),
        "per-level filter stats must be reported"
    );
    assert!(
        stats.level_filters.len() >= 2,
        "more than one level should hold tables: {:?}",
        stats.level_filters
    );

    // Per-level rollup must reconcile with the global totals and table counts.
    let summed_hits: u64 = stats
        .level_filters
        .iter()
        .map(|row| row.filters.table_point_hits)
        .sum();
    let summed_fp: u64 = stats
        .level_filters
        .iter()
        .map(|row| row.filters.table_point_false_positives)
        .sum();
    let summed_misses: u64 = stats
        .level_filters
        .iter()
        .map(|row| row.filters.table_point_misses)
        .sum();
    assert_eq!(summed_hits, stats.filters.table_point_hits);
    assert_eq!(summed_fp, stats.filters.table_point_false_positives);
    assert_eq!(summed_misses, stats.filters.table_point_misses);

    let level_filter_tables: usize = stats.level_filters.iter().map(|row| row.tables).sum();
    let level_tables: usize = stats.level_tables.iter().map(|row| row.tables).sum();
    assert_eq!(level_filter_tables, level_tables);

    // The missing-key sweep must have exercised the point filter somewhere.
    let absent_probes = stats
        .filters
        .table_point_false_positives
        .saturating_add(stats.filters.table_point_misses);
    assert!(
        absent_probes > 0,
        "missing-key lookups should reach the table point filter"
    );

    drop(bucket);
    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_stats_report_tables_blobs_and_compactions() {
    let path = temp_db_path("live-stats");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 1;
    let bucket_options = BucketOptions {
        blob_threshold_bytes: 4,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        assert_eq!(db.stats().live_buckets, 1);

        let large_a = b"large-a".to_vec();
        bucket
            .put_sync(b"a", large_a.clone())
            .expect("write large a");
        assert!(
            db.stats().memtable_bytes > 0,
            "unflushed writes should contribute to memtable stats"
        );
        db.flush_sync().expect("first flush stays L0");
        let stats = db.stats();
        assert_eq!(stats.total_tables, 1);
        assert_eq!(stats.l0_tables, 1);
        assert_eq!(level_table_count(&stats, 0), 1);
        assert!(stats.table_bytes > 0);
        assert_eq!(level_table_bytes(&stats, 0), stats.table_bytes);
        assert_eq!(stats.live_blob_files, 1);
        assert_eq!(stats.live_blob_bytes, large_a.len() as u64);

        let large_b = b"large-b".to_vec();
        bucket
            .put_sync(b"b", large_b.clone())
            .expect("write large b");
        db.flush_sync().expect("second flush triggers compaction");
        let stats = db.stats();
        assert_eq!(stats.total_tables, 1);
        assert_eq!(stats.l0_tables, 0);
        assert_eq!(level_table_count(&stats, 0), 0);
        assert_eq!(level_table_count(&stats, 1), 1);
        assert!(level_table_bytes(&stats, 1) > 0);
        assert_eq!(stats.live_blob_files, 1);
        assert_eq!(
            stats.live_blob_bytes,
            (large_a.len() + large_b.len()) as u64
        );
        assert_eq!(stats.compaction_runs, 1);
        assert_eq!(stats.compaction_input_tables, 2);
        assert_eq!(stats.compaction_output_tables, 1);
        assert!(stats.compaction_input_bytes > 0);
        assert!(stats.compaction_output_bytes > 0);
        assert_eq!(compaction_trigger_runs(&stats, CompactionTrigger::L0Overlap), 1);

        let obsolete_blob_path = blob::blob_path(&path, 999);
        write_file(&obsolete_blob_path, b"obsolete");
        let stats = db.stats();
        assert_eq!(stats.obsolete_blob_files, 1);
        assert_eq!(stats.obsolete_blob_bytes, b"obsolete".len() as u64);
        assert_eq!(stats.stale_blob_files, 1);
        assert_eq!(stats.stale_blob_bytes, b"obsolete".len() as u64);
        fs::remove_file(obsolete_blob_path).expect("remove test obsolete blob");
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_block_cache_records_hits_and_misses() {
    let path = temp_db_path("block-cache-stats");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..64 {
            bucket
                .put_sync(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush_sync().expect("flush table");

        let stats = db.stats();
        assert_eq!(stats.block_cache_hits, 0);
        assert_eq!(stats.block_cache_misses, 0);

        assert_eq!(
            bucket.get_sync(b"key-032").expect("first cached read"),
            Some(b"value-032".to_vec())
        );
        let stats = db.stats();
        assert_eq!(stats.block_cache_hits, 0);
        assert!(
            stats.block_cache_misses > 0,
            "first table block read should miss cache"
        );
        let misses = stats.block_cache_misses;

        assert_eq!(
            bucket.get_sync(b"key-032").expect("second cached read"),
            Some(b"value-032".to_vec())
        );
        let stats = db.stats();
        assert!(stats.block_cache_hits > 0);
        assert_eq!(stats.block_cache_misses, misses);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_range_iterator_defers_table_block_reads_until_next() {
    let path = temp_db_path("range-iterator-lazy-block-read");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..64 {
            bucket
                .put_sync(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush_sync().expect("flush table");

        let mut iter = bucket
            .range_sync(&KeyRange::all())
            .expect("range cursor is created");
        let stats = db.stats();
        assert_eq!(stats.block_cache_hits, 0);
        assert_eq!(
            stats.block_cache_misses, 0,
            "constructing a range cursor should not touch table blocks"
        );

        let first = iter
            .next_sync()
            .expect("first row exists")
            .expect("first row reads");
        assert_eq!(first.key, b"key-000".to_vec());
        assert_eq!(first.value, b"value-000".to_vec());

        let stats = db.stats();
        assert!(
            stats.block_cache_misses > 0,
            "first iterator advance should touch the table block"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_range_iterator_keeps_active_memtable_after_flush() {
    let path = temp_db_path("range-iterator-memtable-handle");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"key-010", b"before-a").expect("write row");
        bucket.put_sync(b"key-020", b"before-b").expect("write row");

        let iter = bucket
            .range_sync(&KeyRange::all())
            .expect("range cursor is created");
        db.flush_sync().expect("flush active memtable");
        bucket
            .put_sync(b"key-000", b"after")
            .expect("write later row");

        assert_eq!(
            collect_rows(iter),
            vec![
                (b"key-010".to_vec(), b"before-a".to_vec()),
                (b"key-020".to_vec(), b"before-b".to_vec()),
            ]
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_transaction_read_range_consumes_scan_before_tracking() {
    let path = temp_db_path("transaction-read-range-consumes-scan");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..64 {
            bucket
                .put_sync(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush_sync().expect("flush table");
        assert_eq!(db.stats().block_cache_misses, 0);

        let mut txn = db.transaction(TransactionOptions::default());
        txn.read_range_sync(KeyRange::all())
            .expect("transaction range read succeeds");

        assert!(
            db.stats().block_cache_misses > 0,
            "transaction range read should advance the table cursor"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_transaction_range_bucket_returns_data_and_tracks_conflict() {
    let path = temp_db_path("transaction-range-bucket-returns-data");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"k1", b"v1").expect("write k1");
        bucket.put_sync(b"k2", b"v2").expect("write k2");

        // Unlike `read_range`, a transaction range read returns the data ...
        let mut txn = db.transaction(TransactionOptions::default());
        let rows: Vec<(Vec<u8>, Vec<u8>)> = txn
            .range_sync(KeyRange::all())
            .expect("range read returns a cursor")
            .map(|item| {
                let kv = item.expect("row decodes");
                (kv.key, kv.value)
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
            ]
        );

        // ... and the range is recorded in the read set, so a later committed
        // write inside it makes this transaction's commit conflict.
        bucket
            .put_sync(b"k3", b"v3")
            .expect("write inside the read range");
        txn.put(b"k4", b"v4");
        let error = txn
            .commit_sync()
            .expect_err("a committed write inside the read range conflicts");
        assert!(matches!(error, Error::Conflict { .. }));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_preserves_snapshot_versions() {
    let path = temp_db_path("flush-snapshot");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"v1").expect("write v1");
        let snapshot = db.snapshot();
        bucket.put_sync(b"a", b"v2").expect("write v2");

        db.flush_sync().expect("flush table");

        assert_eq!(
            snapshot
                .get_sync(&bucket, b"a")
                .expect("snapshot reads table"),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"a").expect("current reads table"),
            Some(b"v2".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_table_block_index_reads_points_and_ranges() {
    let path = temp_db_path("table-block-index");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for index in 0..160 {
            bucket
                .put_sync(
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
                .expect("write indexed row");
        }
        db.flush_sync().expect("flush indexed table");

        assert_eq!(
            bucket
                .get_sync(b"key-042")
                .expect("point reads indexed table"),
            Some(b"value-042".to_vec())
        );
        let rows = bucket
            .range_sync(&KeyRange::half_open(b"key-020", b"key-030"))
            .expect("range reads indexed table")
            .map(|item| {
                let item = item.expect("range item reads");
                (item.key, item.value)
            })
            .collect::<Vec<_>>();
        let expected = (20..30)
            .map(|index| {
                (
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, expected);

        let prefix_rows = collect_rows(bucket.prefix_sync(b"key-12").expect("prefix reads table"));
        let expected_prefix = (120..130)
            .map(|index| {
                (
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(prefix_rows, expected_prefix);
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after block-index flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens from indexed table");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(
            bucket
                .get_sync(b"key-127")
                .expect("point reads after reopen"),
            Some(b"value-127".to_vec())
        );
        let rows = bucket
            .range_sync(&KeyRange::half_open(b"key-150", b"key-160"))
            .expect("range reads after reopen")
            .map(|item| {
                let item = item.expect("range item reads after reopen");
                (item.key, item.value)
            })
            .collect::<Vec<_>>();
        let expected = (150..160)
            .map(|index| {
                (
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, expected);

        let prefix_rows = collect_rows(
            bucket
                .prefix_sync(b"key-12")
                .expect("prefix reads after reopen"),
        );
        let expected_prefix = (120..130)
            .map(|index| {
                (
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(prefix_rows, expected_prefix);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_index_search_policies_preserve_table_reads() {
    let path = temp_db_path("table-search-policies");
    let options = DbOptions::persistent(&path);
    let policies = [
        ("linear", IndexSearchPolicy::Linear),
        ("binary", IndexSearchPolicy::Binary),
        ("auto", IndexSearchPolicy::Auto),
    ];

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        for (name, policy) in policies {
            let bucket_options = BucketOptions {
                index_search_policy: policy,
                prefix_extractor: PrefixExtractor::FixedLen(6),
                ..BucketOptions::default()
            };
            let bucket = db
                .bucket_with_options_sync(name, bucket_options)
                .expect("policy bucket opens");
            for index in 0..80 {
                bucket
                    .put_sync(
                        format!("key-{index:03}").into_bytes(),
                        format!("value-{index:03}").into_bytes(),
                    )
                    .expect("write policy row");
            }
        }
        db.flush_sync().expect("flush policy tables");

        for (name, policy) in policies {
            let bucket_options = BucketOptions {
                index_search_policy: policy,
                prefix_extractor: PrefixExtractor::FixedLen(6),
                ..BucketOptions::default()
            };
            let bucket = db
                .bucket_with_options_sync(name, bucket_options)
                .expect("policy bucket reuses options");
            assert_eq!(
                bucket.get_sync(b"key-042").expect("policy point reads"),
                Some(b"value-042".to_vec())
            );
            assert_eq!(
                collect_rows(
                    bucket
                        .range_sync(&KeyRange::half_open(b"key-020", b"key-023"))
                        .expect("policy range reads")
                ),
                vec![
                    (b"key-020".to_vec(), b"value-020".to_vec()),
                    (b"key-021".to_vec(), b"value-021".to_vec()),
                    (b"key-022".to_vec(), b"value-022".to_vec()),
                ],
                "policy {policy:?} range changed"
            );
            assert_eq!(
                collect_rows(bucket.prefix_sync(b"key-04").expect("policy prefix reads")),
                (40..50)
                    .map(|index| {
                        (
                            format!("key-{index:03}").into_bytes(),
                            format!("value-{index:03}").into_bytes(),
                        )
                    })
                    .collect::<Vec<_>>(),
                "policy {policy:?} prefix changed"
            );
        }
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after search policy flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        for (name, policy) in policies {
            let bucket_options = BucketOptions {
                index_search_policy: policy,
                prefix_extractor: PrefixExtractor::FixedLen(6),
                ..BucketOptions::default()
            };
            let bucket = db
                .bucket_with_options_sync(name, bucket_options)
                .expect("policy bucket reopens");
            assert_eq!(
                bucket.get_sync(b"key-042").expect("policy point reopens"),
                Some(b"value-042".to_vec())
            );
        }
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_table_compression_profiles_round_trip() {
    let path = temp_db_path("table-compression");
    let options = DbOptions::persistent(&path);
    let fast_options = BucketOptions::default();
    let plain_options = BucketOptions {
        compression: CompressionProfile::None,
        ..BucketOptions::default()
    };

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let fast = db
            .bucket_with_options_sync("fast", fast_options.clone())
            .expect("fast bucket opens");
        let plain = db
            .bucket_with_options_sync("plain", plain_options.clone())
            .expect("plain bucket opens");

        for index in 0..64 {
            let value = format!("value-{index:03}-aaaaaaaaaaaaaaaaaaaaaaaa").into_bytes();
            fast.put_sync(format!("key-{index:03}").into_bytes(), value.clone())
                .expect("write fast row");
            plain
                .put_sync(format!("key-{index:03}").into_bytes(), value)
                .expect("write plain row");
        }
        db.flush_sync().expect("flush compressed tables");

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        assert_eq!(
            manifest_state
                .tables()
                .get("fast")
                .and_then(|tables| tables.first())
                .expect("fast table metadata")
                .codec,
            CodecId::FastLz4Block
        );
        assert_eq!(
            manifest_state
                .tables()
                .get("plain")
                .and_then(|tables| tables.first())
                .expect("plain table metadata")
                .codec,
            CodecId::None
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after compressed flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens from compressed tables");
        let fast = db
            .bucket_with_options_sync("fast", fast_options)
            .expect("fast bucket reopens");
        let plain = db
            .bucket_with_options_sync("plain", plain_options)
            .expect("plain bucket reopens");

        assert_eq!(
            fast.get_sync(b"key-042")
                .expect("fast row reads after reopen"),
            Some(b"value-042-aaaaaaaaaaaaaaaaaaaaaaaa".to_vec())
        );
        assert_eq!(
            plain
                .get_sync(b"key-042")
                .expect("plain row reads after reopen"),
            Some(b"value-042-aaaaaaaaaaaaaaaaaaaaaaaa".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_prefix_filter_keeps_range_tombstones_authoritative() {
    let path = temp_db_path("prefix-filter-tombstones");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        prefix_extractor: PrefixExtractor::Separator(b':'),
        prefix_filter_policy: PrefixFilterPolicy::Bloom {
            bits_per_prefix: 32,
        },
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"user:1", b"old").expect("write old user");
        bucket
            .put_sync(b"user:2", b"live")
            .expect("write live user");
        db.flush_sync().expect("flush user table");

        bucket.put_sync(b"post:1", b"post").expect("write post");
        bucket
            .delete_range_sync(KeyRange::half_open(b"user:1", b"user:2"))
            .expect("range delete one user");
        db.flush_sync()
            .expect("flush post table with user tombstone");

        assert_eq!(
            collect_rows(bucket.prefix_sync(b"user:").expect("prefix reads users")),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
        assert_eq!(
            collect_rows(bucket.prefix_sync(b"us").expect("short prefix falls back")),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after prefix-filter flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(
            collect_rows(
                bucket
                    .prefix_sync(b"user:")
                    .expect("prefix reads after reopen")
            ),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
        assert_eq!(
            collect_rows(
                bucket
                    .prefix_sync(b"us")
                    .expect("short prefix after reopen")
            ),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_point_filter_keeps_range_tombstones_authoritative() {
    let path = temp_db_path("point-filter-tombstones");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"user:1", b"old").expect("write old user");
        db.flush_sync().expect("flush user table");

        bucket.put_sync(b"post:1", b"post").expect("write post");
        bucket
            .delete_range_sync(KeyRange::half_open(b"user:1", b"user:2"))
            .expect("range delete user");
        db.flush_sync()
            .expect("flush post table with user tombstone");

        assert_eq!(bucket.get_sync(b"user:1").expect("user is hidden"), None);
        assert_eq!(
            bucket.get_sync(b"post:1").expect("post survives"),
            Some(b"post".to_vec())
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after point-filter flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(
            bucket.get_sync(b"user:1").expect("user remains hidden"),
            None
        );
        assert_eq!(
            bucket.get_sync(b"post:1").expect("post survives reopen"),
            Some(b"post".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_blob_values_survive_flush_reopen_and_compaction() {
    let path = temp_db_path("blob-values");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;
    let large_a = b"large-value-a-large-value-a".to_vec();
    let large_c = b"large-value-c-large-value-c".to_vec();

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket
            .put_sync(b"a", large_a.clone())
            .expect("write blob a");
        bucket.put_sync(b"b", b"small").expect("write inline b");
        db.flush_sync().expect("flush first blob table");

        bucket
            .put_sync(b"c", large_c.clone())
            .expect("write blob c");
        db.flush_sync().expect("flush second blob table");
        db.compact_range_sync(KeyRange::all())
            .expect("compact blob tables");

        assert_eq!(
            bucket.get_sync(b"a").expect("blob a reads"),
            Some(large_a.clone())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("inline b reads"),
            Some(b"small".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"c").expect("blob c reads"),
            Some(large_c.clone())
        );
        assert_eq!(
            blob_file_paths(&path).len(),
            1,
            "auto Level Merge should keep retained compacted blob values together"
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after blob compaction");

    {
        let db = Db::open_sync(options).expect("persistent db reopens with blob refs");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(
            bucket.get_sync(b"a").expect("blob a reopens"),
            Some(large_a)
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("inline b reopens"),
            Some(b"small".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"c").expect("blob c reopens"),
            Some(large_c)
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_blob_level_merge_auto_rewrites_retained_blob_indexes() {
    let path = temp_db_path("blob-level-merge");
    let mut options = DbOptions::persistent(&path);
    options.blob_gc_enabled = false;
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    let old_b = b"large-value-b-old-large-value-b-old".to_vec();
    let new_a = b"large-value-a-new-large-value-a-new".to_vec();

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket
            .put_sync(b"a", b"large-value-a-old-large-value-a-old".to_vec())
            .expect("write old a");
        bucket.put_sync(b"b", old_b.clone()).expect("write old b");
        db.flush_sync().expect("flush shared old blob file");
        bucket.put_sync(b"a", new_a.clone()).expect("write new a");
        db.flush_sync().expect("flush new a blob file");
        assert_eq!(blob_file_paths(&path).len(), 2);

        db.compact_range_sync(KeyRange::all())
            .expect("level merge compaction rewrites retained blob refs");

        assert_eq!(
            bucket.get_sync(b"a").expect("new a reads"),
            Some(new_a.clone())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("old b reads"),
            Some(old_b.clone())
        );
        assert_eq!(
            blob_file_paths(&path).len(),
            1,
            "Level Merge should rewrite retained large values into the output blob file"
        );
        let stats = db.stats();
        assert_eq!(stats.live_blob_files, 1);
        assert_eq!(
            stats.live_blob_bytes,
            new_a.len().saturating_add(old_b.len()) as u64
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after level merge");

    {
        let db = Db::open_sync(options).expect("persistent db reopens after level merge");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(bucket.get_sync(b"a").expect("new a reopens"), Some(new_a));
        assert_eq!(bucket.get_sync(b"b").expect("old b reopens"), Some(old_b));
        assert_eq!(blob_file_paths(&path).len(), 1);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_blob_level_merge_defers_pending_blob_clear_publish() {
    let path = temp_db_path("blob-level-merge-deferred-clear");
    let mut options = DbOptions::persistent(&path);
    options.blob_gc_enabled = false;
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        blob_level_merge_policy: BlobLevelMergePolicy::Always,
        filter_depth_curve: FilterDepthCurve::Auto,
        ..BucketOptions::default()
    };

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket
            .put_sync(b"a", b"large-value-a-old-large-value-a-old".to_vec())
            .expect("write old a");
        bucket
            .put_sync(b"b", b"large-value-b-old-large-value-b-old".to_vec())
            .expect("write old b");
        db.flush_sync().expect("flush shared old blob file");
        bucket
            .put_sync(b"a", b"large-value-a-new-large-value-a-new".to_vec())
            .expect("write new a");
        db.flush_sync().expect("flush new a blob file");

        let before = db.stats();
        db.compact_range_sync(KeyRange::all())
            .expect("level merge compaction succeeds");
        let after = db.stats();
        assert_eq!(
            after
                .storage_operations
                .publish_manifest
                .requests
                .saturating_sub(before.storage_operations.publish_manifest.requests),
            1,
            "foreground level merge should not publish a second manifest only to clear cleanup metadata"
        );
        assert_eq!(
            blob_file_paths(&path).len(),
            1,
            "obsolete blob files are still deleted during foreground compaction"
        );

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        assert!(
            !manifest_state.pending_blob_deletions().is_empty(),
            "deleted blob ids stay pending until the next cleanup boundary"
        );

        db.flush_sync()
            .expect("later flush clears pending blob deletion metadata");
        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest rereads");
        assert!(manifest_state.pending_blob_deletions().is_empty());
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_blob_level_merge_can_be_disabled() {
    let path = temp_db_path("blob-level-merge-disabled");
    let mut options = DbOptions::persistent(&path);
    options.blob_gc_enabled = false;
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        blob_level_merge_policy: BlobLevelMergePolicy::Disabled,
        filter_depth_curve: FilterDepthCurve::Auto,
        ..BucketOptions::default()
    };

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket
            .put_sync(b"a", b"large-value-a-old-large-value-a-old".to_vec())
            .expect("write old a");
        bucket
            .put_sync(b"b", b"large-value-b-old-large-value-b-old".to_vec())
            .expect("write old b");
        db.flush_sync().expect("flush shared old blob file");
        bucket
            .put_sync(b"a", b"large-value-a-new-large-value-a-new".to_vec())
            .expect("write new a");
        db.flush_sync().expect("flush new a blob file");

        db.compact_range_sync(KeyRange::all())
            .expect("disabled level merge compaction succeeds");

        assert_eq!(
            blob_file_paths(&path).len(),
            2,
            "disabled Level Merge should keep retained blob indexes pointing at old files"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_value_lazy_iterator_defers_blob_reads_until_value_access() {
    let path = temp_db_path("value-lazy-iterator");
    let mut options = DbOptions::persistent(&path);
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    let large_value = b"large-value-a-large-value-a".to_vec();

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket
            .put_sync(b"a", large_value.clone())
            .expect("write large value");
        db.flush_sync().expect("flush blob-backed table");

        let mut iter = bucket
            .range_lazy_sync(&KeyRange::all())
            .expect("value-lazy range opens");
        assert_eq!(db.stats().blob_read_count, 0);

        let row = iter
            .next_sync()
            .expect("first row exists")
            .expect("first lazy row reads");
        assert_eq!(row.key, b"a".to_vec());
        assert!(!row.value.is_inline());
        assert_eq!(
            db.stats().blob_read_count,
            0,
            "reading only the key should not load blob bytes"
        );

        assert_eq!(
            row.value.read_sync().expect("lazy value reads"),
            large_value
        );
        let stats = db.stats();
        assert_eq!(stats.blob_read_count, 1);
        assert_eq!(
            stats.blob_read_bytes,
            b"large-value-a-large-value-a".len() as u64
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_reopen_fails_when_referenced_blob_file_is_missing() {
    let path = temp_db_path("missing-blob");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket
            .put_sync(b"a", b"large-value-a-large-value-a".to_vec())
            .expect("write blob a");
        db.flush_sync().expect("flush blob table");
    }

    let blob_path = blob_file_paths(&path)
        .pop()
        .expect("blob file exists after flush");
    fs::remove_file(blob_path).expect("remove blob file");

    let error = Db::open_sync(options).expect_err("referenced blob file is required during open");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(
        error
            .to_string()
            .contains("referenced blob files are missing")
    );

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_removes_blob_files_for_dropped_versions() {
    let path = temp_db_path("compact-dropped-blob-versions");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;
    let old_value = b"large-value-a-old-large-value-a-old".to_vec();
    let new_value = b"large-value-a-new-large-value-a-new".to_vec();

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket
            .put_sync(b"a", old_value)
            .expect("write old blob value");
        db.flush_sync().expect("flush old blob table");
        bucket
            .put_sync(b"a", new_value.clone())
            .expect("write new blob value");
        db.flush_sync().expect("flush new blob table");
        assert_eq!(blob_file_paths(&path).len(), 2);

        db.compact_range_sync(KeyRange::all())
            .expect("manual compaction removes dropped blob");

        assert_eq!(
            bucket.get_sync(b"a").expect("current blob reads"),
            Some(new_value.clone())
        );
        assert_eq!(
            blob_file_paths(&path).len(),
            1,
            "only the live blob file should remain"
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after blob cleanup");

    {
        let db = Db::open_sync(options).expect("persistent db reopens after blob cleanup");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket.get_sync(b"a").expect("blob reopens"),
            Some(new_value)
        );
        assert_eq!(blob_file_paths(&path).len(), 1);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_publish_failure_removes_unpublished_table_and_blob_files() {
    let path = temp_db_path("compact-publish-cleanup");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;
    let old_value = b"large-value-a-old-large-value-a-old".to_vec();
    let new_value = b"large-value-a-new-large-value-a-new".to_vec();

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket
            .put_sync(b"a", old_value)
            .expect("write old blob value");
        db.flush_sync().expect("flush old blob table");
        bucket
            .put_sync(b"a", new_value.clone())
            .expect("write new blob value");
        db.flush_sync().expect("flush new blob table");

        let mut before_tables = table_file_paths(&path);
        before_tables.sort();
        let before_blobs = blob_file_paths(&path);
        assert_eq!(before_tables.len(), 2);
        assert_eq!(before_blobs.len(), 2);

        let manifest_tmp_dir = manifest::manifest_path(&path).with_extension("tmp");
        fs::create_dir(&manifest_tmp_dir).expect("block manifest tmp path");

        let error = db
            .compact_range_sync(KeyRange::all())
            .expect_err("manifest publish should fail");
        assert!(matches!(error, Error::Io(_)));

        let mut after_tables = table_file_paths(&path);
        after_tables.sort();
        assert_eq!(
            after_tables, before_tables,
            "failed compaction should keep only pre-existing table files"
        );
        assert_eq!(
            blob_file_paths(&path),
            before_blobs,
            "failed compaction should remove unpublished blob files"
        );
        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("old tables survive failed compaction"),
            Some(new_value)
        );

        fs::remove_dir(&manifest_tmp_dir).expect("remove manifest tmp blocker");
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_removes_blob_files_after_delete_cleanup() {
    let path = temp_db_path("compact-deleted-blob");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket
            .put_sync(b"a", b"large-value-a-large-value-a".to_vec())
            .expect("write blob value");
        db.flush_sync().expect("flush blob table");
        bucket.delete_sync(b"a").expect("delete blob key");
        db.flush_sync().expect("flush delete table");
        assert_eq!(blob_file_paths(&path).len(), 1);

        db.compact_range_sync(KeyRange::all())
            .expect("manual compaction removes deleted blob");

        assert_eq!(
            bucket.get_sync(b"a").expect("deleted key reads missing"),
            None
        );
        assert!(
            blob_file_paths(&path).is_empty(),
            "deleted blob file should be removed"
        );
        assert!(
            table_file_paths(&path).is_empty(),
            "empty compaction output should leave no table files"
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after deleted blob cleanup");

    {
        let db = Db::open_sync(options).expect("persistent db reopens after deleted blob cleanup");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(bucket.get_sync(b"a").expect("deleted key reopens"), None);
        assert!(blob_file_paths(&path).is_empty());
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_blob_gc_rewrites_live_records_from_partially_stale_file() {
    let path = temp_db_path("blob-gc-partial-stale");
    let mut options = DbOptions::persistent(&path);
    options.blob_gc_min_file_bytes = 1;
    options.blob_gc_discardable_ratio = BlobGcRatio::from_millionths(400_000);
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        blob_level_merge_policy: BlobLevelMergePolicy::Disabled,
        filter_depth_curve: FilterDepthCurve::Auto,
        ..BucketOptions::default()
    };
    let old_a = b"large-value-a-old-large-value-a-old".to_vec();
    let old_b = b"large-value-b-old-large-value-b-old".to_vec();
    let new_a = b"large-value-a-new-large-value-a-new".to_vec();

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", old_a).expect("write old a");
        bucket.put_sync(b"b", old_b.clone()).expect("write old b");
        db.flush_sync().expect("flush shared old blob file");
        let old_blob_path = blob_file_paths(&path)
            .into_iter()
            .next()
            .expect("old blob file exists");

        bucket.put_sync(b"a", new_a.clone()).expect("write new a");
        db.flush_sync().expect("flush new a blob file");
        db.compact_range_sync(KeyRange::all())
            .expect("compaction runs blob GC");

        assert_eq!(bucket.get_sync(b"a").expect("new a reads"), Some(new_a));
        assert_eq!(bucket.get_sync(b"b").expect("old b reads"), Some(old_b));
        assert!(
            !old_blob_path.exists(),
            "partially stale old blob file should be removed after GC"
        );
        assert_eq!(
            blob_file_paths(&path).len(),
            2,
            "new a and rewritten b should be the only blob files"
        );
        let stats = db.stats();
        assert_eq!(stats.blob_gc_runs, 1);
        assert!(stats.blob_gc_input_bytes > 0);
        assert!(stats.blob_gc_output_bytes > 0);
        assert!(stats.blob_gc_discarded_bytes > 0);
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after blob GC");

    {
        let db = Db::open_sync(options).expect("persistent db reopens after blob GC");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket.get_sync(b"b").expect("rewritten b reopens"),
            Some(b"large-value-b-old-large-value-b-old".to_vec())
        );
        assert_eq!(blob_file_paths(&path).len(), 2);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_blob_gc_batches_multiple_stale_candidates() {
    let path = temp_db_path("blob-gc-multi-candidate");
    let mut options = DbOptions::persistent(&path);
    options.blob_gc_min_file_bytes = 1;
    options.blob_gc_discardable_ratio = BlobGcRatio::from_millionths(400_000);
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        blob_level_merge_policy: BlobLevelMergePolicy::Disabled,
        filter_depth_curve: FilterDepthCurve::Auto,
        ..BucketOptions::default()
    };
    let new_a = b"large-value-a-new-large-value-a-new".to_vec();
    let old_b = b"large-value-b-old-large-value-b-old".to_vec();
    let new_c = b"large-value-c-new-large-value-c-new".to_vec();
    let old_d = b"large-value-d-old-large-value-d-old".to_vec();

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket
            .put_sync(b"a", b"large-value-a-old-large-value-a-old".to_vec())
            .expect("write old a");
        bucket.put_sync(b"b", old_b.clone()).expect("write old b");
        db.flush_sync().expect("flush first candidate blob");
        bucket
            .put_sync(b"c", b"large-value-c-old-large-value-c-old".to_vec())
            .expect("write old c");
        bucket.put_sync(b"d", old_d.clone()).expect("write old d");
        db.flush_sync().expect("flush second candidate blob");
        bucket.put_sync(b"a", new_a.clone()).expect("write new a");
        bucket.put_sync(b"c", new_c.clone()).expect("write new c");
        db.flush_sync().expect("flush replacement blob");
        assert_eq!(blob_file_paths(&path).len(), 3);

        db.compact_range_sync(KeyRange::all())
            .expect("compaction runs batched blob GC");

        assert_eq!(bucket.get_sync(b"a").expect("new a reads"), Some(new_a));
        assert_eq!(bucket.get_sync(b"b").expect("old b reads"), Some(old_b));
        assert_eq!(bucket.get_sync(b"c").expect("new c reads"), Some(new_c));
        assert_eq!(bucket.get_sync(b"d").expect("old d reads"), Some(old_d));
        assert_eq!(
            blob_file_paths(&path).len(),
            2,
            "two stale candidate files should be replaced by one GC output blob"
        );
        let stats = db.stats();
        assert_eq!(stats.blob_gc_runs, 1);
        assert!(stats.blob_gc_input_bytes > stats.blob_gc_output_bytes);
        assert!(stats.blob_gc_discarded_bytes > 0);
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after batched blob GC");

    {
        let db = Db::open_sync(options).expect("persistent db reopens after batched blob GC");
        assert_eq!(blob_file_paths(&path).len(), 2);
        assert_eq!(
            db.get_sync(b"d").expect("rewritten d reopens"),
            Some(b"large-value-d-old-large-value-d-old".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_blob_gc_keeps_old_blob_while_read_pin_can_reach_it() {
    let path = temp_db_path("blob-gc-read-pin");
    let mut options = DbOptions::persistent(&path);
    options.blob_gc_min_file_bytes = 1;
    options.blob_gc_discardable_ratio = BlobGcRatio::from_millionths(400_000);
    options.default_bucket_options = BucketOptions {
        blob_threshold_bytes: 8,
        blob_level_merge_policy: BlobLevelMergePolicy::Disabled,
        filter_depth_curve: FilterDepthCurve::Auto,
        ..BucketOptions::default()
    };
    let old_a = b"large-value-a-old-large-value-a-old".to_vec();
    let old_b = b"large-value-b-old-large-value-b-old".to_vec();
    let new_a = b"large-value-a-new-large-value-a-new".to_vec();

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", old_a).expect("write old a");
        bucket.put_sync(b"b", old_b.clone()).expect("write old b");
        db.flush_sync().expect("flush shared old blob file");
        let old_blob_path = blob_file_paths(&path)
            .into_iter()
            .next()
            .expect("old blob file exists");

        bucket.put_sync(b"a", new_a).expect("write new a");
        db.flush_sync().expect("flush new a blob file");
        let mut iter = bucket
            .range_sync(&KeyRange::all())
            .expect("range iterator pins pre-GC table handles");

        db.compact_range_sync(KeyRange::all())
            .expect("compaction runs blob GC with read pin");
        assert!(
            old_blob_path.exists(),
            "old blob file stays while a read pin can reach old table handles"
        );
        let rows = iter
            .by_ref()
            .map(|item| {
                let item = item.expect("iterator item reads");
                (item.key, item.value)
            })
            .collect::<Vec<_>>();
        assert!(rows.contains(&(b"b".to_vec(), old_b.clone())));
        assert_eq!(bucket.get_sync(b"b").expect("current reads b"), Some(old_b));

        drop(iter);
        db.flush_sync()
            .expect("cleanup pending old blob after read pin");
        assert!(
            !old_blob_path.exists(),
            "old blob file is removed after read pin release"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_keeps_lazy_iterator_table_files_until_pin_released() {
    let path = temp_db_path("compaction-lazy-iterator-file-lifetime");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..64 {
            bucket
                .put_sync(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush_sync().expect("flush base table");

        let mut iter = bucket
            .range_sync(&KeyRange::all())
            .expect("range cursor is created");
        assert_eq!(
            db.stats().block_cache_misses,
            0,
            "constructing a range cursor should not touch table blocks"
        );

        let before_manifest =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        let before_table_paths = before_manifest
            .tables()
            .get("default")
            .expect("default table list")
            .iter()
            .map(|properties| table::table_path(&path, properties.id))
            .collect::<Vec<_>>();

        bucket
            .put_sync(b"key-032", b"value-032-new")
            .expect("write overlapping update");
        db.flush_sync().expect("flush overlapping table");
        db.compact_range_sync(KeyRange::all())
            .expect("manual compaction succeeds");

        for old_path in &before_table_paths {
            assert!(
                old_path.exists(),
                "old table file stays available for a lazy iterator at {}",
                old_path.display()
            );
        }

        let first = iter
            .next_sync()
            .expect("first row exists")
            .expect("first row reads after compaction");
        assert_eq!(first.key, b"key-000".to_vec());
        assert_eq!(first.value, b"value-000".to_vec());

        drop(iter);
        db.flush_sync().expect("cleanup pending obsolete tables");
        for old_path in before_table_paths {
            assert!(
                !old_path.exists(),
                "old table file is removed after read pin release at {}",
                old_path.display()
            );
        }
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_rewrites_tables_and_preserves_reads() {
    let path = temp_db_path("compact-default");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"v1").expect("write a v1");
        db.flush_sync().expect("flush first table");
        let snapshot = db.snapshot();

        bucket.put_sync(b"a", b"v2").expect("write a v2");
        bucket.put_sync(b"b", b"b1").expect("write b");
        bucket.put_sync(b"c", b"c1").expect("write c");
        db.flush_sync().expect("flush second table");

        bucket
            .delete_range_sync(KeyRange::half_open(b"b", b"d"))
            .expect("range delete b and c");
        db.flush_sync().expect("flush tombstone table");

        let before_manifest =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        let before_tables = before_manifest
            .tables()
            .get("default")
            .expect("default table list");
        assert_eq!(before_tables.len(), 3);
        let before_table_paths = before_tables
            .iter()
            .map(|properties| table::table_path(&path, properties.id))
            .collect::<Vec<_>>();

        db.compact_range_sync(KeyRange::all())
            .expect("manual compaction succeeds");

        assert_eq!(
            snapshot
                .get_sync(&bucket, b"a")
                .expect("snapshot reads old a"),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"a").expect("current reads new a"),
            Some(b"v2".to_vec())
        );
        assert_eq!(bucket.get_sync(b"b").expect("b is range-deleted"), None);
        assert_eq!(bucket.get_sync(b"c").expect("c is range-deleted"), None);

        let after_manifest =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest rereads");
        let after_tables = after_manifest
            .tables()
            .get("default")
            .expect("default compacted table list");
        assert_eq!(after_tables.len(), 1);
        assert!(table::table_path(&path, after_tables[0].id).exists());
        for old_path in &before_table_paths {
            assert!(
                old_path.exists(),
                "obsolete compacted table is kept while snapshot is pinned at {}",
                old_path.display()
            );
        }

        drop(snapshot);
        db.flush_sync().expect("cleanup pending obsolete tables");
        for old_path in before_table_paths {
            assert!(
                !old_path.exists(),
                "obsolete compacted table still exists at {}",
                old_path.display()
            );
        }
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after flushed compaction");

    {
        let db = Db::open_sync(options).expect("persistent db reopens after compaction");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(
            bucket.get_sync(b"a").expect("a reads after reopen"),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("b delete survives reopen"),
            None
        );
        assert_eq!(
            bucket.get_sync(b"c").expect("c delete survives reopen"),
            None
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_removes_obsolete_point_delete_without_replacement() {
    let path = temp_db_path("compact-empty-output");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"v1").expect("write a");
        db.flush_sync().expect("flush value table");
        bucket.delete_sync(b"a").expect("delete a");
        db.flush_sync().expect("flush delete table");
        assert_eq!(table_file_paths(&path).len(), 2);

        db.compact_range_sync(KeyRange::all())
            .expect("manual compaction removes obsolete delete");
        assert_eq!(
            bucket.get_sync(b"a").expect("deleted key reads missing"),
            None
        );
        assert!(
            table_file_paths(&path).is_empty(),
            "empty compaction output should remove old tables without writing a replacement"
        );

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        assert!(
            manifest_state
                .tables()
                .get("default")
                .expect("default table list exists")
                .is_empty()
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after empty compaction");

    {
        let db = Db::open_sync(options).expect("persistent db reopens after empty compaction");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(bucket.get_sync(b"a").expect("deleted key reopens"), None);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_keeps_buckets_separate() {
    let path = temp_db_path("compact-buckets");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let users = db.bucket_sync("users").expect("users bucket opens");
        let posts = db.bucket_sync("posts").expect("posts bucket opens");

        users.put_sync(b"1", b"ada").expect("write first user");
        posts.put_sync(b"1", b"hello").expect("write first post");
        db.flush_sync().expect("flush first tables");

        users.put_sync(b"1", b"grace").expect("write second user");
        posts.put_sync(b"2", b"reply").expect("write second post");
        db.flush_sync().expect("flush second tables");

        db.compact_range_sync(KeyRange::all())
            .expect("manual compaction succeeds");

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        assert_eq!(
            manifest_state
                .tables()
                .get("users")
                .expect("users table list")
                .len(),
            1
        );
        assert_eq!(
            manifest_state
                .tables()
                .get("posts")
                .expect("posts table list")
                .len(),
            1
        );
        assert_eq!(
            users.get_sync(b"1").expect("current user reads"),
            Some(b"grace".to_vec())
        );
        assert_eq!(
            posts.get_sync(b"1").expect("first post reads"),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            posts.get_sync(b"2").expect("second post reads"),
            Some(b"reply".to_vec())
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after flushed compaction");

    {
        let db = Db::open_sync(options).expect("persistent db reopens after compaction");
        let users = db.bucket_sync("users").expect("users bucket reopens");
        let posts = db.bucket_sync("posts").expect("posts bucket reopens");

        assert_eq!(
            users.get_sync(b"1").expect("user survives reopen"),
            Some(b"grace".to_vec())
        );
        assert_eq!(
            posts.get_sync(b"1").expect("first post survives reopen"),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            posts.get_sync(b"2").expect("second post survives reopen"),
            Some(b"reply".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_reopen_fails_when_manifest_table_file_is_missing() {
    let path = temp_db_path("missing-table");
    let options = DbOptions::persistent(&path);
    let table_path = flushed_default_table_path(&path, &options);

    fs::remove_file(table_path).expect("remove referenced table");

    let error = Db::open_sync(options).expect_err("missing referenced table fails closed");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_reopen_fails_when_table_checksum_is_corrupt() {
    let path = temp_db_path("corrupt-table-checksum");
    let options = DbOptions::persistent(&path);
    let table_path = flushed_default_table_path(&path, &options);

    let mut bytes = fs::read(&table_path).expect("read table");
    let last = bytes.last_mut().expect("table has payload bytes");
    *last ^= 0xff;
    fs::write(&table_path, bytes).expect("write corrupted table");

    let error = Db::open_sync(options).expect_err("corrupt referenced table fails closed");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_reopen_defers_data_block_checksum_until_read() {
    let path = temp_db_path("corrupt-data-block-read");
    let options = DbOptions::persistent(&path);
    let table_path = flushed_default_table_path(&path, &options);

    corrupt_first_data_block_payload(&table_path);

    {
        let db = Db::open_sync(options).expect("metadata-only table open succeeds");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let error = bucket
            .get_sync(b"a")
            .expect_err("corrupt data block fails when read");
        assert!(matches!(error, Error::Corruption { .. }));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_filter_miss_does_not_read_corrupt_data_block() {
    let path = temp_db_path("filter-miss-skips-data-block");
    let options = DbOptions::persistent(&path);
    let table_path;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write a");
        bucket.put_sync(b"c", b"c1").expect("write c");
        db.flush_sync().expect("flush table");

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        let table_id = manifest_state
            .tables()
            .get("default")
            .and_then(|tables| tables.first())
            .expect("default table exists")
            .id;
        table_path = table::table_path(&path, table_id);
    }

    corrupt_first_data_block_payload(&table_path);

    {
        let db = Db::open_sync(options).expect("metadata-only table open succeeds");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket
                .get_sync(b"b")
                .expect("filter miss should not read data block"),
            None
        );
        assert_eq!(
            db.stats().block_cache_misses,
            0,
            "filter miss should avoid block cache lookup"
        );
        let filter_stats = db.stats().filters;
        assert!(
            filter_stats.table_point_misses + filter_stats.block_point_misses > 0,
            "a point filter should reject the missing key before data-block read"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_prefix_filter_stats_skip_nonmatching_tables() {
    let path = temp_db_path("prefix-filter-stats-skip");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        prefix_extractor: PrefixExtractor::Separator(b':'),
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"user:1", b"ada").expect("write user");
        bucket.put_sync(b"post:1", b"hello").expect("write post");
        db.flush_sync().expect("flush table");
        assert_eq!(db.stats().block_cache_misses, 0);

        let mut observed_filter_miss = false;
        for prefix in [
            b"query:".as_slice(),
            b"repo:",
            b"shop:",
            b"task:",
            b"todo:",
            b"unit:",
        ] {
            let before = db.stats();
            assert!(
                collect_rows(
                    bucket
                        .prefix_sync(prefix)
                        .expect("nonmatching prefix scans")
                )
                .is_empty()
            );
            let after = db.stats();
            let before_misses =
                before.filters.table_prefix_misses + before.filters.block_prefix_misses;
            let after_misses =
                after.filters.table_prefix_misses + after.filters.block_prefix_misses;
            if after_misses > before_misses {
                assert!(
                    after.block_cache_misses <= before.block_cache_misses + 1,
                    "prefix miss may load tombstone metadata but should not need data blocks"
                );
                observed_filter_miss = true;
                break;
            }
        }

        assert!(
            observed_filter_miss,
            "a prefix filter should reject at least one nonmatching prefix"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_reopen_fails_when_table_metadata_differs_from_manifest() {
    let path = temp_db_path("table-metadata-mismatch");
    let options = DbOptions::persistent(&path);
    let _table_path = flushed_default_table_path(&path, &options);

    let manifest_path = manifest::manifest_path(&path);
    let mut store =
        manifest::ManifestStore::open_or_create(manifest_path, false).expect("manifest opens");
    let original = store
        .state()
        .tables()
        .get("default")
        .and_then(|tables| tables.first())
        .expect("default table metadata exists")
        .clone();
    let mut mismatched = original.clone();
    mismatched.largest_sequence = mismatched
        .largest_sequence
        .next()
        .expect("test sequence can increment");
    store
        .replace_tables("default", &[original.id], mismatched)
        .expect("manifest metadata is replaced");

    let error = Db::open_sync(options).expect_err("metadata mismatch fails closed");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_ignores_torn_final_record() {
    let path = temp_db_path("torn-tail");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write a");
        db.persist_sync(DurabilityMode::Flush).expect("flush WAL");
    }

    OpenOptions::new()
        .append(true)
        .open(wal::wal_path(&path))
        .expect("open WAL")
        .write_all(&[0xaa, 0xbb, 0xcc])
        .expect("append torn tail");

    {
        let db = Db::open_sync(options).expect("torn final record is ignored");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket.get_sync(b"a").expect("a replays"),
            Some(b"a1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_checksum_corruption_fails_closed() {
    let path = temp_db_path("checksum-corruption");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write a");
        db.persist_sync(DurabilityMode::Flush).expect("flush WAL");
    }

    let wal_path = wal::wal_path(&path);
    let mut bytes = fs::read(&wal_path).expect("read WAL");
    let last = bytes.last_mut().expect("WAL has payload bytes");
    *last ^= 0xff;
    fs::write(&wal_path, bytes).expect("write corrupted WAL");

    let error = Db::open_sync(options).expect_err("checksum corruption must fail closed");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}
