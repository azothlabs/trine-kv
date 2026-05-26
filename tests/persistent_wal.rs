use std::{
    fs,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use trine_kv::{
    CompressionProfile, Db, DbOptions, DurabilityMode, Error, FailOnCorruptionPolicy, FilterPolicy,
    IndexSearchPolicy, KeyRange, KeyspaceOptions, PrefixExtractor, PrefixFilterPolicy, Sequence,
    TransactionOptions, WriteBatch, WriteOptions, blob, codec::CodecId, manifest, recovery, table,
    wal,
};

fn temp_db_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("trine-kv-{name}-{}-{nonce}", std::process::id()))
}

fn flushed_default_table_path(path: &std::path::Path, options: &DbOptions) -> PathBuf {
    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"a", b"a1").expect("write a");
        db.flush().expect("flush table");
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

fn level_table_count(stats: &trine_kv::DbStats, level: u32) -> usize {
    stats
        .level_tables
        .iter()
        .find(|level_stats| level_stats.level == level)
        .map_or(0, |level_stats| level_stats.tables)
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
fn persistent_api_helpers_cover_open_options_and_keyspace_writes() {
    let path = temp_db_path("api-helpers");
    let options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    let keyspace_options =
        KeyspaceOptions::default().with_prefix_extractor(PrefixExtractor::Separator(b':'));

    {
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("keyspace opens");

        let insert_info = keyspace
            .insert_with_options(b"user:001", b"Ada", WriteOptions::sync_all())
            .expect("insert with options commits");
        assert_eq!(insert_info.sequence(), Sequence::new(1));

        keyspace
            .insert_with_options(b"user:002", b"Lin", WriteOptions::flush())
            .expect("second insert commits");
        keyspace
            .remove_with_options(b"user:002", WriteOptions::sync_data())
            .expect("remove with options commits");
        keyspace
            .remove_range_with_options(
                KeyRange::half_open(b"unused:000", b"unused:999"),
                WriteOptions::buffered(),
            )
            .expect("range delete with options commits");

        db.flush().expect("flush helper writes table");
    }

    {
        let db = Db::open_read_only(&path).expect("read-only db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("read-only keyspace opens");
        assert_eq!(
            keyspace.get(b"user:001").expect("user reads"),
            Some(b"Ada".to_vec())
        );
        assert_eq!(keyspace.get(b"user:002").expect("deleted user reads"), None);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_replays_point_and_range_batches() {
    let path = temp_db_path("wal-replay");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"a", b"a1").expect("write a");
        keyspace.insert(b"b", b"b1").expect("write b");
        keyspace.insert(b"c", b"c1").expect("write c");
        keyspace.remove(b"b").expect("delete b");
        keyspace
            .remove_range(KeyRange::half_open(b"c", b"d"))
            .expect("range delete c");
        db.persist(DurabilityMode::Flush).expect("flush WAL");
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");

        assert_eq!(db.stats().live_keyspaces, 1);
        assert_eq!(keyspace.get(b"a").expect("a replays"), Some(b"a1".to_vec()));
        assert_eq!(keyspace.get(b"b").expect("b delete replays"), None);
        assert_eq!(keyspace.get(b"c").expect("range delete replays"), None);

        let mut batch = WriteBatch::new();
        batch.insert("default", b"d", b"d1");
        let info = db
            .write(
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
fn persistent_wal_replays_cross_keyspace_batch() {
    let path = temp_db_path("cross-keyspace");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        db.keyspace("users", KeyspaceOptions::default())
            .expect("users keyspace opens");
        db.keyspace("posts", KeyspaceOptions::default())
            .expect("posts keyspace opens");

        let mut batch = WriteBatch::new();
        batch.insert("users", b"1", b"ada");
        batch.insert("posts", b"1", b"hello");
        db.write(
            batch,
            WriteOptions {
                durability: DurabilityMode::Flush,
            },
        )
        .expect("cross-keyspace batch commits");
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let users = db
            .keyspace("users", KeyspaceOptions::default())
            .expect("users keyspace reopens");
        let posts = db
            .keyspace("posts", KeyspaceOptions::default())
            .expect("posts keyspace reopens");

        assert_eq!(
            users.get(b"1").expect("users replay"),
            Some(b"ada".to_vec())
        );
        assert_eq!(
            posts.get(b"1").expect("posts replay"),
            Some(b"hello".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_manifest_keeps_keyspace_options_across_reopen() {
    let path = temp_db_path("manifest-keyspace-options");
    let options = DbOptions::persistent(&path);
    let keyspace_options = KeyspaceOptions {
        allow_empty_keys: false,
        compression: CompressionProfile::Fast,
        block_bytes: 4096,
        filter_policy: FilterPolicy::Bloom { bits_per_key: 12 },
        prefix_extractor: PrefixExtractor::Separator(b':'),
        prefix_filter_policy: PrefixFilterPolicy::Bloom { bits_per_prefix: 8 },
        index_search_policy: IndexSearchPolicy::Binary,
        blob_threshold_bytes: 128 * 1024,
    };

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("users", keyspace_options.clone())
            .expect("keyspace opens");

        keyspace.insert(b"user:1", b"ada").expect("write user row");
        db.persist(DurabilityMode::Flush).expect("flush WAL");
    }

    let manifest_state =
        manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
    assert_eq!(manifest_state.wal_replay_floor(), Sequence::ZERO);
    assert_eq!(
        manifest_state.keyspaces().get("users"),
        Some(&keyspace_options)
    );

    {
        let db = Db::open(options).expect("persistent db reopens");
        assert_eq!(db.stats().live_keyspaces, 1);

        let keyspace = db
            .keyspace("users", keyspace_options)
            .expect("keyspace reopens with manifest options");
        assert_eq!(
            keyspace.get(b"user:1").expect("user row replays"),
            Some(b"ada".to_vec())
        );

        let error = db
            .keyspace("users", KeyspaceOptions::default())
            .expect_err("wrong keyspace options are rejected");
        assert!(matches!(error, Error::InvalidOptions { .. }));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_writer_open_fails_when_directory_lock_is_held() {
    let path = temp_db_path("writer-lock-held");
    let options = DbOptions::persistent(&path);
    let lock_path = path.join("LOCK");

    let db = Db::open(options.clone()).expect("first writer opens");
    assert!(lock_path.exists());

    let message =
        corruption_message(Db::open(options.clone()).expect_err("second writer must fail closed"));
    assert!(message.contains("database lock is already held"));
    assert!(
        lock_path.exists(),
        "failed writer open should leave the owner lock untouched"
    );

    db.close();
    assert!(
        !lock_path.exists(),
        "close should release the writer directory lock"
    );

    let reopened = Db::open(options).expect("writer reopens after close");
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

    let message =
        corruption_message(Db::open(options).expect_err("existing lock file must fail closed"));
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
        let db = Db::open(options.clone()).expect("writer opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"a", b"a1").expect("write row");
        db.persist(DurabilityMode::Flush).expect("flush WAL");
    }

    let mut read_only_options = options.clone();
    read_only_options.read_only = true;
    read_only_options.create_if_missing = false;
    let read_only_db = Db::open(read_only_options).expect("read-only open succeeds");
    assert!(
        !lock_path.exists(),
        "read-only open should not take the writer directory lock"
    );

    let writer = Db::open(options).expect("writer opens while read-only handle exists");
    assert!(lock_path.exists());

    let keyspace = read_only_db
        .keyspace("default", KeyspaceOptions::default())
        .expect("read-only keyspace opens");
    assert_eq!(
        keyspace.get(b"a").expect("read-only row reads"),
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

    let error = Db::open(options).expect_err("temporary files require explicit repair");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(
        manifest_tmp.exists(),
        "fail-closed recovery should leave evidence untouched"
    );
    assert!(!recovery::recovery_report_path(&path).exists());

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_recovery_repairs_safe_temporary_files_and_writes_report() {
    let path = temp_db_path("recovery-temp-repair");
    let mut options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"a", b"a1").expect("write row");
        db.flush().expect("flush table");
    }

    let manifest_tmp = manifest::manifest_path(&path).with_extension("tmp");
    let wal_tmp = path.join(wal::WAL_REWRITE_TMP_FILE_NAME);
    let blob_tmp = path.join("blob-00000000000000000999.tmp");
    let table_tmp = table::table_path(&path, table::TableId(999)).with_extension("tmp");
    write_file(&manifest_tmp, b"partial manifest publish");
    write_file(&wal_tmp, b"partial WAL rewrite");
    write_file(&blob_tmp, b"partial blob file");
    write_file(&table_tmp, b"partial table file");

    options.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;
    {
        let db = Db::open(options).expect("repair recovery opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");
        assert_eq!(
            keyspace.get(b"a").expect("row survives repair"),
            Some(b"a1".to_vec())
        );
    }

    assert!(!manifest_tmp.exists());
    assert!(!wal_tmp.exists());
    assert!(!blob_tmp.exists());
    assert!(!table_tmp.exists());
    let report = recovery::read_recovery_report(&path).expect("recovery report reads");
    assert_eq!(
        report.repaired_temporary_files(),
        &[
            "MANIFEST.tmp".to_owned(),
            "blob-00000000000000000999.tmp".to_owned(),
            "table-00000000000000000999.tmp".to_owned(),
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
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"a", b"a1").expect("write row");
        db.flush().expect("flush table");

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
        Db::open(options).expect_err("unreferenced table file must fail closed"),
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
    let keyspace_options = KeyspaceOptions {
        blob_threshold_bytes: 8,
        ..KeyspaceOptions::default()
    };

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("keyspace opens");
        keyspace
            .insert(b"a", b"large-value-a-large-value-a".to_vec())
            .expect("write blob value");
        db.flush().expect("flush blob table");
    }

    let unreferenced_blob_path = blob::blob_path(&path, 999);
    write_file(&unreferenced_blob_path, b"unreferenced blob bytes");

    options.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;
    let message =
        corruption_message(Db::open(options).expect_err("unreferenced blob file must fail closed"));
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
fn persistent_recovery_fails_closed_on_malformed_formal_storage_file_name() {
    let path = temp_db_path("recovery-malformed-storage-file");
    let options = DbOptions::persistent(&path);
    let malformed_table_path = path.join("table-not-a-number.trinet");

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        db.keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
    }

    write_file(&malformed_table_path, b"not a valid table file");

    let message =
        corruption_message(Db::open(options).expect_err("malformed table file must fail closed"));
    assert!(message.contains("invalid table file name"));
    assert!(
        malformed_table_path.exists(),
        "startup should leave malformed formal files for operator review"
    );

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_rejects_keyspace_missing_from_manifest() {
    let path = temp_db_path("wal-missing-manifest-keyspace");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"a", b"a1").expect("write a");
        db.persist(DurabilityMode::Flush).expect("flush WAL");
    }

    fs::remove_file(manifest::manifest_path(&path)).expect("remove manifest");

    let error = Db::open(options).expect_err("WAL cannot recreate a missing manifest keyspace");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_writes_table_and_reopen_can_skip_wal() {
    let path = temp_db_path("flush-table");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"a", b"a1").expect("write a");
        keyspace.insert(b"b", b"b1").expect("write b");
        keyspace.insert(b"c", b"c1").expect("write c");
        keyspace.remove(b"b").expect("delete b");
        keyspace
            .remove_range(KeyRange::half_open(b"c", b"d"))
            .expect("range delete c");

        db.flush().expect("flush memtable to table");
        assert_eq!(
            keyspace.get(b"a").expect("a reads from table"),
            Some(b"a1".to_vec())
        );
        assert_eq!(keyspace.get(b"b").expect("b delete reads from table"), None);
        assert_eq!(
            keyspace.get(b"c").expect("range delete reads from table"),
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
        wal::read_batches(&wal::wal_path(&path))
            .expect("WAL reads after checkpoint")
            .is_empty(),
        "flushed batches should not remain in the WAL"
    );

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after flush");

    {
        let db = Db::open(options).expect("persistent db reopens from table");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");

        assert_eq!(
            keyspace.get(b"a").expect("a reads after reopen"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            keyspace.get(b"b").expect("b delete reads after reopen"),
            None
        );
        assert_eq!(
            keyspace.get(b"c").expect("range delete reads after reopen"),
            None
        );

        let mut batch = WriteBatch::new();
        batch.insert("default", b"d", b"d1");
        let info = db
            .write(
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
fn persistent_write_buffer_freezes_active_memtable_and_reads_immutable() {
    let path = temp_db_path("write-buffer-freeze");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;

    {
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"user:1", b"ada").expect("write user");

        let stats = db.stats();
        assert_eq!(stats.immutable_memtables, 1);
        assert_eq!(stats.total_tables, 0);
        assert_eq!(
            keyspace.get(b"user:1").expect("point read sees immutable"),
            Some(b"ada".to_vec())
        );
        assert_eq!(
            collect_rows(keyspace.range(&KeyRange::all()).expect("range reads")),
            vec![(b"user:1".to_vec(), b"ada".to_vec())]
        );
        assert_eq!(
            collect_rows(keyspace.prefix(b"user:").expect("prefix reads")),
            vec![(b"user:1".to_vec(), b"ada".to_vec())]
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

    {
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"k1", b"v1").expect("write k1");
        keyspace
            .remove_range(KeyRange::half_open(b"k", b"l"))
            .expect("range delete freezes");

        assert_eq!(
            keyspace
                .get(b"k1")
                .expect("point read checks immutable tombstone"),
            None
        );
        assert!(collect_rows(keyspace.range(&KeyRange::all()).expect("range reads")).is_empty());
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_immutable_pressure_flushes_before_next_write_and_keeps_new_wal_batch() {
    let path = temp_db_path("immutable-pressure-flush");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 1;

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        let first = keyspace
            .insert_with_options(b"a", b"a1", WriteOptions::sync_all())
            .expect("first write freezes");
        assert_eq!(first.sequence(), Sequence::new(1));
        assert_eq!(db.stats().immutable_memtables, 1);
        assert_eq!(db.stats().total_tables, 0);

        let second = keyspace
            .insert_with_options(b"b", b"b1", WriteOptions::sync_all())
            .expect("second write flushes pressure first");
        assert_eq!(second.sequence(), Sequence::new(2));

        let stats = db.stats();
        assert_eq!(stats.total_tables, 1);
        assert_eq!(stats.immutable_memtables, 1);
        assert_eq!(
            keyspace.get(b"a").expect("flushed row reads"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            keyspace.get(b"b").expect("new immutable row reads"),
            Some(b"b1".to_vec())
        );

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        assert_eq!(manifest_state.wal_replay_floor(), Sequence::new(1));
        let wal_batches = wal::read_batches(&wal::wal_path(&path)).expect("WAL reads");
        assert_eq!(
            wal_batches
                .iter()
                .map(|batch| batch.sequence)
                .collect::<Vec<_>>(),
            vec![Sequence::new(2)]
        );
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");
        assert_eq!(
            keyspace.get(b"a").expect("flushed row survives reopen"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            keyspace.get(b"b").expect("WAL row survives reopen"),
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

    {
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"a", b"a1").expect("write first value");

        let mut txn = db.transaction(TransactionOptions::default());
        assert_eq!(
            txn.get("default", b"a").expect("transaction reads a"),
            Some(b"a1".to_vec())
        );

        keyspace
            .insert(b"a", b"a2")
            .expect("write conflicting value");
        txn.insert("default", b"b", b"b1");
        let error = txn
            .commit()
            .expect_err("immutable memtable update should conflict");
        assert!(matches!(error, Error::Conflict { .. }));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_publish_failure_removes_unpublished_table_and_blob_files() {
    let path = temp_db_path("flush-publish-cleanup");
    let options = DbOptions::persistent(&path);
    let keyspace_options = KeyspaceOptions {
        blob_threshold_bytes: 8,
        ..KeyspaceOptions::default()
    };
    let value = b"large-value-a-large-value-a".to_vec();

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("keyspace opens");
        keyspace
            .insert(b"a", value.clone())
            .expect("write blob value");

        let manifest_tmp_dir = manifest::manifest_path(&path).with_extension("tmp");
        fs::create_dir(&manifest_tmp_dir).expect("block manifest tmp path");

        let error = db.flush().expect_err("manifest publish should fail");
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
            keyspace
                .get(b"a")
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
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"a", b"old-a").expect("write old a");
        db.flush().expect("flush first L0 table");
        keyspace.insert(b"b", b"old-b").expect("write b");
        db.flush().expect("flush second L0 table");
        assert_eq!(default_table_levels(&path), vec![0, 0]);

        db.compact_range(KeyRange::all())
            .expect("compact L0 tables");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            keyspace.get(b"a").expect("compacted a reads"),
            Some(b"old-a".to_vec())
        );

        keyspace.insert(b"a", b"new-a").expect("write newer L0 a");
        db.flush().expect("flush newer L0 table");
        assert_eq!(default_table_levels(&path), vec![0, 1]);
        assert_eq!(
            keyspace.get(b"a").expect("newer L0 a reads"),
            Some(b"new-a".to_vec())
        );

        db.compact_range(KeyRange::all())
            .expect("compact L0 into L1");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            keyspace
                .get(b"a")
                .expect("newer a survives second compaction"),
            Some(b"new-a".to_vec())
        );
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            keyspace.get(b"a").expect("newer L0 a reopens"),
            Some(b"new-a".to_vec())
        );
        assert_eq!(
            keyspace.get(b"b").expect("compacted b reopens"),
            Some(b"old-b".to_vec())
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
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"a", b"a1").expect("write a");
        db.flush().expect("first flush stays L0");
        assert_eq!(default_table_levels(&path), vec![0]);

        keyspace.insert(b"b", b"b1").expect("write b");
        db.flush().expect("second flush triggers compaction");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            keyspace.get(b"a").expect("a reads after auto compaction"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            keyspace.get(b"b").expect("b reads after auto compaction"),
            Some(b"b1".to_vec())
        );

        keyspace.insert(b"a", b"a2").expect("write newer a");
        db.flush().expect("new L0 below pressure limit");
        assert_eq!(default_table_levels(&path), vec![0, 1]);
        assert_eq!(
            keyspace.get(b"a").expect("newer a reads over L1"),
            Some(b"a2".to_vec())
        );
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");
        assert_eq!(default_table_levels(&path), vec![0, 1]);
        assert_eq!(
            keyspace.get(b"a").expect("newer a reopens"),
            Some(b"a2".to_vec())
        );
        assert_eq!(keyspace.get(b"b").expect("b reopens"), Some(b"b1".to_vec()));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_background_workers_flush_and_compact_pressure() {
    let path = temp_db_path("background-maintenance");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;
    options.max_l0_files = 1;
    options.background_worker_count = 1;

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"a", b"a1").expect("write a");
        wait_until("background flush of first immutable memtable", || {
            let stats = db.stats();
            stats.total_tables == 1 && stats.immutable_memtables == 0
        });

        keyspace.insert(b"b", b"b1").expect("write b");
        wait_until("background compaction after L0 pressure", || {
            default_table_levels(&path) == vec![1]
        });

        assert_eq!(
            keyspace.get(b"a").expect("a reads after background work"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            keyspace.get(b"b").expect("b reads after background work"),
            Some(b"b1".to_vec())
        );
        db.close();
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(keyspace.get(b"a").expect("a reopens"), Some(b"a1".to_vec()));
        assert_eq!(keyspace.get(b"b").expect("b reopens"), Some(b"b1".to_vec()));
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
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        let manifest_tmp_dir = manifest::manifest_path(&path).with_extension("tmp");
        fs::create_dir(&manifest_tmp_dir).expect("block manifest tmp path");
        keyspace.insert(b"a", b"a1").expect("write schedules flush");

        let mut surfaced = false;
        for index in 0..100 {
            thread::sleep(std::time::Duration::from_millis(20));
            let key = format!("probe-{index:03}").into_bytes();
            match keyspace.insert(key, b"value") {
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
        db.close();
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_splits_outputs_and_moves_overfull_l1_down() {
    let path = temp_db_path("compaction-split-output");
    let mut options = DbOptions::persistent(&path);
    options.target_table_bytes = 240;
    options.level_size_multiplier = 2;
    let keyspace_options = KeyspaceOptions {
        compression: CompressionProfile::None,
        block_bytes: 256,
        ..KeyspaceOptions::default()
    };

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("keyspace opens");

        for index in 0..30 {
            let key = format!("key-{index:03}").into_bytes();
            let value = format!("value-{index:03}-{}", "x".repeat(48)).into_bytes();
            keyspace.insert(key, value).expect("write first batch");
        }
        db.flush().expect("flush first L0 table");
        for index in 30..60 {
            let key = format!("key-{index:03}").into_bytes();
            let value = format!("value-{index:03}-{}", "y".repeat(48)).into_bytes();
            keyspace.insert(key, value).expect("write second batch");
        }
        db.flush().expect("flush second L0 table");

        db.compact_range(KeyRange::all())
            .expect("manual compaction splits L1 output");
        let levels = default_table_levels(&path);
        assert!(levels.len() > 1, "small target should split output tables");
        assert!(levels.iter().all(|level| *level == 1));

        db.compact_range(KeyRange::all())
            .expect("overfull L1 compacts into L2");
        let levels = default_table_levels(&path);
        assert!(levels.len() > 1, "L2 output should stay split");
        assert!(levels.iter().all(|level| *level == 2));

        for index in [0, 17, 30, 59] {
            let key = format!("key-{index:03}").into_bytes();
            let expected_prefix = format!("value-{index:03}-").into_bytes();
            let value = keyspace
                .get(&key)
                .expect("value reads")
                .expect("key exists");
            assert!(value.starts_with(&expected_prefix));
        }
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let keyspace = db
            .keyspace("default", keyspace_options)
            .expect("keyspace reopens");
        assert!(default_table_levels(&path).iter().all(|level| *level == 2));
        assert_eq!(
            keyspace.get(b"key-059").expect("latest key reopens"),
            Some(format!("value-059-{}", "y".repeat(48)).into_bytes())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_stats_report_tables_blobs_and_compactions() {
    let path = temp_db_path("live-stats");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 1;
    let keyspace_options = KeyspaceOptions {
        blob_threshold_bytes: 4,
        ..KeyspaceOptions::default()
    };

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options)
            .expect("keyspace opens");
        assert_eq!(db.stats().live_keyspaces, 1);

        let large_a = b"large-a".to_vec();
        keyspace
            .insert(b"a", large_a.clone())
            .expect("write large a");
        assert!(
            db.stats().memtable_bytes > 0,
            "unflushed writes should contribute to memtable stats"
        );
        db.flush().expect("first flush stays L0");
        let stats = db.stats();
        assert_eq!(stats.total_tables, 1);
        assert_eq!(stats.l0_tables, 1);
        assert_eq!(level_table_count(&stats, 0), 1);
        assert!(stats.table_bytes > 0);
        assert_eq!(stats.live_blob_files, 1);
        assert_eq!(stats.live_blob_bytes, large_a.len() as u64);

        let large_b = b"large-b".to_vec();
        keyspace
            .insert(b"b", large_b.clone())
            .expect("write large b");
        db.flush().expect("second flush triggers compaction");
        let stats = db.stats();
        assert_eq!(stats.total_tables, 1);
        assert_eq!(stats.l0_tables, 0);
        assert_eq!(level_table_count(&stats, 1), 1);
        assert_eq!(stats.live_blob_files, 2);
        assert_eq!(
            stats.live_blob_bytes,
            (large_a.len() + large_b.len()) as u64
        );
        assert_eq!(stats.compaction_runs, 1);
        assert_eq!(stats.compaction_input_tables, 2);
        assert_eq!(stats.compaction_output_tables, 1);
        assert!(stats.compaction_input_bytes > 0);
        assert!(stats.compaction_output_bytes > 0);

        let obsolete_blob_path = blob::blob_path(&path, 999);
        write_file(&obsolete_blob_path, b"obsolete");
        let stats = db.stats();
        assert_eq!(stats.obsolete_blob_files, 1);
        assert_eq!(stats.obsolete_blob_bytes, b"obsolete".len() as u64);
        fs::remove_file(obsolete_blob_path).expect("remove test obsolete blob");
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_block_cache_records_hits_and_misses() {
    let path = temp_db_path("block-cache-stats");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        for index in 0..64 {
            keyspace
                .insert(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush().expect("flush table");

        let stats = db.stats();
        assert_eq!(stats.block_cache_hits, 0);
        assert_eq!(stats.block_cache_misses, 0);

        assert_eq!(
            keyspace.get(b"key-032").expect("first cached read"),
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
            keyspace.get(b"key-032").expect("second cached read"),
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
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        for index in 0..64 {
            keyspace
                .insert(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush().expect("flush table");

        let mut iter = keyspace
            .range(&KeyRange::all())
            .expect("range cursor is created");
        let stats = db.stats();
        assert_eq!(stats.block_cache_hits, 0);
        assert_eq!(
            stats.block_cache_misses, 0,
            "constructing a range cursor should not touch table blocks"
        );

        let first = iter
            .next()
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
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"key-010", b"before-a").expect("write row");
        keyspace.insert(b"key-020", b"before-b").expect("write row");

        let iter = keyspace
            .range(&KeyRange::all())
            .expect("range cursor is created");
        db.flush().expect("flush active memtable");
        keyspace
            .insert(b"key-000", b"after")
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
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        for index in 0..64 {
            keyspace
                .insert(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush().expect("flush table");
        assert_eq!(db.stats().block_cache_misses, 0);

        let mut txn = db.transaction(TransactionOptions::default());
        txn.read_range("default", KeyRange::all())
            .expect("transaction range read succeeds");

        assert!(
            db.stats().block_cache_misses > 0,
            "transaction range read should advance the table cursor"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_preserves_snapshot_versions() {
    let path = temp_db_path("flush-snapshot");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"a", b"v1").expect("write v1");
        let snapshot = db.snapshot();
        keyspace.insert(b"a", b"v2").expect("write v2");

        db.flush().expect("flush table");

        assert_eq!(
            snapshot.get(&keyspace, b"a").expect("snapshot reads table"),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            keyspace.get(b"a").expect("current reads table"),
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
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        for index in 0..160 {
            keyspace
                .insert(
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
                .expect("write indexed row");
        }
        db.flush().expect("flush indexed table");

        assert_eq!(
            keyspace.get(b"key-042").expect("point reads indexed table"),
            Some(b"value-042".to_vec())
        );
        let rows = keyspace
            .range(&KeyRange::half_open(b"key-020", b"key-030"))
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

        let prefix_rows = collect_rows(keyspace.prefix(b"key-12").expect("prefix reads table"));
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
        let db = Db::open(options).expect("persistent db reopens from indexed table");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");

        assert_eq!(
            keyspace.get(b"key-127").expect("point reads after reopen"),
            Some(b"value-127".to_vec())
        );
        let rows = keyspace
            .range(&KeyRange::half_open(b"key-150", b"key-160"))
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
            keyspace
                .prefix(b"key-12")
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
        ("eytzinger", IndexSearchPolicy::Eytzinger),
        ("galloping", IndexSearchPolicy::GallopingWithHint),
    ];

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        for (name, policy) in policies {
            let keyspace_options = KeyspaceOptions {
                index_search_policy: policy,
                prefix_extractor: PrefixExtractor::FixedLen(6),
                ..KeyspaceOptions::default()
            };
            let keyspace = db
                .keyspace(name, keyspace_options)
                .expect("policy keyspace opens");
            for index in 0..80 {
                keyspace
                    .insert(
                        format!("key-{index:03}").into_bytes(),
                        format!("value-{index:03}").into_bytes(),
                    )
                    .expect("write policy row");
            }
        }
        db.flush().expect("flush policy tables");

        for (name, policy) in policies {
            let keyspace_options = KeyspaceOptions {
                index_search_policy: policy,
                prefix_extractor: PrefixExtractor::FixedLen(6),
                ..KeyspaceOptions::default()
            };
            let keyspace = db
                .keyspace(name, keyspace_options)
                .expect("policy keyspace reuses options");
            assert_eq!(
                keyspace.get(b"key-042").expect("policy point reads"),
                Some(b"value-042".to_vec())
            );
            assert_eq!(
                collect_rows(
                    keyspace
                        .range(&KeyRange::half_open(b"key-020", b"key-023"))
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
                collect_rows(keyspace.prefix(b"key-04").expect("policy prefix reads")),
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
        let db = Db::open(options).expect("persistent db reopens");
        for (name, policy) in policies {
            let keyspace_options = KeyspaceOptions {
                index_search_policy: policy,
                prefix_extractor: PrefixExtractor::FixedLen(6),
                ..KeyspaceOptions::default()
            };
            let keyspace = db
                .keyspace(name, keyspace_options)
                .expect("policy keyspace reopens");
            assert_eq!(
                keyspace.get(b"key-042").expect("policy point reopens"),
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
    let fast_options = KeyspaceOptions::default();
    let plain_options = KeyspaceOptions {
        compression: CompressionProfile::None,
        ..KeyspaceOptions::default()
    };

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let fast = db
            .keyspace("fast", fast_options.clone())
            .expect("fast keyspace opens");
        let plain = db
            .keyspace("plain", plain_options.clone())
            .expect("plain keyspace opens");

        for index in 0..64 {
            let value = format!("value-{index:03}-aaaaaaaaaaaaaaaaaaaaaaaa").into_bytes();
            fast.insert(format!("key-{index:03}").into_bytes(), value.clone())
                .expect("write fast row");
            plain
                .insert(format!("key-{index:03}").into_bytes(), value)
                .expect("write plain row");
        }
        db.flush().expect("flush compressed tables");

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
        let db = Db::open(options).expect("persistent db reopens from compressed tables");
        let fast = db
            .keyspace("fast", fast_options)
            .expect("fast keyspace reopens");
        let plain = db
            .keyspace("plain", plain_options)
            .expect("plain keyspace reopens");

        assert_eq!(
            fast.get(b"key-042").expect("fast row reads after reopen"),
            Some(b"value-042-aaaaaaaaaaaaaaaaaaaaaaaa".to_vec())
        );
        assert_eq!(
            plain.get(b"key-042").expect("plain row reads after reopen"),
            Some(b"value-042-aaaaaaaaaaaaaaaaaaaaaaaa".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_prefix_filter_keeps_range_tombstones_authoritative() {
    let path = temp_db_path("prefix-filter-tombstones");
    let options = DbOptions::persistent(&path);
    let keyspace_options = KeyspaceOptions {
        prefix_extractor: PrefixExtractor::Separator(b':'),
        ..KeyspaceOptions::default()
    };

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("keyspace opens");

        keyspace.insert(b"user:1", b"old").expect("write old user");
        keyspace
            .insert(b"user:2", b"live")
            .expect("write live user");
        db.flush().expect("flush user table");

        keyspace.insert(b"post:1", b"post").expect("write post");
        keyspace
            .remove_range(KeyRange::half_open(b"user:1", b"user:2"))
            .expect("range delete one user");
        db.flush().expect("flush post table with user tombstone");

        assert_eq!(
            collect_rows(keyspace.prefix(b"user:").expect("prefix reads users")),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
        assert_eq!(
            collect_rows(keyspace.prefix(b"us").expect("short prefix falls back")),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after prefix-filter flush");

    {
        let db = Db::open(options).expect("persistent db reopens");
        let keyspace = db
            .keyspace("default", keyspace_options)
            .expect("keyspace reopens");

        assert_eq!(
            collect_rows(
                keyspace
                    .prefix(b"user:")
                    .expect("prefix reads after reopen")
            ),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
        assert_eq!(
            collect_rows(keyspace.prefix(b"us").expect("short prefix after reopen")),
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
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"user:1", b"old").expect("write old user");
        db.flush().expect("flush user table");

        keyspace.insert(b"post:1", b"post").expect("write post");
        keyspace
            .remove_range(KeyRange::half_open(b"user:1", b"user:2"))
            .expect("range delete user");
        db.flush().expect("flush post table with user tombstone");

        assert_eq!(keyspace.get(b"user:1").expect("user is hidden"), None);
        assert_eq!(
            keyspace.get(b"post:1").expect("post survives"),
            Some(b"post".to_vec())
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after point-filter flush");

    {
        let db = Db::open(options).expect("persistent db reopens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");

        assert_eq!(keyspace.get(b"user:1").expect("user remains hidden"), None);
        assert_eq!(
            keyspace.get(b"post:1").expect("post survives reopen"),
            Some(b"post".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_blob_values_survive_flush_reopen_and_compaction() {
    let path = temp_db_path("blob-values");
    let options = DbOptions::persistent(&path);
    let keyspace_options = KeyspaceOptions {
        blob_threshold_bytes: 8,
        ..KeyspaceOptions::default()
    };
    let large_a = b"large-value-a-large-value-a".to_vec();
    let large_c = b"large-value-c-large-value-c".to_vec();

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("keyspace opens");

        keyspace
            .insert(b"a", large_a.clone())
            .expect("write blob a");
        keyspace.insert(b"b", b"small").expect("write inline b");
        db.flush().expect("flush first blob table");

        keyspace
            .insert(b"c", large_c.clone())
            .expect("write blob c");
        db.flush().expect("flush second blob table");
        db.compact_range(KeyRange::all())
            .expect("compact blob tables");

        assert_eq!(
            keyspace.get(b"a").expect("blob a reads"),
            Some(large_a.clone())
        );
        assert_eq!(
            keyspace.get(b"b").expect("inline b reads"),
            Some(b"small".to_vec())
        );
        assert_eq!(
            keyspace.get(b"c").expect("blob c reads"),
            Some(large_c.clone())
        );
        assert!(
            blob_file_paths(&path).len() >= 2,
            "flushed blob values should create blob files"
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after blob compaction");

    {
        let db = Db::open(options).expect("persistent db reopens with blob refs");
        let keyspace = db
            .keyspace("default", keyspace_options)
            .expect("keyspace reopens");

        assert_eq!(keyspace.get(b"a").expect("blob a reopens"), Some(large_a));
        assert_eq!(
            keyspace.get(b"b").expect("inline b reopens"),
            Some(b"small".to_vec())
        );
        assert_eq!(keyspace.get(b"c").expect("blob c reopens"), Some(large_c));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_reopen_defers_missing_blob_file_until_read() {
    let path = temp_db_path("missing-blob");
    let options = DbOptions::persistent(&path);
    let keyspace_options = KeyspaceOptions {
        blob_threshold_bytes: 8,
        ..KeyspaceOptions::default()
    };

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("keyspace opens");
        keyspace
            .insert(b"a", b"large-value-a-large-value-a".to_vec())
            .expect("write blob a");
        db.flush().expect("flush blob table");
    }

    let blob_path = blob_file_paths(&path)
        .pop()
        .expect("blob file exists after flush");
    fs::remove_file(blob_path).expect("remove blob file");

    let db = Db::open(options).expect("missing blob is not read during table open");
    let keyspace = db
        .keyspace("default", keyspace_options)
        .expect("keyspace reopens");
    let error = keyspace
        .get(b"a")
        .expect_err("missing blob file fails when value is read");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_removes_blob_files_for_dropped_versions() {
    let path = temp_db_path("compact-dropped-blob-versions");
    let options = DbOptions::persistent(&path);
    let keyspace_options = KeyspaceOptions {
        blob_threshold_bytes: 8,
        ..KeyspaceOptions::default()
    };
    let old_value = b"large-value-a-old-large-value-a-old".to_vec();
    let new_value = b"large-value-a-new-large-value-a-new".to_vec();

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("keyspace opens");

        keyspace
            .insert(b"a", old_value)
            .expect("write old blob value");
        db.flush().expect("flush old blob table");
        keyspace
            .insert(b"a", new_value.clone())
            .expect("write new blob value");
        db.flush().expect("flush new blob table");
        assert_eq!(blob_file_paths(&path).len(), 2);

        db.compact_range(KeyRange::all())
            .expect("manual compaction removes dropped blob");

        assert_eq!(
            keyspace.get(b"a").expect("current blob reads"),
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
        let db = Db::open(options).expect("persistent db reopens after blob cleanup");
        let keyspace = db
            .keyspace("default", keyspace_options)
            .expect("keyspace reopens");
        assert_eq!(keyspace.get(b"a").expect("blob reopens"), Some(new_value));
        assert_eq!(blob_file_paths(&path).len(), 1);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_publish_failure_removes_unpublished_table_and_blob_files() {
    let path = temp_db_path("compact-publish-cleanup");
    let options = DbOptions::persistent(&path);
    let keyspace_options = KeyspaceOptions {
        blob_threshold_bytes: 8,
        ..KeyspaceOptions::default()
    };
    let old_value = b"large-value-a-old-large-value-a-old".to_vec();
    let new_value = b"large-value-a-new-large-value-a-new".to_vec();

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options)
            .expect("keyspace opens");

        keyspace
            .insert(b"a", old_value)
            .expect("write old blob value");
        db.flush().expect("flush old blob table");
        keyspace
            .insert(b"a", new_value.clone())
            .expect("write new blob value");
        db.flush().expect("flush new blob table");

        let mut before_tables = table_file_paths(&path);
        before_tables.sort();
        let before_blobs = blob_file_paths(&path);
        assert_eq!(before_tables.len(), 2);
        assert_eq!(before_blobs.len(), 2);

        let manifest_tmp_dir = manifest::manifest_path(&path).with_extension("tmp");
        fs::create_dir(&manifest_tmp_dir).expect("block manifest tmp path");

        let error = db
            .compact_range(KeyRange::all())
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
            keyspace
                .get(b"a")
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
    let options = DbOptions::persistent(&path);
    let keyspace_options = KeyspaceOptions {
        blob_threshold_bytes: 8,
        ..KeyspaceOptions::default()
    };

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options.clone())
            .expect("keyspace opens");

        keyspace
            .insert(b"a", b"large-value-a-large-value-a".to_vec())
            .expect("write blob value");
        db.flush().expect("flush blob table");
        keyspace.remove(b"a").expect("delete blob key");
        db.flush().expect("flush delete table");
        assert_eq!(blob_file_paths(&path).len(), 1);

        db.compact_range(KeyRange::all())
            .expect("manual compaction removes deleted blob");

        assert_eq!(keyspace.get(b"a").expect("deleted key reads missing"), None);
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
        let db = Db::open(options).expect("persistent db reopens after deleted blob cleanup");
        let keyspace = db
            .keyspace("default", keyspace_options)
            .expect("keyspace reopens");
        assert_eq!(keyspace.get(b"a").expect("deleted key reopens"), None);
        assert!(blob_file_paths(&path).is_empty());
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_keeps_lazy_iterator_table_files_until_pin_released() {
    let path = temp_db_path("compaction-lazy-iterator-file-lifetime");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        for index in 0..64 {
            keyspace
                .insert(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush().expect("flush base table");

        let mut iter = keyspace
            .range(&KeyRange::all())
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

        keyspace
            .insert(b"key-032", b"value-032-new")
            .expect("write overlapping update");
        db.flush().expect("flush overlapping table");
        db.compact_range(KeyRange::all())
            .expect("manual compaction succeeds");

        for old_path in &before_table_paths {
            assert!(
                old_path.exists(),
                "old table file stays available for a lazy iterator at {}",
                old_path.display()
            );
        }

        let first = iter
            .next()
            .expect("first row exists")
            .expect("first row reads after compaction");
        assert_eq!(first.key, b"key-000".to_vec());
        assert_eq!(first.value, b"value-000".to_vec());

        drop(iter);
        db.flush().expect("cleanup pending obsolete tables");
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
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"a", b"v1").expect("write a v1");
        db.flush().expect("flush first table");
        let snapshot = db.snapshot();

        keyspace.insert(b"a", b"v2").expect("write a v2");
        keyspace.insert(b"b", b"b1").expect("write b");
        keyspace.insert(b"c", b"c1").expect("write c");
        db.flush().expect("flush second table");

        keyspace
            .remove_range(KeyRange::half_open(b"b", b"d"))
            .expect("range delete b and c");
        db.flush().expect("flush tombstone table");

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

        db.compact_range(KeyRange::all())
            .expect("manual compaction succeeds");

        assert_eq!(
            snapshot.get(&keyspace, b"a").expect("snapshot reads old a"),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            keyspace.get(b"a").expect("current reads new a"),
            Some(b"v2".to_vec())
        );
        assert_eq!(keyspace.get(b"b").expect("b is range-deleted"), None);
        assert_eq!(keyspace.get(b"c").expect("c is range-deleted"), None);

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
        db.flush().expect("cleanup pending obsolete tables");
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
        let db = Db::open(options).expect("persistent db reopens after compaction");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");

        assert_eq!(
            keyspace.get(b"a").expect("a reads after reopen"),
            Some(b"v2".to_vec())
        );
        assert_eq!(keyspace.get(b"b").expect("b delete survives reopen"), None);
        assert_eq!(keyspace.get(b"c").expect("c delete survives reopen"), None);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_removes_obsolete_point_delete_without_replacement() {
    let path = temp_db_path("compact-empty-output");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"a", b"v1").expect("write a");
        db.flush().expect("flush value table");
        keyspace.remove(b"a").expect("delete a");
        db.flush().expect("flush delete table");
        assert_eq!(table_file_paths(&path).len(), 2);

        db.compact_range(KeyRange::all())
            .expect("manual compaction removes obsolete delete");
        assert_eq!(keyspace.get(b"a").expect("deleted key reads missing"), None);
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
        let db = Db::open(options).expect("persistent db reopens after empty compaction");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");
        assert_eq!(keyspace.get(b"a").expect("deleted key reopens"), None);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_keeps_keyspaces_separate() {
    let path = temp_db_path("compact-keyspaces");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let users = db
            .keyspace("users", KeyspaceOptions::default())
            .expect("users keyspace opens");
        let posts = db
            .keyspace("posts", KeyspaceOptions::default())
            .expect("posts keyspace opens");

        users.insert(b"1", b"ada").expect("write first user");
        posts.insert(b"1", b"hello").expect("write first post");
        db.flush().expect("flush first tables");

        users.insert(b"1", b"grace").expect("write second user");
        posts.insert(b"2", b"reply").expect("write second post");
        db.flush().expect("flush second tables");

        db.compact_range(KeyRange::all())
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
            users.get(b"1").expect("current user reads"),
            Some(b"grace".to_vec())
        );
        assert_eq!(
            posts.get(b"1").expect("first post reads"),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            posts.get(b"2").expect("second post reads"),
            Some(b"reply".to_vec())
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after flushed compaction");

    {
        let db = Db::open(options).expect("persistent db reopens after compaction");
        let users = db
            .keyspace("users", KeyspaceOptions::default())
            .expect("users keyspace reopens");
        let posts = db
            .keyspace("posts", KeyspaceOptions::default())
            .expect("posts keyspace reopens");

        assert_eq!(
            users.get(b"1").expect("user survives reopen"),
            Some(b"grace".to_vec())
        );
        assert_eq!(
            posts.get(b"1").expect("first post survives reopen"),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            posts.get(b"2").expect("second post survives reopen"),
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

    let error = Db::open(options).expect_err("missing referenced table fails closed");
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

    let error = Db::open(options).expect_err("corrupt referenced table fails closed");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_reopen_defers_data_block_checksum_until_read() {
    let path = temp_db_path("corrupt-data-block-read");
    let options = DbOptions::persistent(&path);
    let table_path = flushed_default_table_path(&path, &options);

    corrupt_first_data_block_payload(&table_path);

    let db = Db::open(options).expect("metadata-only table open succeeds");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace reopens");
    let error = keyspace
        .get(b"a")
        .expect_err("corrupt data block fails when read");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_filter_miss_does_not_read_corrupt_data_block() {
    let path = temp_db_path("filter-miss-skips-data-block");
    let options = DbOptions::persistent(&path);
    let table_path;

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"a", b"a1").expect("write a");
        keyspace.insert(b"c", b"c1").expect("write c");
        db.flush().expect("flush table");

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

    let db = Db::open(options).expect("metadata-only table open succeeds");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace reopens");
    assert_eq!(
        keyspace
            .get(b"b")
            .expect("filter miss should not read data block"),
        None
    );
    assert_eq!(
        db.stats().block_cache_misses,
        0,
        "filter miss should avoid block cache lookup"
    );

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

    let error = Db::open(options).expect_err("metadata mismatch fails closed");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_ignores_torn_final_record() {
    let path = temp_db_path("torn-tail");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"a", b"a1").expect("write a");
        db.persist(DurabilityMode::Flush).expect("flush WAL");
    }

    OpenOptions::new()
        .append(true)
        .open(wal::wal_path(&path))
        .expect("open WAL")
        .write_all(&[0xaa, 0xbb, 0xcc])
        .expect("append torn tail");

    {
        let db = Db::open(options).expect("torn final record is ignored");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");
        assert_eq!(keyspace.get(b"a").expect("a replays"), Some(b"a1".to_vec()));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_checksum_corruption_fails_closed() {
    let path = temp_db_path("checksum-corruption");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");
        keyspace.insert(b"a", b"a1").expect("write a");
        db.persist(DurabilityMode::Flush).expect("flush WAL");
    }

    let wal_path = wal::wal_path(&path);
    let mut bytes = fs::read(&wal_path).expect("read WAL");
    let last = bytes.last_mut().expect("WAL has payload bytes");
    *last ^= 0xff;
    fs::write(&wal_path, bytes).expect("write corrupted WAL");

    let error = Db::open(options).expect_err("checksum corruption must fail closed");
    assert!(matches!(error, Error::Corruption { .. }));

    fs::remove_dir_all(path).expect("cleanup test db");
}
