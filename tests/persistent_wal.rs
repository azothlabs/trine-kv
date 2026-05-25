use std::{
    fs,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use trine_kv::{
    CompressionProfile, Db, DbOptions, DurabilityMode, Error, FilterPolicy, IndexSearchPolicy,
    KeyRange, KeyspaceOptions, PrefixExtractor, PrefixFilterPolicy, Sequence, WriteBatch,
    WriteOptions, manifest, table, wal,
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
    assert!(table::table_path(&path, tables[0].id).exists());

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
