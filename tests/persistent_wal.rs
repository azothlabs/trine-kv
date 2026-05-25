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
    WriteOptions, codec::CodecId, manifest, table, wal,
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

fn collect_rows(iter: trine_kv::Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|item| {
        let item = item.expect("iterator item reads");
        (item.key, item.value)
    })
    .collect()
}

fn blob_file_paths(path: &std::path::Path) -> Vec<PathBuf> {
    fs::read_dir(path)
        .expect("read test db directory")
        .map(|entry| entry.expect("read directory entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("blob-"))
        })
        .collect()
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
fn persistent_reopen_fails_when_blob_file_is_missing() {
    let path = temp_db_path("missing-blob");
    let options = DbOptions::persistent(&path);
    let keyspace_options = KeyspaceOptions {
        blob_threshold_bytes: 8,
        ..KeyspaceOptions::default()
    };

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", keyspace_options)
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

    let error = Db::open(options).expect_err("missing blob file fails closed");
    assert!(matches!(error, Error::Corruption { .. }));

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
