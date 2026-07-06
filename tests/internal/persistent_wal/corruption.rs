use super::*;

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
        // The data the snapshot needs is retained in the compaction output, so
        // the obsolete inputs are reclaimed immediately by liveness-gated
        // cleanup even though the snapshot is still pinned (no reader holds the
        // obsolete table handles).
        for old_path in &before_table_paths {
            assert!(
                !old_path.exists(),
                "obsolete compacted table is reclaimed despite the pinned snapshot at {}",
                old_path.display()
            );
        }

        // The pinned snapshot still reads its consistent old view from the
        // retained output table.
        assert_eq!(
            snapshot
                .get_sync(&bucket, b"a")
                .expect("snapshot still reads old a after cleanup"),
            Some(b"v1".to_vec())
        );
        drop(snapshot);
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
    let options = DbOptions::persistent(&path).with_default_bucket_options(BucketOptions {
        filter_policy: FilterPolicy::Disabled,
        prefix_filter_policy: PrefixFilterPolicy::Disabled,
        ..BucketOptions::default()
    });
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
fn persistent_pinned_filter_table_rejects_corrupt_data_block_on_open() {
    let path = temp_db_path("filter-table-corrupt-data-block");
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

    let error = Db::open_sync(options).expect_err("pinned filter table must verify data blocks");
    assert!(matches!(error, Error::Corruption { .. }));

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
fn persistent_wal_rejects_truncated_confirmed_tail() {
    let path = temp_db_path("confirmed-tail-truncated");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write a");
    }

    let wal_path = wal::wal_path(&path);
    let mut bytes = fs::read(&wal_path).expect("read WAL");
    let new_len = bytes
        .len()
        .checked_sub(3)
        .expect("test WAL has a payload tail");
    bytes.truncate(new_len);
    fs::write(&wal_path, bytes).expect("write truncated WAL");

    let error = Db::open_sync(options).expect_err("confirmed WAL tail must fail closed");
    assert!(
        matches!(error, Error::Corruption { ref message } if message.contains("confirmed sequence")),
        "expected confirmed sequence corruption, got {error:?}"
    );

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
