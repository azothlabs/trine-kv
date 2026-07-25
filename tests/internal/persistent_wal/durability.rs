use super::*;

#[test]
fn obsolete_tables_drop_while_point_snapshot_open() {
    let path = temp_db_path("obsolete-drop-with-snapshot");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a0").expect("write a");
        db.flush_sync().expect("flush first L0 table");
        bucket.put_sync(b"b", b"b0").expect("write b");
        db.flush_sync().expect("flush second L0 table");
        assert_eq!(table_file_paths(&path).len(), 2);

        // A point snapshot pins only a read sequence, not any table handle.
        let snapshot = db.snapshot();

        db.compact_range_sync(KeyRange::all())
            .expect("compact L0 tables into L1");

        // The coarse model kept every obsolete file while any snapshot was open;
        // liveness-gated cleanup frees them because no reader pins their handles.
        assert_eq!(
            table_file_paths(&path).len(),
            1,
            "obsolete inputs are deleted even though a snapshot is open"
        );

        // Reads through the still-open snapshot remain correct (current version).
        assert_eq!(
            snapshot.get_sync(&bucket, b"a").expect("snapshot reads a"),
            Some(b"a0".to_vec())
        );
        assert_eq!(
            snapshot.get_sync(&bucket, b"b").expect("snapshot reads b"),
            Some(b"b0".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn obsolete_table_files_kept_until_inflight_iterator_drops() {
    let path = temp_db_path("obsolete-keep-until-iter-drops");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a0").expect("write a");
        db.flush_sync().expect("flush first L0 table");
        bucket.put_sync(b"b", b"b0").expect("write b");
        db.flush_sync().expect("flush second L0 table");
        assert_eq!(table_file_paths(&path).len(), 2);

        // A live iterator pins the pre-compaction tables: its scan sources hold
        // their `Arc<Table>` handles for the iterator's whole lifetime.
        let iter = bucket.range_sync(&KeyRange::all()).expect("range iterator");

        db.compact_range_sync(KeyRange::all())
            .expect("compact L0 tables into L1");

        // The output is installed (one new file) but the two obsolete inputs are
        // still pinned by the live iterator, so their files must remain on disk.
        assert_eq!(
            table_file_paths(&path).len(),
            3,
            "obsolete inputs stay on disk while an iterator pins them"
        );

        // The iterator still reads its consistent pre-compaction view.
        let rows = collect_rows(iter);
        assert_eq!(
            rows,
            vec![
                (b"a".to_vec(), b"a0".to_vec()),
                (b"b".to_vec(), b"b0".to_vec()),
            ]
        );

        // With the pin released, the next cleanup pass deletes the obsolete files.
        db.flush_sync().expect("run cleanup pass");
        assert_eq!(
            table_file_paths(&path).len(),
            1,
            "obsolete inputs are reclaimed once no reader pins them"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn compaction_drops_range_deleted_keys_at_source() {
    let path = temp_db_path("compaction-drops-range-deleted");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for index in 0..50u32 {
            bucket
                .put_sync(format!("k{index:03}"), b"v")
                .expect("write key");
        }
        db.flush_sync().expect("flush data");

        // Delete the middle band, then re-add one key inside it (a write made
        // after the delete must survive).
        bucket
            .delete_range_sync(KeyRange::half_open(b"k010", b"k040"))
            .expect("range delete middle band");
        bucket.put_sync("k020", b"re-added").expect("re-add after delete");
        db.flush_sync().expect("flush tombstone");

        db.compact_range_sync(KeyRange::all()).expect("compact all");

        // After compaction the covered rows are physically gone: a full scan
        // wades through no hidden records (read amplification ~1x), not the ~10x
        // it paid before source-level range-tombstone GC.
        let before = db.stats();
        let rows = collect_rows(bucket.range_sync(&KeyRange::all()).expect("scan opens"));
        let after = db.stats();

        let internal = after
            .scan_internal_records
            .saturating_sub(before.scan_internal_records);
        let user = after.scan_user_keys.saturating_sub(before.scan_user_keys);
        let hidden = after
            .scan_tombstone_hidden_keys
            .saturating_sub(before.scan_tombstone_hidden_keys);

        // Live keys: k000..k009, the re-added k020, and k040..k049 = 21.
        assert_eq!(user, 21, "only live keys returned");
        assert_eq!(
            hidden, 0,
            "covered rows are dropped at compaction, not filtered every read"
        );
        assert_eq!(internal, user, "no obsolete records remain to wade through");

        let keys = rows
            .iter()
            .map(|(key, _)| String::from_utf8(key.clone()).unwrap())
            .collect::<Vec<_>>();
        assert!(
            keys.contains(&"k020".to_string()),
            "a write made after the delete survives"
        );
        assert!(
            !keys.contains(&"k015".to_string()),
            "a range-deleted key is gone"
        );
        assert_eq!(
            bucket.get_sync(b"k020").expect("read re-added key"),
            Some(b"re-added".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn compaction_keeps_range_deleted_keys_for_older_snapshot() {
    let path = temp_db_path("compaction-keeps-for-snapshot");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for index in 0..20u32 {
            bucket
                .put_sync(format!("k{index:03}"), b"v")
                .expect("write key");
        }
        db.flush_sync().expect("flush data");

        // Snapshot taken before the delete: its read sequence holds retention, so
        // compaction must NOT drop the covered rows it can still observe.
        let snapshot = db.snapshot();

        bucket
            .delete_range_sync(KeyRange::half_open(b"k005", b"k015"))
            .expect("range delete");
        db.flush_sync().expect("flush tombstone");
        db.compact_range_sync(KeyRange::all()).expect("compact all");

        // The older snapshot still reads the pre-delete value.
        assert_eq!(
            snapshot
                .get_sync(&bucket, b"k010")
                .expect("snapshot reads covered key"),
            Some(b"v".to_vec())
        );
        // Current readers see the delete.
        assert_eq!(bucket.get_sync(b"k010").expect("current read"), None);

        // After the snapshot drops, a fresh compaction drops the covered rows.
        // Re-write a live key inside the bottom table's span so the next
        // compaction overlaps and rewrites that run.
        drop(snapshot);
        bucket
            .put_sync("k001", b"v")
            .expect("rewrite live key to force overlap");
        db.flush_sync().expect("flush");
        db.compact_range_sync(KeyRange::all()).expect("compact again");

        let before = db.stats();
        let _ = collect_rows(bucket.range_sync(&KeyRange::all()).expect("scan opens"));
        let after = db.stats();
        let hidden = after
            .scan_tombstone_hidden_keys
            .saturating_sub(before.scan_tombstone_hidden_keys);
        assert_eq!(hidden, 0, "covered rows reclaimed once no snapshot needs them");
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn compaction_preserves_put_after_range_delete_in_same_batch() {
    let path = temp_db_path("same-batch-range-delete-compaction");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"m", b"old").expect("old value writes");
        db.flush_sync().expect("old value flushes");

        let mut batch = WriteBatch::new();
        batch.delete_range(KeyRange::half_open(b"a", b"z"));
        batch.put(b"m", b"new");
        db.write_sync(batch, WriteOptions::default())
            .expect("same-batch delete then put commits");
        db.flush_sync().expect("same-batch table flushes");
        db.compact_range_sync(KeyRange::all())
            .expect("tables compact");

        assert_eq!(
            bucket.get_sync(b"m").expect("value reads after compaction"),
            Some(b"new".to_vec())
        );
    }
    {
        let db = Db::open_sync(options).expect("database reopens");
        assert_eq!(
            db.get_sync(b"m").expect("value reads after reopen"),
            Some(b"new".to_vec())
        );
    }
    fs::remove_dir_all(path).expect("cleanup test database");
}

#[test]
fn strict_durability_write_persists_and_reopens() {
    let path = temp_db_path("strict-durability-write");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        // Strict full sync (F_FULLFSYNC on macOS) for a power-loss-durable commit.
        let commit = db
            .put_with_options_sync(b"k", b"v", WriteOptions::sync_all_strict())
            .expect("strict-durability write commits");
        assert!(commit.read_version().as_u64() > 0);
        assert_eq!(bucket.get_sync(b"k").expect("read back"), Some(b"v".to_vec()));
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket.get_sync(b"k").expect("strict write survives reopen"),
            Some(b"v".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn database_default_durability_can_be_configured_strict() {
    let path = temp_db_path("configured-strict-default");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::SyncAllStrict);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        // A plain put (no per-write options) inherits the strict database floor.
        bucket.put_sync(b"k", b"v").expect("write under strict default");
        assert_eq!(bucket.get_sync(b"k").expect("read back"), Some(b"v".to_vec()));
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            bucket.get_sync(b"k").expect("strict default survives reopen"),
            Some(b"v".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}
