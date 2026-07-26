use super::*;

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
            2,
            "one publish reserves globally unique output IDs and one installs outputs; cleanup metadata is not cleared by a third publish"
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
