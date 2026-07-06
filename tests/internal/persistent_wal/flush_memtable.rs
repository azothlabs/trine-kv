use super::*;

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

