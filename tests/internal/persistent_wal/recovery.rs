use super::*;

#[test]
fn commit_sequence_value_replays_with_the_same_sequence_after_reopen() {
    let path = temp_db_path("commit-sequence-value-reopen");
    let options = DbOptions::persistent(&path);
    let committed_sequence;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let metadata = db.bucket_sync("metadata").expect("metadata bucket opens");
        let mut transaction = db.transaction(TransactionOptions::default());
        transaction
            .put_bucket_with_commit_sequence(
                metadata.name().as_str(),
                b"version",
                b"v1:",
                b"",
            )
            .expect("sequence value stages");
        committed_sequence = transaction
            .commit_sync()
            .expect("transaction commits")
            .read_version()
            .as_u64();
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let metadata = db.bucket_sync("metadata").expect("metadata bucket reopens");
        let value = metadata
            .get_sync(b"version")
            .expect("version reads")
            .expect("version exists");
        assert_eq!(&value[..3], b"v1:");
        assert_eq!(
            u64::from_be_bytes(value[3..11].try_into().expect("sequence bytes")),
            committed_sequence
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
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

    let error = Db::open_sync(options.clone()).expect_err("second writer must fail closed");
    assert!(
        matches!(error, Error::LeaseUnavailable { .. }),
        "expected LeaseUnavailable, got {error:?}"
    );
    assert!(
        lock_path.exists(),
        "failed writer open should leave the owner lock untouched"
    );

    db.close_sync();
    assert!(
        lock_path.exists(),
        "close should keep the lock file inode for the next writer"
    );
    assert!(
        fs::read(&lock_path)
            .expect("lock marker reads after close")
            .is_empty(),
        "close should clear the writer lock owner text"
    );

    let reopened = Db::open_sync(options).expect("writer reopens after close");
    drop(reopened);
    assert!(
        lock_path.exists(),
        "dropping the final writer handle should keep the lock file inode"
    );
    assert!(
        fs::read(&lock_path)
            .expect("lock marker reads after drop")
            .is_empty(),
        "dropping the final writer handle should clear owner text"
    );

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_writer_open_recovers_stale_lock_marker() {
    let path = temp_db_path("writer-lock-stale");
    let options = DbOptions::persistent(&path);
    let lock_path = path.join("LOCK");
    write_file(&lock_path, b"pid=stale\n");

    let db = Db::open_sync(options).expect("stale lock marker does not block open");
    assert_ne!(
        fs::read(&lock_path).expect("new lock marker remains readable"),
        b"pid=stale\n",
        "open should overwrite a stale marker with the new owner"
    );
    assert!(!recovery::recovery_report_path(&path).exists());
    db.close_sync();
    assert!(
        lock_path.exists(),
        "close should keep the recovered writer lock file inode"
    );
    assert!(
        fs::read(&lock_path)
            .expect("recovered lock marker reads after close")
            .is_empty(),
        "close should clear the recovered writer lock owner text"
    );

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
        fs::read(&lock_path)
            .expect("lock marker remains readable")
            .is_empty(),
        "read-only open should not write writer lock owner text"
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
    let _reserved = manifest_store
        .reserve_file_ids(999)
        .expect("advance durable file-id high-water mark");
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
