use trine_kv::{Db, DbOptions, Error, ReadVersion, WriteBatch, WriteOptions};

#[test]
fn write_buffer_budget_reads_delta_backed_in_memory_writes() {
    let mut options = DbOptions::memory();
    options.write_buffer_bytes = 1;
    let db = Db::open_sync(options).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    bucket.put_sync(b"user:1", b"ada").expect("write user");

    let stats = db.stats();
    assert!(
        stats.memtable_bytes > 0,
        "delta-backed writes still count as in-memory write data"
    );
    assert_eq!(stats.immutable_memtables, 0);
    assert_eq!(
        bucket.get_sync(b"user:1").expect("point read sees delta"),
        Some(b"ada".to_vec())
    );
}

#[test]
fn point_writes_deletes_and_snapshot_reads_are_mvcc_visible() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    assert_eq!(bucket.get_sync(b"a").expect("initial read"), None);

    bucket.put_sync(b"a", b"v1").expect("first write");
    let snapshot = db.snapshot();

    bucket.put_sync(b"a", b"v2").expect("second write");
    assert_eq!(
        bucket.get_sync(b"a").expect("current read"),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        snapshot.get_sync(&bucket, b"a").expect("snapshot read"),
        Some(b"v1".to_vec())
    );

    bucket.delete_sync(b"a").expect("point delete");
    assert_eq!(bucket.get_sync(b"a").expect("deleted read"), None);
    assert_eq!(
        snapshot
            .get_sync(&bucket, b"a")
            .expect("snapshot survives delete"),
        Some(b"v1".to_vec())
    );
}

#[test]
fn snapshots_pin_and_release_read_sequences() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    assert_eq!(db.stats().active_snapshots, 0);

    let snapshot = db.snapshot();
    assert_eq!(db.stats().active_snapshots, 1);

    let snapshot_clone = snapshot.clone();
    assert_eq!(db.stats().active_snapshots, 2);

    drop(snapshot_clone);
    assert_eq!(db.stats().active_snapshots, 1);

    drop(snapshot);
    assert_eq!(db.stats().active_snapshots, 0);
}

#[test]
fn write_batch_commits_multiple_buckets_at_one_sequence() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let users = db.bucket_sync("users").expect("users bucket opens");
    let posts = db.bucket_sync("posts").expect("posts bucket opens");

    let mut batch = WriteBatch::new();
    batch
        .put_bucket("users", b"1", b"ada")
        .expect("stage users write");
    batch
        .put_bucket("posts", b"1", b"hello")
        .expect("stage posts write");

    let info = db
        .write_sync(batch, WriteOptions::default())
        .expect("batch commits");
    assert_eq!(info.sequence().get(), 1);
    assert_eq!(
        users.get_sync(b"1").expect("users read"),
        Some(b"ada".to_vec())
    );
    assert_eq!(
        posts.get_sync(b"1").expect("posts read"),
        Some(b"hello".to_vec())
    );
}

#[test]
fn read_versions_track_latest_and_empty_batches_do_not_advance() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    assert_eq!(db.latest_read_version(), ReadVersion::ZERO);
    assert_eq!(db.oldest_retained_read_version(), ReadVersion::ZERO);

    let empty = db
        .write_sync(WriteBatch::new(), WriteOptions::default())
        .expect("empty batch is accepted");
    assert_eq!(empty.read_version(), ReadVersion::ZERO);
    assert_eq!(db.latest_read_version(), ReadVersion::ZERO);

    let first = db
        .put_with_options_sync(b"a", b"v1", WriteOptions::default())
        .expect("write commits");
    assert_eq!(first.sequence().get(), 1);
    assert_eq!(first.read_version(), ReadVersion::from_u64(1));
    assert_eq!(db.latest_read_version(), first.read_version());
    assert_eq!(db.oldest_retained_read_version(), first.read_version());
}

#[test]
fn snapshot_at_validates_read_version_bounds_and_pins_history() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");

    let first = db
        .put_with_options_sync(b"a", b"v1", WriteOptions::default())
        .expect("first write commits");
    let first_version = first.read_version();
    let keep_first = db
        .snapshot_at(first_version)
        .expect("latest read version is retained");

    let second = db
        .put_with_options_sync(b"a", b"v2", WriteOptions::default())
        .expect("second write commits");
    let second_version = second.read_version();
    assert_eq!(db.latest_read_version(), second_version);
    assert_eq!(db.oldest_retained_read_version(), first_version);

    let snapshot = db
        .snapshot_at(first_version)
        .expect("active pin keeps first version retained");
    assert_eq!(snapshot.read_version(), first_version);
    assert_eq!(
        db.get_at_sync(&snapshot, b"a")
            .expect("old snapshot reads old value"),
        Some(b"v1".to_vec())
    );

    let too_new = ReadVersion::from_u64(second_version.as_u64() + 1);
    let error = db
        .snapshot_at(too_new)
        .expect_err("future read version is rejected");
    assert!(matches!(
        error,
        Error::ReadVersionTooNew {
            requested,
            latest
        } if requested == too_new && latest == second_version
    ));

    drop(snapshot);
    drop(keep_first);
    assert_eq!(db.oldest_retained_read_version(), second_version);

    let error = db
        .snapshot_at(first_version)
        .expect_err("unretained read version is expired");
    assert!(matches!(
        error,
        Error::ReadVersionExpired {
            requested,
            oldest_retained
        } if requested == first_version && oldest_retained == second_version
    ));
}

#[test]
fn read_version_retention_window_keeps_recent_history() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(2))
        .expect("memory db opens");

    let first = db
        .put_with_options_sync(b"a", b"v1", WriteOptions::default())
        .expect("first write");
    let first_version = first.read_version();
    let second = db
        .put_with_options_sync(b"a", b"v2", WriteOptions::default())
        .expect("second write");
    let second_version = second.read_version();
    assert_eq!(db.oldest_retained_read_version(), first_version);

    let first_snapshot = db
        .snapshot_at(first_version)
        .expect("retention window keeps first version");
    assert_eq!(
        db.get_at_sync(&first_snapshot, b"a")
            .expect("old version reads"),
        Some(b"v1".to_vec())
    );
    drop(first_snapshot);

    let third = db
        .put_with_options_sync(b"a", b"v3", WriteOptions::default())
        .expect("third write");
    assert_eq!(db.oldest_retained_read_version(), second_version);
    let error = db
        .snapshot_at(first_version)
        .expect_err("first version moved out of the retention window");
    assert!(matches!(
        error,
        Error::ReadVersionExpired {
            requested,
            oldest_retained
        } if requested == first_version && oldest_retained == second_version
    ));
    assert_eq!(db.latest_read_version(), third.read_version());
}

#[test]
fn checkpoints_pin_named_read_versions_until_deleted() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");

    db.put_sync(b"a", b"v1").expect("first write");
    let checkpoint = db
        .create_checkpoint_sync("before-update")
        .expect("checkpoint creates");
    assert_eq!(
        db.checkpoint_read_version_sync("before-update")
            .expect("checkpoint reads back"),
        checkpoint
    );

    db.put_sync(b"a", b"v2").expect("second write");
    assert_eq!(db.oldest_retained_read_version(), checkpoint);
    let snapshot = db
        .snapshot_at(checkpoint)
        .expect("checkpoint keeps old version");
    assert_eq!(
        db.get_at_sync(&snapshot, b"a")
            .expect("checkpoint snapshot reads old value"),
        Some(b"v1".to_vec())
    );
    drop(snapshot);

    let duplicate = db
        .create_checkpoint_sync("before-update")
        .expect_err("duplicate checkpoint is rejected");
    assert!(matches!(duplicate, Error::CheckpointAlreadyExists { .. }));

    db.delete_checkpoint_sync("before-update")
        .expect("checkpoint deletes");
    let missing = db
        .checkpoint_read_version_sync("before-update")
        .expect_err("deleted checkpoint is missing");
    assert!(matches!(missing, Error::CheckpointNotFound { .. }));
    assert_eq!(db.oldest_retained_read_version(), db.latest_read_version());
}

#[test]
fn named_batch_methods_reject_reserved_default_bucket_name() {
    let mut batch = WriteBatch::new();
    let error = batch
        .put_bucket("default", b"a", b"b")
        .expect_err("default writes use batch.put");
    assert!(matches!(error, Error::InvalidOptions { .. }));

    let error = batch
        .delete_bucket("", b"a")
        .expect_err("empty named bucket is rejected");
    assert!(matches!(error, Error::InvalidOptions { .. }));

    assert!(batch.is_empty());
}

#[test]
fn failed_batch_does_not_partially_apply() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    let mut batch = WriteBatch::new();
    batch.put(b"a", b"visible only if batch commits");
    batch
        .put_bucket("missing", b"b", b"nope")
        .expect("stage missing-bucket write");

    let error = db
        .write_sync(batch, WriteOptions::default())
        .expect_err("missing bucket rejects whole batch");
    assert!(matches!(error, Error::BucketMissing { .. }));
    assert_eq!(bucket.get_sync(b"a").expect("no partial write"), None);
}

#[test]
fn duplicate_keys_in_one_batch_use_later_operation() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    let mut put_then_delete = WriteBatch::new();
    put_then_delete.put(b"a", b"v1");
    put_then_delete.delete(b"a");
    db.write_sync(put_then_delete, WriteOptions::default())
        .expect("batch commits");
    assert_eq!(bucket.get_sync(b"a").expect("later delete wins"), None);

    let mut delete_then_put = WriteBatch::new();
    delete_then_put.delete(b"a");
    delete_then_put.put(b"a", b"v2");
    db.write_sync(delete_then_put, WriteOptions::default())
        .expect("batch commits");
    assert_eq!(
        bucket.get_sync(b"a").expect("later put wins"),
        Some(b"v2".to_vec())
    );
}
