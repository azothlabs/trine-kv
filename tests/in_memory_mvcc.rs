use trine_kv::{Db, DbOptions, Error, WriteBatch, WriteOptions};

#[test]
fn write_buffer_budget_reads_delta_backed_in_memory_writes() {
    let mut options = DbOptions::memory();
    options.write_buffer_bytes = 1;
    let db = Db::memory(options).expect("memory db opens");
    let bucket = db.default_bucket().expect("bucket opens");

    bucket.put(b"user:1", b"ada").expect("write user");

    let stats = db.stats();
    assert!(
        stats.memtable_bytes > 0,
        "delta-backed writes still count as in-memory write data"
    );
    assert_eq!(stats.immutable_memtables, 0);
    assert_eq!(
        bucket.get(b"user:1").expect("point read sees delta"),
        Some(b"ada".to_vec())
    );
}

#[test]
fn point_writes_deletes_and_snapshot_reads_are_mvcc_visible() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket().expect("bucket opens");

    assert_eq!(bucket.get(b"a").expect("initial read"), None);

    bucket.put(b"a", b"v1").expect("first write");
    let snapshot = db.snapshot();

    bucket.put(b"a", b"v2").expect("second write");
    assert_eq!(
        bucket.get(b"a").expect("current read"),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        snapshot.get(&bucket, b"a").expect("snapshot read"),
        Some(b"v1".to_vec())
    );

    bucket.delete(b"a").expect("point delete");
    assert_eq!(bucket.get(b"a").expect("deleted read"), None);
    assert_eq!(
        snapshot
            .get(&bucket, b"a")
            .expect("snapshot survives delete"),
        Some(b"v1".to_vec())
    );
}

#[test]
fn snapshots_pin_and_release_read_sequences() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
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
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let users = db.bucket("users").expect("users bucket opens");
    let posts = db.bucket("posts").expect("posts bucket opens");

    let mut batch = WriteBatch::new();
    batch
        .put_bucket("users", b"1", b"ada")
        .expect("stage users write");
    batch
        .put_bucket("posts", b"1", b"hello")
        .expect("stage posts write");

    let info = db
        .write(batch, WriteOptions::default())
        .expect("batch commits");
    assert_eq!(info.sequence().get(), 1);
    assert_eq!(users.get(b"1").expect("users read"), Some(b"ada".to_vec()));
    assert_eq!(
        posts.get(b"1").expect("posts read"),
        Some(b"hello".to_vec())
    );
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
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket().expect("bucket opens");

    let mut batch = WriteBatch::new();
    batch.put(b"a", b"visible only if batch commits");
    batch
        .put_bucket("missing", b"b", b"nope")
        .expect("stage missing-bucket write");

    let error = db
        .write(batch, WriteOptions::default())
        .expect_err("missing bucket rejects whole batch");
    assert!(matches!(error, Error::BucketMissing { .. }));
    assert_eq!(bucket.get(b"a").expect("no partial write"), None);
}

#[test]
fn duplicate_keys_in_one_batch_use_later_operation() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket().expect("bucket opens");

    let mut put_then_delete = WriteBatch::new();
    put_then_delete.put(b"a", b"v1");
    put_then_delete.delete(b"a");
    db.write(put_then_delete, WriteOptions::default())
        .expect("batch commits");
    assert_eq!(bucket.get(b"a").expect("later delete wins"), None);

    let mut delete_then_put = WriteBatch::new();
    delete_then_put.delete(b"a");
    delete_then_put.put(b"a", b"v2");
    db.write(delete_then_put, WriteOptions::default())
        .expect("batch commits");
    assert_eq!(
        bucket.get(b"a").expect("later put wins"),
        Some(b"v2".to_vec())
    );
}
