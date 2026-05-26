use trine_kv::{
    BucketOptions, Db, DbOptions, Error, KeyRange, TransactionOptions, WriteBatch, WriteOptions,
};

#[test]
fn transaction_commits_staged_writes_without_reads() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");
    let mut txn = db.transaction(TransactionOptions::default());

    txn.put("default", b"a", b"txn");
    let info = txn.commit().expect("transaction commits");

    assert_eq!(info.sequence().get(), 1);
    assert_eq!(
        bucket.get(b"a").expect("committed value"),
        Some(b"txn".to_vec())
    );
}

#[test]
fn transaction_point_read_conflicts_with_later_point_write() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");
    bucket.put(b"a", b"v1").expect("seed value");

    let mut txn = db.transaction(TransactionOptions::default());
    assert_eq!(
        txn.get("default", b"a").expect("txn read"),
        Some(b"v1".to_vec())
    );
    bucket.put(b"a", b"v2").expect("concurrent write");

    let error = txn.commit().expect_err("point read must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_point_read_conflicts_with_later_range_delete() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");
    bucket.put(b"m", b"value").expect("seed value");

    let mut txn = db.transaction(TransactionOptions::default());
    assert_eq!(
        txn.get("default", b"m").expect("txn read"),
        Some(b"value".to_vec())
    );
    bucket
        .delete_range(KeyRange::half_open(b"a", b"z"))
        .expect("concurrent range delete");

    let error = txn.commit().expect_err("range delete must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_range_read_conflicts_with_later_point_write_inside_range() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");
    let mut txn = db.transaction(TransactionOptions::default());

    txn.read_range("default", KeyRange::half_open(b"a", b"m"))
        .expect("track range read");
    bucket.put(b"b", b"new").expect("concurrent write");

    let error = txn.commit().expect_err("range read must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_range_read_conflicts_with_later_overlapping_range_delete() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    db.open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");
    let mut txn = db.transaction(TransactionOptions::default());

    txn.read_range("default", KeyRange::half_open(b"c", b"g"))
        .expect("track range read");
    let mut delete = WriteBatch::new();
    delete.delete_range("default", KeyRange::half_open(b"f", b"z"));
    db.write(delete, WriteOptions::default())
        .expect("concurrent range delete");

    let error = txn
        .commit()
        .expect_err("overlapping range delete must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_range_read_allows_later_write_outside_range() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");
    let mut txn = db.transaction(TransactionOptions::default());

    txn.read_range("default", KeyRange::half_open(b"a", b"m"))
        .expect("track range read");
    bucket.put(b"z", b"outside").expect("outside write");
    txn.put("default", b"b", b"inside");

    txn.commit().expect("outside write does not conflict");
    assert_eq!(
        bucket.get(b"b").expect("txn write visible"),
        Some(b"inside".to_vec())
    );
}
