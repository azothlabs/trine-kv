use trine_kv::{
    Db, DbOptions, Error, KeyRange, ReadVersion, TransactionOptions, WriteBatch, WriteOptions,
};

#[test]
fn transaction_commits_staged_writes_without_reads() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let mut txn = db.transaction(TransactionOptions::default());

    txn.put(b"a", b"txn");
    let info = txn.commit_sync().expect("transaction commits");

    assert_eq!(info.read_version(), ReadVersion::from_u64(1));
    assert_eq!(
        bucket.get_sync(b"a").expect("committed value"),
        Some(b"txn".to_vec())
    );
}

#[test]
fn transaction_exposes_read_version_boundary() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    db.put_sync(b"a", b"v1").expect("seed value");

    let txn = db.transaction(TransactionOptions::default());
    assert_eq!(txn.read_version(), db.latest_read_version());
}

#[test]
fn named_transaction_methods_reject_reserved_default_bucket_name() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let mut txn = db.transaction(TransactionOptions::default());

    let error = txn
        .put_bucket("default", b"a", b"b")
        .expect_err("default writes use txn.put");
    assert!(matches!(error, Error::InvalidOptions { .. }));

    let error = txn
        .delete_range_bucket("", KeyRange::all())
        .expect_err("empty named bucket is rejected");
    assert!(matches!(error, Error::InvalidOptions { .. }));
}

#[test]
fn transaction_point_read_conflicts_with_later_point_write() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    bucket.put_sync(b"a", b"v1").expect("seed value");

    let mut txn = db.transaction(TransactionOptions::default());
    assert_eq!(txn.get_sync(b"a").expect("txn read"), Some(b"v1".to_vec()));
    bucket.put_sync(b"a", b"v2").expect("concurrent write");

    let error = txn.commit_sync().expect_err("point read must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_point_read_conflicts_with_later_range_delete() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    bucket.put_sync(b"m", b"value").expect("seed value");

    let mut txn = db.transaction(TransactionOptions::default());
    assert_eq!(
        txn.get_sync(b"m").expect("txn read"),
        Some(b"value".to_vec())
    );
    bucket
        .delete_range_sync(KeyRange::half_open(b"a", b"z"))
        .expect("concurrent range delete");

    let error = txn.commit_sync().expect_err("range delete must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_range_read_conflicts_with_later_point_write_inside_range() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let mut txn = db.transaction(TransactionOptions::default());

    txn.read_range_sync(KeyRange::half_open(b"a", b"m"))
        .expect("track range read");
    bucket.put_sync(b"b", b"new").expect("concurrent write");

    let error = txn.commit_sync().expect_err("range read must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_range_read_conflicts_with_later_overlapping_range_delete() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    db.default_bucket_sync().expect("bucket opens");
    let mut txn = db.transaction(TransactionOptions::default());

    txn.read_range_sync(KeyRange::half_open(b"c", b"g"))
        .expect("track range read");
    let mut delete = WriteBatch::new();
    delete.delete_range(KeyRange::half_open(b"f", b"z"));
    db.write_sync(delete, WriteOptions::default())
        .expect("concurrent range delete");

    let error = txn
        .commit_sync()
        .expect_err("overlapping range delete must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_range_read_allows_later_write_outside_range() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let mut txn = db.transaction(TransactionOptions::default());

    txn.read_range_sync(KeyRange::half_open(b"a", b"m"))
        .expect("track range read");
    bucket.put_sync(b"z", b"outside").expect("outside write");
    txn.put(b"b", b"inside");

    txn.commit_sync().expect("outside write does not conflict");
    assert_eq!(
        bucket.get_sync(b"b").expect("txn write visible"),
        Some(b"inside".to_vec())
    );
}
