use trine_kv::{Db, DbOptions, Error, KeyRange, KeyspaceOptions};

#[test]
fn transaction_commits_staged_writes_without_reads() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");
    let mut txn = db.transaction(Default::default());

    txn.insert("default", b"a", b"txn");
    let info = txn.commit().expect("transaction commits");

    assert_eq!(info.sequence().get(), 1);
    assert_eq!(
        keyspace.get(b"a").expect("committed value"),
        Some(b"txn".to_vec())
    );
}

#[test]
fn transaction_point_read_conflicts_with_later_point_write() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");
    keyspace.insert(b"a", b"v1").expect("seed value");

    let mut txn = db.transaction(Default::default());
    assert_eq!(
        txn.get("default", b"a").expect("txn read"),
        Some(b"v1".to_vec())
    );
    keyspace.insert(b"a", b"v2").expect("concurrent write");

    let error = txn.commit().expect_err("point read must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_point_read_conflicts_with_later_range_delete() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");
    keyspace.insert(b"m", b"value").expect("seed value");

    let mut txn = db.transaction(Default::default());
    assert_eq!(
        txn.get("default", b"m").expect("txn read"),
        Some(b"value".to_vec())
    );
    keyspace
        .remove_range(KeyRange::half_open(b"a", b"z"))
        .expect("concurrent range delete");

    let error = txn.commit().expect_err("range delete must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_range_read_conflicts_with_later_point_write_inside_range() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");
    let mut txn = db.transaction(Default::default());

    txn.read_range("default", KeyRange::half_open(b"a", b"m"))
        .expect("track range read");
    keyspace.insert(b"b", b"new").expect("concurrent write");

    let error = txn.commit().expect_err("range read must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_range_read_conflicts_with_later_overlapping_range_delete() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    db.keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");
    let mut txn = db.transaction(Default::default());

    txn.read_range("default", KeyRange::half_open(b"c", b"g"))
        .expect("track range read");
    let mut delete = trine_kv::WriteBatch::new();
    delete.remove_range("default", KeyRange::half_open(b"f", b"z"));
    db.write(delete, Default::default())
        .expect("concurrent range delete");

    let error = txn
        .commit()
        .expect_err("overlapping range delete must conflict");
    assert!(matches!(error, Error::Conflict { .. }));
}

#[test]
fn transaction_range_read_allows_later_write_outside_range() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");
    let mut txn = db.transaction(Default::default());

    txn.read_range("default", KeyRange::half_open(b"a", b"m"))
        .expect("track range read");
    keyspace.insert(b"z", b"outside").expect("outside write");
    txn.insert("default", b"b", b"inside");

    txn.commit().expect("outside write does not conflict");
    assert_eq!(
        keyspace.get(b"b").expect("txn write visible"),
        Some(b"inside".to_vec())
    );
}
