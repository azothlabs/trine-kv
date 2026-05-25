use trine_kv::{Db, DbOptions, Error, KeyspaceOptions, WriteBatch};

#[test]
fn point_writes_deletes_and_snapshot_reads_are_mvcc_visible() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");

    assert_eq!(keyspace.get(b"a").expect("initial read"), None);

    keyspace.insert(b"a", b"v1").expect("first write");
    let snapshot = db.snapshot();

    keyspace.insert(b"a", b"v2").expect("second write");
    assert_eq!(
        keyspace.get(b"a").expect("current read"),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        snapshot.get(&keyspace, b"a").expect("snapshot read"),
        Some(b"v1".to_vec())
    );

    keyspace.remove(b"a").expect("point delete");
    assert_eq!(keyspace.get(b"a").expect("deleted read"), None);
    assert_eq!(
        snapshot
            .get(&keyspace, b"a")
            .expect("snapshot survives delete"),
        Some(b"v1".to_vec())
    );
}

#[test]
fn write_batch_commits_multiple_keyspaces_at_one_sequence() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let users = db
        .keyspace("users", KeyspaceOptions::default())
        .expect("users keyspace opens");
    let posts = db
        .keyspace("posts", KeyspaceOptions::default())
        .expect("posts keyspace opens");

    let mut batch = WriteBatch::new();
    batch.insert("users", b"1", b"ada");
    batch.insert("posts", b"1", b"hello");

    let info = db.write(batch, Default::default()).expect("batch commits");
    assert_eq!(info.sequence().get(), 1);
    assert_eq!(users.get(b"1").expect("users read"), Some(b"ada".to_vec()));
    assert_eq!(
        posts.get(b"1").expect("posts read"),
        Some(b"hello".to_vec())
    );
}

#[test]
fn failed_batch_does_not_partially_apply() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");

    let mut batch = WriteBatch::new();
    batch.insert("default", b"a", b"visible only if batch commits");
    batch.insert("missing", b"b", b"nope");

    let error = db
        .write(batch, Default::default())
        .expect_err("missing keyspace rejects whole batch");
    assert!(matches!(error, Error::KeyspaceMissing { .. }));
    assert_eq!(keyspace.get(b"a").expect("no partial write"), None);
}

#[test]
fn duplicate_keys_in_one_batch_use_later_operation() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");

    let mut insert_then_delete = WriteBatch::new();
    insert_then_delete.insert("default", b"a", b"v1");
    insert_then_delete.remove("default", b"a");
    db.write(insert_then_delete, Default::default())
        .expect("batch commits");
    assert_eq!(keyspace.get(b"a").expect("later delete wins"), None);

    let mut delete_then_insert = WriteBatch::new();
    delete_then_insert.remove("default", b"a");
    delete_then_insert.insert("default", b"a", b"v2");
    db.write(delete_then_insert, Default::default())
        .expect("batch commits");
    assert_eq!(
        keyspace.get(b"a").expect("later insert wins"),
        Some(b"v2".to_vec())
    );
}
