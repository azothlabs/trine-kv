use trine_kv::{Db, DbOptions, Iter, KeyRange, KeyValue, KeyspaceOptions, WriteBatch};

fn collect(iter: Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|item| {
        let KeyValue { key, value } = item.expect("iterator item is readable");
        (key, value)
    })
    .collect()
}

#[test]
fn range_delete_hides_point_reads_and_scans_without_breaking_snapshots() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");

    for (key, value) in [(b"a", b"a1"), (b"b", b"b1"), (b"c", b"c1"), (b"d", b"d1")] {
        keyspace.insert(key, value).expect("write key");
    }
    let snapshot = db.snapshot();

    let mut delete = WriteBatch::new();
    delete.remove_range("default", KeyRange::half_open(b"b", b"d"));
    db.write(delete, Default::default())
        .expect("range delete commits");

    assert_eq!(
        keyspace.get(b"a").expect("a survives"),
        Some(b"a1".to_vec())
    );
    assert_eq!(keyspace.get(b"b").expect("b hidden"), None);
    assert_eq!(keyspace.get(b"c").expect("c hidden"), None);
    assert_eq!(
        keyspace.get(b"d").expect("d survives"),
        Some(b"d1".to_vec())
    );
    assert_eq!(
        collect(keyspace.range(&KeyRange::all()).expect("current range")),
        vec![
            (b"a".to_vec(), b"a1".to_vec()),
            (b"d".to_vec(), b"d1".to_vec()),
        ]
    );
    assert_eq!(
        collect(
            snapshot
                .range(&keyspace, &KeyRange::all())
                .expect("snapshot range")
        ),
        vec![
            (b"a".to_vec(), b"a1".to_vec()),
            (b"b".to_vec(), b"b1".to_vec()),
            (b"c".to_vec(), b"c1".to_vec()),
            (b"d".to_vec(), b"d1".to_vec()),
        ]
    );
}

#[test]
fn range_delete_participates_in_prefix_scans() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");

    keyspace.insert(b"user:1", b"old").expect("write user 1");
    keyspace.insert(b"user:2", b"old").expect("write user 2");
    keyspace.insert(b"order:1", b"keep").expect("write order");

    let mut delete = WriteBatch::new();
    delete.remove_range("default", KeyRange::half_open(b"user:1", b"user:3"));
    db.write(delete, Default::default())
        .expect("range delete commits");

    assert_eq!(
        collect(keyspace.prefix(b"user:").expect("prefix after delete")),
        Vec::<(Vec<u8>, Vec<u8>)>::new()
    );
    assert_eq!(
        collect(keyspace.prefix(b"order:").expect("other prefix survives")),
        vec![(b"order:1".to_vec(), b"keep".to_vec())]
    );
}

#[test]
fn same_batch_order_decides_range_delete_conflicts() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");

    let mut delete_then_insert = WriteBatch::new();
    delete_then_insert.remove_range("default", KeyRange::half_open(b"a", b"z"));
    delete_then_insert.insert("default", b"m", b"visible");
    db.write(delete_then_insert, Default::default())
        .expect("first batch commits");
    assert_eq!(
        keyspace.get(b"m").expect("later insert survives"),
        Some(b"visible".to_vec())
    );

    let mut insert_then_delete = WriteBatch::new();
    insert_then_delete.insert("default", b"n", b"hidden");
    insert_then_delete.remove_range("default", KeyRange::half_open(b"a", b"z"));
    db.write(insert_then_delete, Default::default())
        .expect("second batch commits");
    assert_eq!(keyspace.get(b"n").expect("later range delete wins"), None);
}
