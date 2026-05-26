use trine_kv::{Db, DbOptions, Error, Iter, KeyRange, KeyValue, KeyspaceOptions, PrefixExtractor};

fn collect(iter: Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|item| {
        let KeyValue { key, value } = item.expect("iterator item is readable");
        (key, value)
    })
    .collect()
}

#[test]
fn range_iteration_returns_ordered_live_keys() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");

    keyspace.insert(b"b", b"b1").expect("write b1");
    keyspace.insert(b"a", b"a1").expect("write a1");
    keyspace.insert(b"c", b"c1").expect("write c1");
    let snapshot = db.snapshot();

    keyspace.insert(b"b", b"b2").expect("write b2");
    keyspace.remove(b"c").expect("delete c");
    keyspace.insert(b"d", b"d1").expect("write d1");

    assert_eq!(
        collect(keyspace.range(&KeyRange::all()).expect("current range")),
        vec![
            (b"a".to_vec(), b"a1".to_vec()),
            (b"b".to_vec(), b"b2".to_vec()),
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
        ]
    );
}

#[test]
fn bounded_range_and_reverse_iteration_obey_key_order() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace opens");

    for key in [b"a", b"b", b"c", b"d", b"e"] {
        keyspace.insert(key, key).expect("write key");
    }

    let range = KeyRange::half_open(b"b", b"e");
    assert_eq!(
        collect(keyspace.range(&range).expect("forward range")),
        vec![
            (b"b".to_vec(), b"b".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
            (b"d".to_vec(), b"d".to_vec()),
        ]
    );
    assert_eq!(
        collect(keyspace.range_reverse(&range).expect("reverse range")),
        vec![
            (b"d".to_vec(), b"d".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
            (b"b".to_vec(), b"b".to_vec()),
        ]
    );
}

#[test]
fn prefix_iteration_uses_snapshot_visibility() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let options = KeyspaceOptions {
        prefix_extractor: PrefixExtractor::Separator(b':'),
        ..KeyspaceOptions::default()
    };
    let keyspace = db.keyspace("default", options).expect("keyspace opens");

    keyspace.insert(b"user:1", b"old").expect("write old");
    keyspace.insert(b"order:1", b"order").expect("write order");
    let snapshot = db.snapshot();

    keyspace.insert(b"user:2", b"new").expect("write new");
    keyspace.remove(b"user:1").expect("delete old");

    assert_eq!(
        collect(keyspace.prefix(b"user:").expect("current prefix")),
        vec![(b"user:2".to_vec(), b"new".to_vec())]
    );
    assert_eq!(
        collect(
            snapshot
                .prefix(&keyspace, b"user:")
                .expect("snapshot prefix")
        ),
        vec![(b"user:1".to_vec(), b"old".to_vec())]
    );
    assert_eq!(
        collect(keyspace.prefix_reverse(b"user:").expect("reverse prefix")),
        vec![(b"user:2".to_vec(), b"new".to_vec())]
    );
}

#[test]
fn reopening_keyspace_with_different_options_is_rejected() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    db.keyspace("default", KeyspaceOptions::default())
        .expect("first keyspace open");

    let options = KeyspaceOptions {
        allow_empty_keys: false,
        ..KeyspaceOptions::default()
    };
    let error = db
        .keyspace("default", options)
        .expect_err("conflicting options must be explicit");

    assert!(matches!(error, Error::InvalidOptions { .. }));
}
