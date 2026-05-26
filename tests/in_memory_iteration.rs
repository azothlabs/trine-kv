use trine_kv::{BucketOptions, Db, DbOptions, Error, Iter, KeyRange, KeyValue, PrefixExtractor};

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
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");

    bucket.put(b"b", b"b1").expect("write b1");
    bucket.put(b"a", b"a1").expect("write a1");
    bucket.put(b"c", b"c1").expect("write c1");
    let snapshot = db.snapshot();

    bucket.put(b"b", b"b2").expect("write b2");
    bucket.delete(b"c").expect("delete c");
    bucket.put(b"d", b"d1").expect("write d1");

    assert_eq!(
        collect(bucket.range(&KeyRange::all()).expect("current range")),
        vec![
            (b"a".to_vec(), b"a1".to_vec()),
            (b"b".to_vec(), b"b2".to_vec()),
            (b"d".to_vec(), b"d1".to_vec()),
        ]
    );
    assert_eq!(
        collect(
            snapshot
                .range(&bucket, &KeyRange::all())
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
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");

    for key in [b"a", b"b", b"c", b"d", b"e"] {
        bucket.put(key, key).expect("write key");
    }

    let range = KeyRange::half_open(b"b", b"e");
    assert_eq!(
        collect(bucket.range(&range).expect("forward range")),
        vec![
            (b"b".to_vec(), b"b".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
            (b"d".to_vec(), b"d".to_vec()),
        ]
    );
    assert_eq!(
        collect(bucket.range_reverse(&range).expect("reverse range")),
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
    let options = BucketOptions {
        prefix_extractor: PrefixExtractor::Separator(b':'),
        ..BucketOptions::default()
    };
    let bucket = db
        .open_bucket_with_options("default", options)
        .expect("bucket opens");

    bucket.put(b"user:1", b"old").expect("write old");
    bucket.put(b"order:1", b"order").expect("write order");
    let snapshot = db.snapshot();

    bucket.put(b"user:2", b"new").expect("write new");
    bucket.delete(b"user:1").expect("delete old");

    assert_eq!(
        collect(bucket.prefix(b"user:").expect("current prefix")),
        vec![(b"user:2".to_vec(), b"new".to_vec())]
    );
    assert_eq!(
        collect(snapshot.prefix(&bucket, b"user:").expect("snapshot prefix")),
        vec![(b"user:1".to_vec(), b"old".to_vec())]
    );
    assert_eq!(
        collect(bucket.prefix_reverse(b"user:").expect("reverse prefix")),
        vec![(b"user:2".to_vec(), b"new".to_vec())]
    );
}

#[test]
fn reopening_bucket_with_different_options_is_rejected() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    db.open_bucket_with_options("default", BucketOptions::default())
        .expect("first bucket open");
    db.put(b"already-written", b"value")
        .expect("default bucket write fixes options");

    let options = BucketOptions {
        allow_empty_keys: false,
        ..BucketOptions::default()
    };
    let error = db
        .open_bucket_with_options("default", options)
        .expect_err("conflicting options must be explicit");

    assert!(matches!(error, Error::InvalidOptions { .. }));
}
