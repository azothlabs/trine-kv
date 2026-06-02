use trine_kv::{
    BucketOptions, Db, DbOptions, Error, Iter, KeyRange, KeyValue, LazyIter, PrefixExtractor,
};

fn collect(iter: Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|item| {
        let KeyValue { key, value } = item.expect("iterator item is readable");
        (key, value)
    })
    .collect()
}

fn collect_lazy(iter: LazyIter) -> Vec<(Vec<u8>, Vec<u8>, bool)> {
    iter.map(|item| {
        let item = item.expect("iterator item is readable");
        let is_inline = item.value.is_inline();
        let value = item.value.read_sync().expect("lazy value reads");
        (item.key, value, is_inline)
    })
    .collect()
}

#[test]
fn range_iteration_returns_ordered_live_keys() {
    let db = Db::memory_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    bucket.put_sync(b"b", b"b1").expect("write b1");
    bucket.put_sync(b"a", b"a1").expect("write a1");
    bucket.put_sync(b"c", b"c1").expect("write c1");
    let snapshot = db.snapshot();

    bucket.put_sync(b"b", b"b2").expect("write b2");
    bucket.delete_sync(b"c").expect("delete c");
    bucket.put_sync(b"d", b"d1").expect("write d1");

    assert_eq!(
        collect(bucket.range_sync(&KeyRange::all()).expect("current range")),
        vec![
            (b"a".to_vec(), b"a1".to_vec()),
            (b"b".to_vec(), b"b2".to_vec()),
            (b"d".to_vec(), b"d1".to_vec()),
        ]
    );
    assert_eq!(
        collect(
            snapshot
                .range_sync(&bucket, &KeyRange::all())
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
    let db = Db::memory_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    for key in [b"a", b"b", b"c", b"d", b"e"] {
        bucket.put_sync(key, key).expect("write key");
    }

    let range = KeyRange::half_open(b"b", b"e");
    assert_eq!(
        collect(bucket.range_sync(&range).expect("forward range")),
        vec![
            (b"b".to_vec(), b"b".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
            (b"d".to_vec(), b"d".to_vec()),
        ]
    );
    assert_eq!(
        collect(bucket.range_reverse_sync(&range).expect("reverse range")),
        vec![
            (b"d".to_vec(), b"d".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
            (b"b".to_vec(), b"b".to_vec()),
        ]
    );
}

#[test]
fn prefix_iteration_uses_snapshot_visibility() {
    let options = BucketOptions {
        prefix_extractor: PrefixExtractor::Separator(b':'),
        ..BucketOptions::default()
    };
    let mut db_options = DbOptions::memory();
    db_options.default_bucket_options = options;
    let db = Db::memory_sync(db_options).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    bucket.put_sync(b"user:1", b"old").expect("write old");
    bucket.put_sync(b"order:1", b"order").expect("write order");
    let snapshot = db.snapshot();

    bucket.put_sync(b"user:2", b"new").expect("write new");
    bucket.delete_sync(b"user:1").expect("delete old");

    assert_eq!(
        collect(bucket.prefix_sync(b"user:").expect("current prefix")),
        vec![(b"user:2".to_vec(), b"new".to_vec())]
    );
    assert_eq!(
        collect(
            snapshot
                .prefix_sync(&bucket, b"user:")
                .expect("snapshot prefix")
        ),
        vec![(b"user:1".to_vec(), b"old".to_vec())]
    );
    assert_eq!(
        collect(
            bucket
                .prefix_reverse_sync(b"user:")
                .expect("reverse prefix")
        ),
        vec![(b"user:2".to_vec(), b"new".to_vec())]
    );
}

#[test]
fn value_lazy_iteration_works_in_memory_without_blob_files() {
    let db = Db::memory_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    bucket.put_sync(b"a", b"a1").expect("write a1");
    bucket.put_sync(b"b", b"b1").expect("write b1");
    let snapshot = db.snapshot();
    bucket.put_sync(b"b", b"b2").expect("write b2");

    assert_eq!(
        collect_lazy(
            bucket
                .range_lazy_sync(&KeyRange::all())
                .expect("current lazy range")
        ),
        vec![
            (b"a".to_vec(), b"a1".to_vec(), true),
            (b"b".to_vec(), b"b2".to_vec(), true),
        ]
    );
    assert_eq!(
        collect_lazy(
            snapshot
                .range_lazy_sync(&bucket, &KeyRange::all())
                .expect("snapshot lazy range")
        ),
        vec![
            (b"a".to_vec(), b"a1".to_vec(), true),
            (b"b".to_vec(), b"b1".to_vec(), true),
        ]
    );
}

#[test]
fn opening_default_bucket_as_named_bucket_is_rejected() {
    let db = Db::memory_sync(DbOptions::memory()).expect("memory db opens");
    db.put_sync(b"already-written", b"value")
        .expect("default bucket write fixes options");

    let error = db
        .bucket_sync("default")
        .expect_err("default is not a named bucket");
    assert!(matches!(error, Error::InvalidOptions { .. }));

    let options = BucketOptions {
        allow_empty_keys: false,
        ..BucketOptions::default()
    };
    let error = db
        .bucket_with_options_sync("default", options)
        .expect_err("default is not a named bucket");

    assert!(matches!(error, Error::InvalidOptions { .. }));
}
