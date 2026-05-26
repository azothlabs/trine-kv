use trine_kv::{BucketOptions, Db, DbOptions, Iter, KeyRange, KeyValue, WriteBatch, WriteOptions};

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
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");

    for (key, value) in [(b"a", b"a1"), (b"b", b"b1"), (b"c", b"c1"), (b"d", b"d1")] {
        bucket.put(key, value).expect("write key");
    }
    let snapshot = db.snapshot();

    let mut delete = WriteBatch::new();
    delete.delete_range("default", KeyRange::half_open(b"b", b"d"));
    db.write(delete, WriteOptions::default())
        .expect("range delete commits");

    assert_eq!(bucket.get(b"a").expect("a survives"), Some(b"a1".to_vec()));
    assert_eq!(bucket.get(b"b").expect("b hidden"), None);
    assert_eq!(bucket.get(b"c").expect("c hidden"), None);
    assert_eq!(bucket.get(b"d").expect("d survives"), Some(b"d1".to_vec()));
    assert_eq!(
        collect(bucket.range(&KeyRange::all()).expect("current range")),
        vec![
            (b"a".to_vec(), b"a1".to_vec()),
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
            (b"d".to_vec(), b"d1".to_vec()),
        ]
    );
}

#[test]
fn range_delete_participates_in_prefix_scans() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");

    bucket.put(b"user:1", b"old").expect("write user 1");
    bucket.put(b"user:2", b"old").expect("write user 2");
    bucket.put(b"order:1", b"keep").expect("write order");

    let mut delete = WriteBatch::new();
    delete.delete_range("default", KeyRange::half_open(b"user:1", b"user:3"));
    db.write(delete, WriteOptions::default())
        .expect("range delete commits");

    assert_eq!(
        collect(bucket.prefix(b"user:").expect("prefix after delete")),
        Vec::<(Vec<u8>, Vec<u8>)>::new()
    );
    assert_eq!(
        collect(bucket.prefix(b"order:").expect("other prefix survives")),
        vec![(b"order:1".to_vec(), b"keep".to_vec())]
    );
}

#[test]
fn same_batch_order_decides_range_delete_conflicts() {
    let db = Db::memory(DbOptions::memory()).expect("memory db opens");
    let bucket = db
        .open_bucket_with_options("default", BucketOptions::default())
        .expect("bucket opens");

    let mut delete_then_put = WriteBatch::new();
    delete_then_put.delete_range("default", KeyRange::half_open(b"a", b"z"));
    delete_then_put.put("default", b"m", b"visible");
    db.write(delete_then_put, WriteOptions::default())
        .expect("first batch commits");
    assert_eq!(
        bucket.get(b"m").expect("later put survives"),
        Some(b"visible".to_vec())
    );

    let mut put_then_delete = WriteBatch::new();
    put_then_delete.put("default", b"n", b"hidden");
    put_then_delete.delete_range("default", KeyRange::half_open(b"a", b"z"));
    db.write(put_then_delete, WriteOptions::default())
        .expect("second batch commits");
    assert_eq!(bucket.get(b"n").expect("later range delete wins"), None);
}
