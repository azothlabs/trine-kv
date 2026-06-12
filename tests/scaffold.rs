use trine_kv::{
    BucketOptions, CompressionProfile, Db, DbOptions, Direction, DurabilityMode, KeyRange,
    PrefixExtractor, ReadVersion, StorageMode, WriteBatch,
};

#[test]
fn scaffold_exposes_v1_public_boundaries() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db scaffold opens");
    db.put_sync(b"default-key", b"default-value")
        .expect("default bucket put works");

    assert_eq!(
        db.get_sync(b"default-key")
            .expect("default bucket get works"),
        Some(b"default-value".to_vec())
    );
    assert_eq!(db.snapshot().read_version(), ReadVersion::from_u64(1));
    assert_eq!(db.stats().live_buckets, 1);
    assert_eq!(CompressionProfile::default(), CompressionProfile::Fast);

    let mut batch = WriteBatch::new();
    batch.put(b"a", b"b");
    batch.delete_range(KeyRange::half_open(b"a", b"z"));
    assert_eq!(batch.len(), 2);

    let iter = trine_kv::Bucket::empty_iter(Direction::Forward);
    assert_eq!(iter.direction(), Direction::Forward);
}

#[test]
fn prefix_and_compression_profiles_are_usable() {
    let extractor = PrefixExtractor::Separator(b':');
    assert_eq!(extractor.extract(b"user:42"), Some(&b"user:"[..]));

    let options = BucketOptions {
        compression: CompressionProfile::None,
        ..BucketOptions::default()
    };
    assert_eq!(options.compression, CompressionProfile::None);
    assert_eq!(CompressionProfile::Fast, CompressionProfile::default());
}

#[test]
fn persistent_options_default_to_safe_durability() {
    assert_eq!(
        DbOptions::new("trine-data").durability,
        DurabilityMode::SyncAll
    );
    assert_eq!(
        DbOptions::wasi_persistent("trine-data").durability,
        DurabilityMode::Flush
    );
    assert_eq!(
        DbOptions::browser_persistent().durability,
        DurabilityMode::Flush
    );
    assert_eq!(DbOptions::memory().durability, DurabilityMode::Buffered);
}

#[test]
fn path_open_defaults_to_persistent_storage() {
    let path = std::env::temp_dir().join(format!(
        "trine-kv-path-open-scaffold-{}",
        std::process::id()
    ));
    if path.exists() {
        std::fs::remove_dir_all(&path).expect("old scaffold directory removed");
    }

    let db = Db::open_sync(&path).expect("path open defaults to persistent storage");
    assert!(matches!(
        &db.options().storage_mode,
        StorageMode::Persistent { .. }
    ));
    assert_eq!(db.options().durability, DurabilityMode::SyncAll);
    drop(db);

    std::fs::remove_dir_all(path).expect("scaffold directory removed");
}
