use super::{
    BenchResult, Db, DbOptions, DurabilityMode, OPS, PathBuf, ROWS, WRITE_DIAGNOSTIC_OPS,
    WriteBatch, WriteOptions, cleanup_dir, key, measure, temp_dir, value,
};

pub(super) fn benchmark_persistent_options(path: impl Into<PathBuf>) -> DbOptions {
    DbOptions::new(path).with_durability(DurabilityMode::Buffered)
}

pub(super) fn bench_single_key_put() -> BenchResult {
    measure("single-key put", OPS, || {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        let mut checksum = 0;
        for index in 0..OPS {
            let value = value(index);
            checksum += value.len() as u64;
            bucket.put_sync(key(index), value).expect("put succeeds");
        }
        checksum
    })
}

pub(super) fn bench_batch_write() -> BenchResult {
    measure("batch write", ROWS, || {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        db.default_bucket_sync().expect("bucket opens");
        let mut batch = WriteBatch::new();
        for index in 0..ROWS {
            batch.put(key(index), value(index));
        }
        db.write_sync(batch, WriteOptions::default())
            .expect("batch write succeeds");
        ROWS as u64
    })
}

pub(super) fn bench_persistent_write_path() -> Vec<BenchResult> {
    vec![
        bench_persistent_single_key_put(
            DurabilityMode::Buffered,
            "persistent single-key put buffered",
        ),
        bench_persistent_single_key_put(DurabilityMode::Flush, "persistent single-key put flush"),
        bench_persistent_single_key_put(
            DurabilityMode::SyncData,
            "persistent single-key put sync-data",
        ),
        bench_persistent_single_key_put(
            DurabilityMode::SyncAll,
            "persistent single-key put sync-all",
        ),
        bench_persistent_batch_write(DurabilityMode::Buffered, "persistent batch write buffered"),
        bench_persistent_batch_write(DurabilityMode::Flush, "persistent batch write flush"),
        bench_persistent_batch_write(DurabilityMode::SyncData, "persistent batch write sync-data"),
        bench_persistent_batch_write(DurabilityMode::SyncAll, "persistent batch write sync-all"),
    ]
}

pub(super) fn bench_persistent_single_key_put(
    durability: DurabilityMode,
    name: &'static str,
) -> BenchResult {
    measure(name, WRITE_DIAGNOSTIC_OPS, || {
        let dir = temp_dir(name);
        let db = Db::open_sync(DbOptions::persistent(&dir).with_durability(durability))
            .expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..WRITE_DIAGNOSTIC_OPS {
            bucket
                .put_sync(key(index), value(index))
                .expect("put succeeds");
        }
        let stats = db.stats();
        drop(db);
        cleanup_dir(&dir);
        stats
            .wal_bytes_accepted
            .saturating_add(stats.commit_visible_sequence)
    })
}

pub(super) fn bench_persistent_batch_write(
    durability: DurabilityMode,
    name: &'static str,
) -> BenchResult {
    measure(name, ROWS, || {
        let dir = temp_dir(name);
        let db = Db::open_sync(DbOptions::persistent(&dir).with_durability(durability))
            .expect("persistent db opens");
        db.default_bucket_sync().expect("bucket opens");
        let mut batch = WriteBatch::new();
        for index in 0..ROWS {
            batch.put(key(index), value(index));
        }
        db.write_sync(batch, WriteOptions::default())
            .expect("batch write succeeds");
        let stats = db.stats();
        drop(db);
        cleanup_dir(&dir);
        stats
            .wal_bytes_accepted
            .saturating_add(stats.commit_visible_sequence)
    })
}
