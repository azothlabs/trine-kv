use super::*;

pub(super) fn bench_flush_throughput() -> BenchResult {
    measure("flush throughput", ROWS, || {
        let dir = temp_dir("flush");
        let db = Db::open_sync(benchmark_persistent_options(&dir)).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..ROWS {
            bucket
                .put_sync(key(index), value(index))
                .expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
        let stats = db.stats();
        drop(db);
        cleanup_dir(&dir);
        stats.table_bytes
    })
}

pub(super) fn bench_compaction_throughput() -> BenchResult {
    measure("compaction throughput", ROWS, || {
        let dir = temp_dir("compact");
        let db = Db::open_sync(benchmark_persistent_options(&dir)).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for chunk in 0..4 {
            for index in 0..(ROWS / 4) {
                let row = chunk * (ROWS / 4) + index;
                bucket.put_sync(key(row), value(row)).expect("put succeeds");
            }
            db.flush_sync().expect("flush succeeds");
        }
        db.compact_range_sync(KeyRange::all())
            .expect("compaction succeeds");
        let stats = db.stats();
        drop(db);
        cleanup_dir(&dir);
        stats.compaction_output_bytes
    })
}

pub(super) fn bench_large_inline_values() -> BenchResult {
    measure("large inline values", 256, || {
        let db = Db::open_sync(
            DbOptions::memory().with_default_bucket_options(BucketOptions {
                blob_threshold_bytes: 128 * 1024,
                ..BucketOptions::default()
            }),
        )
        .expect("memory db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        let value = vec![b'x'; 16 * 1024];
        for index in 0..256 {
            bucket
                .put_sync(key(index), value.clone())
                .expect("put succeeds");
        }
        256 * value.len() as u64
    })
}

pub(super) fn bench_separated_blob_values() -> BenchResult {
    measure("separated blob values", 256, || {
        let dir = temp_dir("blob");
        let db = Db::open_sync(
            benchmark_persistent_options(&dir).with_default_bucket_options(BucketOptions {
                blob_threshold_bytes: 4 * 1024,
                ..BucketOptions::default()
            }),
        )
        .expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        let value = vec![b'x'; 16 * 1024];
        for index in 0..256 {
            bucket
                .put_sync(key(index), value.clone())
                .expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
        let stats = db.stats();
        drop(db);
        cleanup_dir(&dir);
        stats.live_blob_bytes
    })
}

pub(super) fn bench_blob_point_read() -> BenchResult {
    let (dir, db, bucket) = large_blob_db("blob-point-read", LARGE_ROWS);
    let result = measure("blob point read", LARGE_OPS, || {
        let mut checksum = 0;
        let mut seed = 0x6b1d_f00d_u64;
        for _ in 0..LARGE_OPS {
            seed = xorshift(seed);
            let index = seed_index(seed, LARGE_ROWS);
            checksum += bucket
                .get_sync(&key(index))
                .expect("blob point get succeeds")
                .map_or(0, |value| value.len() as u64);
        }
        checksum
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_blob_range_scan() -> BenchResult {
    let (dir, db, bucket) = large_blob_db("blob-range-scan", LARGE_ROWS);
    let result = measure("blob range scan", 32, || {
        let mut checksum = 0;
        for start in 0..32 {
            let first = (start * 3) % (LARGE_ROWS - 8);
            let iter = bucket
                .range_sync(&KeyRange::half_open(key(first), key(first + 8)))
                .expect("blob range succeeds");
            checksum += iter
                .map(|item| item.expect("blob range item").value.len() as u64)
                .sum::<u64>();
        }
        checksum
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_blob_range_lazy_keys() -> BenchResult {
    let (dir, db, bucket) = large_blob_db("blob-range-lazy-keys", LARGE_ROWS);
    let result = measure("blob range lazy keys", 32, || {
        let mut checksum = 0;
        for start in 0..32 {
            let first = (start * 3) % (LARGE_ROWS - 8);
            let iter = bucket
                .range_lazy_sync(&KeyRange::half_open(key(first), key(first + 8)))
                .expect("blob lazy range succeeds");
            checksum += iter
                .map(|item| item.expect("blob lazy range item").key.len() as u64)
                .sum::<u64>();
        }
        checksum
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_blob_gc_rewrite() -> BenchResult {
    measure("blob GC rewrite", LARGE_ROWS, || {
        let dir = temp_dir("blob-gc");
        let mut options = benchmark_persistent_options(&dir);
        options.blob_gc_min_file_bytes = 1;
        options.blob_gc_discardable_ratio = BlobGcRatio::from_millionths(300_000);
        options.default_bucket_options = BucketOptions {
            blob_level_merge_policy: BlobLevelMergePolicy::Disabled,
            ..large_blob_options()
        };
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for index in 0..LARGE_ROWS {
            bucket
                .put_sync(key(index), large_value(index))
                .expect("initial large put succeeds");
        }
        db.flush_sync().expect("initial blob flush succeeds");
        for index in (0..LARGE_ROWS).step_by(2) {
            bucket
                .put_sync(key(index), large_value(index + LARGE_ROWS))
                .expect("overwrite large put succeeds");
        }
        db.flush_sync().expect("overwrite blob flush succeeds");
        db.compact_range_sync(KeyRange::all())
            .expect("blob GC compaction succeeds");

        let stats = db.stats();
        let checksum = stats
            .blob_gc_input_bytes
            .saturating_add(stats.blob_gc_output_bytes)
            .saturating_add(stats.blob_gc_discarded_bytes);
        drop(db);
        cleanup_dir(&dir);
        checksum
    })
}

pub(super) fn bench_blob_level_merge() -> BenchResult {
    measure("blob level merge", LARGE_ROWS, || {
        let dir = temp_dir("blob-level-merge");
        let mut options = benchmark_persistent_options(&dir);
        options.blob_gc_enabled = false;
        options.default_bucket_options = BucketOptions {
            blob_level_merge_policy: BlobLevelMergePolicy::Always,
            ..large_blob_options()
        };
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for index in 0..LARGE_ROWS {
            bucket
                .put_sync(key(index), large_value(index))
                .expect("initial large put succeeds");
        }
        db.flush_sync().expect("initial blob flush succeeds");
        for index in (0..LARGE_ROWS).step_by(2) {
            bucket
                .put_sync(key(index), large_value(index + LARGE_ROWS))
                .expect("overwrite large put succeeds");
        }
        db.flush_sync().expect("overwrite blob flush succeeds");
        db.compact_range_sync(KeyRange::all())
            .expect("level merge compaction succeeds");

        let checksum = db.stats().live_blob_bytes;
        drop(db);
        cleanup_dir(&dir);
        checksum
    })
}
