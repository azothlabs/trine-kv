use super::{
    BenchResult, BucketOptions, Db, Instant, OPS, ROWS, RuntimeOptions,
    benchmark_persistent_options, cleanup_dir, duration_micros, flushed_persistent_db, key,
    labelled, measure, seed_index, temp_dir, value, xorshift,
};

pub(super) fn bench_block_cache_warm_read() -> BenchResult {
    let (dir, db, bucket) = flushed_persistent_db("warm-read", ROWS, BucketOptions::default());
    bucket
        .get_sync(&key(ROWS / 2))
        .expect("warmup get succeeds");
    let result = measure("block cache warm read", OPS, || {
        let mut checksum = 0;
        for _ in 0..OPS {
            checksum += bucket
                .get_sync(&key(ROWS / 2))
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64);
        }
        checksum
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_block_cache_random_block_read() -> BenchResult {
    let dir = temp_dir("block-cache-random-block-read");
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = BucketOptions {
        block_bytes: 512,
        ..BucketOptions::default()
    };
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");
    for index in 0..ROWS {
        bucket.get_sync(&key(index)).expect("warmup get succeeds");
    }

    let before = db.stats();
    let result = measure("block cache random block read", OPS, || {
        let mut checksum = 0_u64;
        let mut seed = 0xa51c_f00d_u64;
        for _ in 0..OPS {
            seed = xorshift(seed);
            let index = seed_index(seed, ROWS);
            checksum = checksum.saturating_add(
                bucket
                    .get_sync(&key(index))
                    .expect("get succeeds")
                    .map_or(0, |value| value.len() as u64),
            );
        }
        checksum
    });
    let after = db.stats();
    assert!(
        after
            .block_cache_hits
            .saturating_sub(before.block_cache_hits)
            >= OPS as u64,
        "random block read should exercise cached blocks"
    );
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn extend_block_cache_decode_diagnostics(results: &mut Vec<BenchResult>) {
    extend_warm_cache_hit_diagnostic(results);
    extend_random_cache_hit_diagnostic(results);
    extend_forced_block_decode_diagnostic(results);
}

pub(super) fn extend_warm_cache_hit_diagnostic(results: &mut Vec<BenchResult>) {
    let (dir, db, bucket) =
        flushed_persistent_db("warm-cache-hit-diagnostic", ROWS, BucketOptions::default());
    let read_key = key(ROWS / 2);
    bucket
        .get_sync(&read_key)
        .expect("warmup get succeeds")
        .expect("warmup key exists");

    let before = db.stats();
    let started = Instant::now();
    let mut checksum = 0_u64;
    for _ in 0..OPS {
        checksum = checksum.saturating_add(
            bucket
                .get_sync(&read_key)
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64),
        );
    }
    let after = db.stats();
    assert!(checksum > 0, "warm cache diagnostic must read values");
    push_cache_decode_diagnostic_results(
        results,
        "block cache warm hit diagnostic",
        &before,
        &after,
        duration_micros(started.elapsed()),
    );
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn extend_random_cache_hit_diagnostic(results: &mut Vec<BenchResult>) {
    let dir = temp_dir("random-cache-hit-diagnostic");
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = BucketOptions {
        block_bytes: 512,
        ..BucketOptions::default()
    };
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");
    for index in 0..ROWS {
        bucket.get_sync(&key(index)).expect("warmup get succeeds");
    }

    let before = db.stats();
    let started = Instant::now();
    let mut checksum = 0_u64;
    let mut seed = 0xa51c_f00d_u64;
    for _ in 0..OPS {
        seed = xorshift(seed);
        let index = seed_index(seed, ROWS);
        checksum = checksum.saturating_add(
            bucket
                .get_sync(&key(index))
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64),
        );
    }
    let after = db.stats();
    assert!(checksum > 0, "random cache diagnostic must read values");
    push_cache_decode_diagnostic_results(
        results,
        "block cache random hit diagnostic",
        &before,
        &after,
        duration_micros(started.elapsed()),
    );
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn extend_forced_block_decode_diagnostic(results: &mut Vec<BenchResult>) {
    let dir = temp_dir("forced-block-decode-diagnostic");
    let mut options = benchmark_persistent_options(&dir);
    options.runtime = RuntimeOptions::inline();
    options.block_cache_bytes = 0;
    options.background_worker_count = 0;
    options.default_bucket_options = BucketOptions {
        block_bytes: 512,
        ..BucketOptions::default()
    };
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");

    let before = db.stats();
    let started = Instant::now();
    let mut checksum = 0_u64;
    let mut seed = 0xa51c_f00d_u64;
    for _ in 0..OPS {
        seed = xorshift(seed);
        let index = seed_index(seed, ROWS);
        checksum = checksum.saturating_add(
            bucket
                .get_sync(&key(index))
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64),
        );
    }
    let after = db.stats();
    assert!(checksum > 0, "forced decode diagnostic must read values");
    push_cache_decode_diagnostic_results(
        results,
        "block decode forced diagnostic",
        &before,
        &after,
        duration_micros(started.elapsed()),
    );
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn push_cache_decode_diagnostic_results(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    before: &trine_kv::DbStats,
    after: &trine_kv::DbStats,
    wall_micros: u64,
) {
    results.push(BenchResult::diagnostic(
        labelled(label, "wall micros"),
        wall_micros,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "cache hits"),
        after
            .block_cache_hits
            .saturating_sub(before.block_cache_hits),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "cache misses"),
        after
            .block_cache_misses
            .saturating_sub(before.block_cache_misses),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "point block metadata probes"),
        after
            .read_path
            .point_block_metadata_probes
            .saturating_sub(before.read_path.point_block_metadata_probes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "point data block reads"),
        after
            .read_path
            .point_data_block_reads
            .saturating_sub(before.read_path.point_data_block_reads),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "storage read owned requests"),
        after
            .storage_operations
            .read_exact_at_owned
            .requests
            .saturating_sub(before.storage_operations.read_exact_at_owned.requests),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "storage read owned micros"),
        after
            .storage_operations
            .read_exact_at_owned
            .total_latency_micros
            .saturating_sub(
                before
                    .storage_operations
                    .read_exact_at_owned
                    .total_latency_micros,
            ),
    ));
}
