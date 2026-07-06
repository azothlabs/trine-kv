use super::*;

pub(super) fn bench_random_get() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("random get", OPS, || {
        random_get_checksum(&bucket, ROWS, OPS, 0x1234_5678)
    })
}

pub(super) fn bench_missing_get() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let keys = missing_point_read_keys(OPS);
    measure("missing get", OPS, || {
        sequential_point_batch_checksum(&bucket, &keys)
    })
}

pub(super) fn bench_memory_sequential_point_batch() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let keys = point_read_keys(ROWS, OPS, 0x55aa_1001);
    measure("sequential point batch memory", OPS, || {
        sequential_point_batch_checksum(&bucket, &keys)
    })
}

pub(super) fn bench_memory_batched_point_read() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let keys = point_read_keys(ROWS, OPS, 0x55aa_1001);
    measure("batched point read memory", OPS, || {
        batched_point_read_checksum(&bucket, &keys, POINT_READ_BATCH)
    })
}

pub(super) fn bench_persistent_sequential_point_batch() -> BenchResult {
    let (dir, db, bucket) =
        flushed_persistent_db("sequential-point-batch", ROWS, BucketOptions::default());
    let keys = point_read_keys(ROWS, OPS, 0x55aa_2002);
    let result = measure("sequential point batch persistent", OPS, || {
        sequential_point_batch_checksum(&bucket, &keys)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_persistent_batched_point_read() -> BenchResult {
    let (dir, db, bucket) =
        flushed_persistent_db("batched-point-read", ROWS, BucketOptions::default());
    let keys = point_read_keys(ROWS, OPS, 0x55aa_2002);
    let result = measure("batched point read persistent", OPS, || {
        batched_point_read_checksum(&bucket, &keys, POINT_READ_BATCH)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_persistent_localized_sequential_point_batch() -> BenchResult {
    let (dir, db, bucket) = flushed_persistent_db(
        "localized-sequential-point-batch",
        ROWS,
        BucketOptions::default(),
    );
    let keys = localized_point_read_keys(ROWS, OPS);
    let result = measure("localized sequential point batch persistent", OPS, || {
        sequential_point_batch_checksum(&bucket, &keys)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_persistent_localized_batched_point_read() -> BenchResult {
    let (dir, db, bucket) = flushed_persistent_db(
        "localized-batched-point-read",
        ROWS,
        BucketOptions::default(),
    );
    let keys = localized_point_read_keys(ROWS, OPS);
    let result = measure("localized batched point read persistent", OPS, || {
        batched_point_read_checksum(&bucket, &keys, LOCALIZED_POINT_READ_BATCH)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_persistent_missing_sequential_point_batch() -> BenchResult {
    let (dir, db, bucket) = flushed_persistent_db(
        "missing-sequential-point-batch",
        ROWS,
        BucketOptions::default(),
    );
    let keys = missing_point_read_keys(OPS);
    let result = measure("missing sequential point batch persistent", OPS, || {
        sequential_point_batch_checksum(&bucket, &keys)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_persistent_missing_batched_point_read() -> BenchResult {
    let (dir, db, bucket) =
        flushed_persistent_db("missing-batched-point-read", ROWS, BucketOptions::default());
    let keys = missing_point_read_keys(OPS);
    let result = measure("missing batched point read persistent", OPS, || {
        batched_point_read_checksum(&bucket, &keys, POINT_READ_BATCH)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_persistent_bounded_missing_sequential_point_batch() -> BenchResult {
    let (dir, db, bucket) = flushed_persistent_db(
        "bounded-missing-sequential-point-batch",
        ROWS,
        BucketOptions::default(),
    );
    let keys = bounded_missing_point_read_keys(ROWS, OPS);
    let result = measure(
        "bounded missing sequential point batch persistent",
        OPS,
        || sequential_point_batch_checksum(&bucket, &keys),
    );
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_persistent_bounded_missing_batched_point_read() -> BenchResult {
    let (dir, db, bucket) = flushed_persistent_db(
        "bounded-missing-batched-point-read",
        ROWS,
        BucketOptions::default(),
    );
    let keys = bounded_missing_point_read_keys(ROWS, OPS);
    let result = measure("bounded missing batched point read persistent", OPS, || {
        batched_point_read_checksum(&bucket, &keys, POINT_READ_BATCH)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn extend_localized_point_batch_diagnostics(results: &mut Vec<BenchResult>) {
    let keys = localized_point_read_keys(ROWS, OPS);
    push_localized_point_read_diagnostics(
        results,
        "localized point diagnostic sequential",
        &keys,
        sequential_point_batch_checksum,
    );
    for (batch_size, label) in [
        (4, "localized point diagnostic batch 4"),
        (8, "localized point diagnostic batch 8"),
        (16, "localized point diagnostic batch 16"),
        (32, "localized point diagnostic batch 32"),
    ] {
        push_localized_point_read_diagnostics(results, label, &keys, |bucket, keys| {
            batched_point_read_checksum(bucket, keys, batch_size)
        });
    }
}

pub(super) fn extend_missing_point_batch_diagnostics(results: &mut Vec<BenchResult>) {
    let keys = missing_point_read_keys(OPS);
    push_missing_point_read_diagnostics(
        results,
        "missing point diagnostic sequential",
        &keys,
        sequential_point_batch_checksum,
    );
    for (batch_size, label) in [
        (4, "missing point diagnostic batch 4"),
        (8, "missing point diagnostic batch 8"),
        (16, "missing point diagnostic batch 16"),
        (32, "missing point diagnostic batch 32"),
    ] {
        push_missing_point_read_diagnostics(results, label, &keys, |bucket, keys| {
            batched_point_read_checksum(bucket, keys, batch_size)
        });
    }
}

pub(super) fn extend_bounded_missing_point_batch_diagnostics(results: &mut Vec<BenchResult>) {
    let keys = bounded_missing_point_read_keys(ROWS, OPS);
    push_missing_point_read_diagnostics(
        results,
        "bounded missing point diagnostic sequential",
        &keys,
        sequential_point_batch_checksum,
    );
    for (batch_size, label) in [
        (4, "bounded missing point diagnostic batch 4"),
        (8, "bounded missing point diagnostic batch 8"),
        (16, "bounded missing point diagnostic batch 16"),
        (32, "bounded missing point diagnostic batch 32"),
    ] {
        push_missing_point_read_diagnostics(results, label, &keys, |bucket, keys| {
            batched_point_read_checksum(bucket, keys, batch_size)
        });
    }
}

pub(super) fn push_localized_point_read_diagnostics(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    keys: &[Vec<u8>],
    read: impl FnOnce(&trine_kv::Bucket, &[Vec<u8>]) -> u64,
) {
    let (dir, db, bucket) = flushed_persistent_db(label, ROWS, BucketOptions::default());
    let before = db.stats();
    let start = Instant::now();
    let checksum = read(&bucket, keys);
    assert!(checksum > 0, "localized point diagnostic must read values");
    let elapsed_micros = duration_micros(start.elapsed());
    let after = db.stats();

    let mut diagnostics = ColdReadDiagnostics::default();
    diagnostics.record_delta(&before, &after);
    results.push(BenchResult::diagnostic(
        labelled(label, "wall micros"),
        elapsed_micros,
    ));
    diagnostics.push_results_with_label(results, label);

    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn push_missing_point_read_diagnostics(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    keys: &[Vec<u8>],
    read: impl FnOnce(&trine_kv::Bucket, &[Vec<u8>]) -> u64,
) {
    let (dir, db, bucket) = flushed_persistent_db(label, ROWS, BucketOptions::default());
    let before = db.stats();
    let start = Instant::now();
    let checksum = read(&bucket, keys);
    assert_eq!(checksum, 0, "missing point diagnostic must miss every key");
    let elapsed_micros = duration_micros(start.elapsed());
    let after = db.stats();

    let mut diagnostics = ColdReadDiagnostics::default();
    diagnostics.record_delta(&before, &after);
    results.push(BenchResult::diagnostic(
        labelled(label, "wall micros"),
        elapsed_micros,
    ));
    diagnostics.push_results_with_label(results, label);

    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn bench_active_memtable_random_get() -> BenchResult {
    let (dir, db, bucket) = populated_active_memtable_db("active-memtable-random-get", ROWS);
    let result = measure("active memtable random get", OPS, || {
        random_get_checksum(&bucket, ROWS, OPS, 0x4ac7_1fe5)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_delta_backed_random_get() -> BenchResult {
    let db = populated_delta_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("merged delta random get", OPS, || {
        random_get_checksum(&bucket, ROWS, OPS, 0x4ac7_1fe5)
    })
}

pub(super) fn bench_delta_backed_missing_get() -> BenchResult {
    let db = populated_delta_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let keys = missing_point_read_keys(OPS);
    measure("merged delta missing get", OPS, || {
        sequential_point_batch_checksum(&bucket, &keys)
    })
}

pub(super) fn bench_bounded_range_scan() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("bounded range scan", 128, || {
        range_scan_checksum(&bucket, 128)
    })
}

pub(super) fn bench_active_memtable_range_scan() -> BenchResult {
    let (dir, db, bucket) = populated_active_memtable_db("active-memtable-range-scan", ROWS);
    let result = measure("active memtable range scan", 128, || {
        range_scan_checksum(&bucket, 128)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_delta_backed_range_scan() -> BenchResult {
    let db = populated_delta_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("merged delta range scan", 128, || {
        range_scan_checksum(&bucket, 128)
    })
}

pub(super) fn bench_prefix_scan() -> BenchResult {
    let db = populated_prefix_db(ROWS, false);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("prefix scan", 128, || {
        let mut checksum = 0;
        for tenant in 0..128 {
            let prefix = format!("tenant:{:02}:", tenant % 16);
            let iter = bucket
                .prefix_sync(prefix.as_bytes())
                .expect("prefix succeeds");
            checksum += iter
                .map(|item| item.expect("prefix item").value.len() as u64)
                .sum::<u64>();
        }
        checksum
    })
}

pub(super) fn bench_prefix_partition_scans() -> Vec<BenchResult> {
    let dir = temp_dir("prefix-partition");
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = prefix_options(true);
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(prefix_key(index), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");

    let matching = measure("prefix scan table partitions matching", 128, || {
        let mut checksum = 0;
        for tenant in 0..128 {
            let prefix = format!("tenant:{:02}:", tenant % 16);
            let iter = bucket
                .prefix_sync(prefix.as_bytes())
                .expect("prefix succeeds");
            checksum += iter
                .map(|item| item.expect("prefix item").value.len() as u64)
                .sum::<u64>();
        }
        checksum
    });
    let nonmatching = measure("prefix scan table partitions nonmatching", 128, || {
        let mut checksum = 0;
        for tenant in 0..128 {
            let prefix = format!("missing:{tenant:02}:");
            let iter = bucket
                .prefix_sync(prefix.as_bytes())
                .expect("prefix succeeds");
            checksum += iter.count() as u64;
        }
        checksum
    });
    drop(db);
    cleanup_dir(&dir);
    vec![matching, nonmatching]
}
