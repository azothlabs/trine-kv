use std::{
    fs,
    hint::black_box,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use trine_kv::{
    BlobGcRatio, BlobLevelMergePolicy, BucketOptions, Db, DbOptions, DurabilityMode, FilterPolicy,
    IndexSearchPolicy, KeyRange, PrefixExtractor, PrefixFilterPolicy, RuntimeOptions,
    TransactionOptions, WriteBatch, WriteOptions, search,
};

const ROWS: usize = 1_024;
const OPS: usize = 2_048;
const POINT_READ_BATCH: usize = 4;
const LOCALIZED_POINT_READ_BATCH: usize = 16;
const LARGE_ROWS: usize = 128;
const LARGE_OPS: usize = 256;
const LARGE_VALUE_BYTES: usize = 16 * 1024;
const WAL_REPLAY_DIAGNOSTIC_RUNS: usize = 32;

fn main() {
    println!("trine-kv v1 benchmark");
    println!("rows={ROWS} ops={OPS}");
    println!("name,iterations,elapsed_us,units_per_sec,checksum");

    let mut results = vec![
        bench_single_key_put(),
        bench_batch_write(),
        bench_random_get(),
        bench_missing_get(),
        bench_memory_sequential_point_batch(),
        bench_memory_batched_point_read(),
        bench_persistent_sequential_point_batch(),
        bench_persistent_batched_point_read(),
        bench_persistent_localized_sequential_point_batch(),
        bench_persistent_localized_batched_point_read(),
        bench_active_memtable_random_get(),
        bench_delta_backed_random_get(),
        bench_delta_backed_missing_get(),
        bench_bounded_range_scan(),
        bench_active_memtable_range_scan(),
        bench_delta_backed_range_scan(),
        bench_prefix_scan(),
    ];
    results.extend(bench_prefix_partition_scans());
    extend_localized_point_batch_diagnostics(&mut results);
    results.push(bench_snapshot_read_under_writes());
    results.push(bench_transaction_commit());
    results.push(bench_transaction_conflict());
    results.push(bench_wal_replay());
    results.push(bench_wal_replay_read_only());
    extend_wal_replay_diagnostics(&mut results);
    results.push(bench_flush_throughput());
    results.push(bench_compaction_throughput());
    results.push(bench_large_inline_values());
    results.push(bench_separated_blob_values());
    results.push(bench_blob_point_read());
    results.push(bench_blob_range_scan());
    results.push(bench_blob_range_lazy_keys());
    results.push(bench_blob_gc_rewrite());
    results.push(bench_blob_level_merge());
    results.push(bench_block_cache_warm_read());
    results.push(bench_cold_table_read());
    results.push(bench_cold_table_read_only());
    results.extend(bench_read_pruning_diagnostics());
    results.extend(bench_runtime_block_decode_reads());
    results.extend(bench_index_seek_policies());
    results.push(bench_long_shared_prefix_get());
    results.extend(bench_iterator_advance_to());
    results.extend(bench_codec_comparison());

    for result in results {
        println!(
            "{},{},{},{},{}",
            result.name,
            result.iterations,
            result.elapsed.as_micros(),
            result.units_per_second(),
            result.checksum
        );
    }
}

struct BenchResult {
    name: &'static str,
    iterations: usize,
    elapsed: Duration,
    checksum: u64,
}

impl BenchResult {
    const fn diagnostic(name: &'static str, value: u64) -> Self {
        Self {
            name,
            iterations: 1,
            elapsed: Duration::ZERO,
            checksum: value,
        }
    }

    fn units_per_second(&self) -> u64 {
        let nanos = self.elapsed.as_nanos();
        if nanos == 0 {
            return 0;
        }
        let units = (self.iterations as u128).saturating_mul(1_000_000_000);
        u64::try_from(units / nanos).unwrap_or(u64::MAX)
    }
}

fn measure(name: &'static str, iterations: usize, mut run: impl FnMut() -> u64) -> BenchResult {
    let start = Instant::now();
    let checksum = run();
    BenchResult {
        name,
        iterations,
        elapsed: start.elapsed(),
        checksum,
    }
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn benchmark_persistent_options(path: impl Into<PathBuf>) -> DbOptions {
    DbOptions::new(path).with_durability(DurabilityMode::Buffered)
}

fn bench_single_key_put() -> BenchResult {
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

fn bench_batch_write() -> BenchResult {
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

fn bench_random_get() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("random get", OPS, || {
        random_get_checksum(&bucket, ROWS, OPS, 0x1234_5678)
    })
}

fn bench_missing_get() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("missing get", OPS, || missing_get_checksum(&bucket, OPS))
}

fn bench_memory_sequential_point_batch() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let keys = point_read_keys(ROWS, OPS, 0x55aa_1001);
    measure("sequential point batch memory", OPS, || {
        sequential_point_batch_checksum(&bucket, &keys)
    })
}

fn bench_memory_batched_point_read() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let keys = point_read_keys(ROWS, OPS, 0x55aa_1001);
    measure("batched point read memory", OPS, || {
        batched_point_read_checksum(&bucket, &keys, POINT_READ_BATCH)
    })
}

fn bench_persistent_sequential_point_batch() -> BenchResult {
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

fn bench_persistent_batched_point_read() -> BenchResult {
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

fn bench_persistent_localized_sequential_point_batch() -> BenchResult {
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

fn bench_persistent_localized_batched_point_read() -> BenchResult {
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

fn extend_localized_point_batch_diagnostics(results: &mut Vec<BenchResult>) {
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

fn push_localized_point_read_diagnostics(
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

fn bench_active_memtable_random_get() -> BenchResult {
    let (dir, db, bucket) = populated_active_memtable_db("active-memtable-random-get", ROWS);
    let result = measure("active memtable random get", OPS, || {
        random_get_checksum(&bucket, ROWS, OPS, 0x4ac7_1fe5)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

fn bench_delta_backed_random_get() -> BenchResult {
    let db = populated_delta_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("merged delta random get", OPS, || {
        random_get_checksum(&bucket, ROWS, OPS, 0x4ac7_1fe5)
    })
}

fn bench_delta_backed_missing_get() -> BenchResult {
    let db = populated_delta_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("merged delta missing get", OPS, || {
        missing_get_checksum(&bucket, OPS)
    })
}

fn bench_bounded_range_scan() -> BenchResult {
    let db = populated_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("bounded range scan", 128, || {
        range_scan_checksum(&bucket, 128)
    })
}

fn bench_active_memtable_range_scan() -> BenchResult {
    let (dir, db, bucket) = populated_active_memtable_db("active-memtable-range-scan", ROWS);
    let result = measure("active memtable range scan", 128, || {
        range_scan_checksum(&bucket, 128)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

fn bench_delta_backed_range_scan() -> BenchResult {
    let db = populated_delta_memory_db(ROWS);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    measure("merged delta range scan", 128, || {
        range_scan_checksum(&bucket, 128)
    })
}

fn bench_prefix_scan() -> BenchResult {
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

fn bench_prefix_partition_scans() -> Vec<BenchResult> {
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

fn bench_snapshot_read_under_writes() -> BenchResult {
    measure("snapshot read under concurrent writes", OPS, || {
        let db = populated_memory_db(ROWS);
        let bucket = db.default_bucket_sync().expect("bucket opens");
        let snapshot = db.snapshot();
        let mut checksum = 0;
        for index in 0..OPS {
            bucket
                .put_sync(key(index % ROWS), value(index + ROWS))
                .expect("write succeeds");
            checksum += snapshot
                .get_sync(&bucket, &key(index % ROWS))
                .expect("snapshot get succeeds")
                .map_or(0, |value| value.len() as u64);
        }
        checksum
    })
}

fn bench_transaction_commit() -> BenchResult {
    measure("optimistic transaction commit", 512, || {
        let db = populated_memory_db(ROWS);
        let mut checksum = 0;
        for index in 0..512 {
            let mut txn = db.transaction(TransactionOptions::default());
            checksum += txn
                .get_sync(&key(index))
                .expect("txn get succeeds")
                .map_or(0, |value| value.len() as u64);
            txn.put(key(index + ROWS), value(index));
            txn.commit_sync().expect("txn commit succeeds");
        }
        checksum
    })
}

fn bench_transaction_conflict() -> BenchResult {
    measure("optimistic transaction conflict", 512, || {
        let db = populated_memory_db(ROWS);
        let bucket = db.default_bucket_sync().expect("bucket opens");
        let mut conflicts = 0;
        for index in 0..512 {
            let mut txn = db.transaction(TransactionOptions::default());
            txn.get_sync(&key(index)).expect("txn get succeeds");
            bucket
                .put_sync(key(index), value(index + ROWS))
                .expect("conflicting write succeeds");
            txn.put(key(index), value(index));
            if txn.commit_sync().is_err() {
                conflicts += 1;
            }
        }
        conflicts
    })
}

fn bench_wal_replay() -> BenchResult {
    measure("WAL replay", ROWS, || {
        let dir = temp_dir("wal-replay");
        let options = benchmark_persistent_options(&dir);
        populate_wal_replay_dir(options.clone());
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let checksum = bucket
            .get_sync(&key(ROWS / 2))
            .expect("get succeeds")
            .map_or(0, |value| value.len() as u64);
        drop(db);
        cleanup_dir(&dir);
        checksum
    })
}

fn bench_wal_replay_read_only() -> BenchResult {
    measure("WAL replay read-only", ROWS, || {
        let dir = temp_dir("wal-replay-read-only");
        let options = benchmark_persistent_options(&dir);
        populate_wal_replay_dir(options.clone());
        let db = Db::open_sync(options.read_only()).expect("read-only persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let checksum = bucket
            .get_sync(&key(ROWS / 2))
            .expect("get succeeds")
            .map_or(0, |value| value.len() as u64);
        drop(db);
        cleanup_dir(&dir);
        checksum
    })
}

fn extend_wal_replay_diagnostics(results: &mut Vec<BenchResult>) {
    extend_wal_replay_open_diagnostics(results, "WAL replay writable open", false);
    extend_wal_replay_open_diagnostics(results, "WAL replay read-only open", true);
}

fn extend_wal_replay_open_diagnostics(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    read_only: bool,
) {
    let mut open_diagnostics = ColdReadDiagnostics::default();
    let mut first_read_diagnostics = ColdReadDiagnostics::default();
    let mut open_wall_micros = 0_u64;
    let mut open_memtable_bytes = 0_u64;
    let mut open_visible_sequence = 0_u64;
    let mut open_wal_shards = 0_u64;
    let mut open_wal_open_shards = 0_u64;

    for _ in 0..WAL_REPLAY_DIAGNOSTIC_RUNS {
        let dir = temp_dir("wal-replay-diagnostics");
        let options = benchmark_persistent_options(&dir);
        populate_wal_replay_dir(options.clone());
        let open_options = if read_only {
            options.read_only()
        } else {
            options
        };

        let start = Instant::now();
        let db = Db::open_sync(open_options).expect("persistent db reopens");
        open_wall_micros = open_wall_micros.saturating_add(duration_micros(start.elapsed()));

        let open_stats = db.stats();
        open_diagnostics.record(&open_stats);
        open_memtable_bytes = open_memtable_bytes.saturating_add(open_stats.memtable_bytes);
        open_visible_sequence =
            open_visible_sequence.saturating_add(open_stats.commit_visible_sequence);
        open_wal_shards = open_wal_shards.saturating_add(open_stats.wal_shards as u64);
        open_wal_open_shards =
            open_wal_open_shards.saturating_add(open_stats.wal_open_shards as u64);

        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let value_len = bucket
            .get_sync(&key(ROWS / 2))
            .expect("get succeeds")
            .map_or(0, |value| value.len());
        assert!(value_len > 0, "WAL replay diagnostic must read a value");

        let after_first_read = db.stats();
        first_read_diagnostics.record_delta(&open_stats, &after_first_read);
        drop(db);
        cleanup_dir(&dir);
    }

    results.push(BenchResult::diagnostic(
        labelled(label, "wall micros"),
        open_wall_micros,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "memtable bytes"),
        open_memtable_bytes,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "visible sequence"),
        open_visible_sequence,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "configured shards"),
        open_wal_shards,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "active shards"),
        open_wal_open_shards,
    ));
    open_diagnostics.push_results_with_label(results, label);
    first_read_diagnostics.push_results_with_label(results, labelled(label, "first read"));
}

fn populate_wal_replay_dir(options: DbOptions) {
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
}

fn bench_flush_throughput() -> BenchResult {
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

fn bench_compaction_throughput() -> BenchResult {
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

fn bench_large_inline_values() -> BenchResult {
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

fn bench_separated_blob_values() -> BenchResult {
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

fn bench_blob_point_read() -> BenchResult {
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

fn bench_blob_range_scan() -> BenchResult {
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

fn bench_blob_range_lazy_keys() -> BenchResult {
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

fn bench_blob_gc_rewrite() -> BenchResult {
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

fn bench_blob_level_merge() -> BenchResult {
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

fn bench_block_cache_warm_read() -> BenchResult {
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

fn bench_cold_table_read() -> BenchResult {
    measure("cold table read", 32, || {
        let dir = temp_dir("cold-read");
        let options = benchmark_persistent_options(&dir);
        {
            let db = Db::open_sync(options.clone()).expect("persistent db opens");
            let bucket = db.default_bucket_sync().expect("bucket opens");
            for index in 0..ROWS {
                bucket
                    .put_sync(key(index), value(index))
                    .expect("put succeeds");
            }
            db.flush_sync().expect("flush succeeds");
        }

        let mut checksum = 0;
        for _ in 0..32 {
            let db = Db::open_sync(options.clone()).expect("persistent db reopens");
            let bucket = db.default_bucket_sync().expect("bucket reopens");
            checksum += bucket
                .get_sync(&key(ROWS / 2))
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64);
        }
        cleanup_dir(&dir);
        checksum
    })
}

fn bench_cold_table_read_only() -> BenchResult {
    measure("cold table read-only", 32, || {
        let dir = temp_dir("cold-read-only");
        let options = benchmark_persistent_options(&dir);
        {
            let db = Db::open_sync(options.clone()).expect("persistent db opens");
            let bucket = db.default_bucket_sync().expect("bucket opens");
            for index in 0..ROWS {
                bucket
                    .put_sync(key(index), value(index))
                    .expect("put succeeds");
            }
            db.flush_sync().expect("flush succeeds");
        }

        let read_only_options = options.read_only();
        let mut checksum = 0;
        for _ in 0..32 {
            let db =
                Db::open_sync(read_only_options.clone()).expect("read-only persistent db reopens");
            let bucket = db.default_bucket_sync().expect("bucket reopens");
            checksum += bucket
                .get_sync(&key(ROWS / 2))
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64);
        }
        cleanup_dir(&dir);
        checksum
    })
}

fn bench_read_pruning_diagnostics() -> Vec<BenchResult> {
    let mut results = Vec::new();
    extend_cold_table_read_diagnostics(&mut results, false);
    extend_cold_table_read_diagnostics(&mut results, true);
    extend_prefix_partition_diagnostics(&mut results);
    results
}

fn extend_cold_table_read_diagnostics(results: &mut Vec<BenchResult>, read_only: bool) {
    let dir = if read_only {
        temp_dir("read-pruning-cold-read-only")
    } else {
        temp_dir("read-pruning-cold-read")
    };
    let options = benchmark_persistent_options(&dir);
    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..ROWS {
            bucket
                .put_sync(key(index), value(index))
                .expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
    }

    let open_options = if read_only {
        options.read_only()
    } else {
        options
    };
    let mut diagnostics = ColdReadDiagnostics::default();
    let mut open_diagnostics = ColdReadDiagnostics::default();
    let mut first_read_diagnostics = ColdReadDiagnostics::default();
    for _ in 0..32 {
        let db = Db::open_sync(open_options.clone()).expect("persistent db reopens");
        let open_stats = db.stats();
        open_diagnostics.record(&open_stats);

        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let value_len = bucket
            .get_sync(&key(ROWS / 2))
            .expect("get succeeds")
            .map_or(0, |value| value.len());
        assert!(value_len > 0, "cold-read diagnostic must read a value");
        let stats = db.stats();
        diagnostics.record(&stats);
        first_read_diagnostics.record_delta(&open_stats, &stats);
    }
    cleanup_dir(&dir);

    diagnostics.push_results(results, read_only);
    open_diagnostics.push_phase_results(results, read_only, "open");
    first_read_diagnostics.push_phase_results(results, read_only, "first read");
}

#[derive(Default)]
struct ColdReadDiagnostics {
    table_probes: u64,
    block_metadata_probes: u64,
    data_block_reads: u64,
    filter_misses: u64,
    cache_misses: u64,
    open_read_requests: u64,
    len_requests: u64,
    read_exact_at_owned_requests: u64,
    read_object_bytes_requests: u64,
    read_current_manifest_requests: u64,
    open_append_requests: u64,
    acquire_writer_lease_requests: u64,
    list_directory_files_requests: u64,
    list_objects_requests: u64,
    open_read_micros: u64,
    len_micros: u64,
    read_exact_at_owned_micros: u64,
    read_object_bytes_micros: u64,
    read_current_manifest_micros: u64,
    open_append_micros: u64,
    acquire_writer_lease_micros: u64,
    list_directory_files_micros: u64,
    list_objects_micros: u64,
}

impl ColdReadDiagnostics {
    fn record(&mut self, stats: &trine_kv::DbStats) {
        self.table_probes = self
            .table_probes
            .saturating_add(stats.read_path.point_table_probes);
        self.block_metadata_probes = self
            .block_metadata_probes
            .saturating_add(stats.read_path.point_block_metadata_probes);
        self.data_block_reads = self
            .data_block_reads
            .saturating_add(stats.read_path.point_data_block_reads);
        self.filter_misses = self
            .filter_misses
            .saturating_add(stats.read_path.point_filter_misses);
        self.cache_misses = self.cache_misses.saturating_add(stats.block_cache_misses);
        self.open_read_requests = self
            .open_read_requests
            .saturating_add(stats.storage_operations.open_read.requests);
        self.len_requests = self
            .len_requests
            .saturating_add(stats.storage_operations.len.requests);
        self.read_exact_at_owned_requests = self
            .read_exact_at_owned_requests
            .saturating_add(stats.storage_operations.read_exact_at_owned.requests);
        self.read_object_bytes_requests = self
            .read_object_bytes_requests
            .saturating_add(stats.storage_operations.read_object_bytes.requests);
        self.read_current_manifest_requests = self
            .read_current_manifest_requests
            .saturating_add(stats.storage_operations.read_current_manifest.requests);
        self.open_append_requests = self
            .open_append_requests
            .saturating_add(stats.storage_operations.open_append.requests);
        self.acquire_writer_lease_requests = self
            .acquire_writer_lease_requests
            .saturating_add(stats.storage_operations.acquire_writer_lease.requests);
        self.list_directory_files_requests = self
            .list_directory_files_requests
            .saturating_add(stats.storage_operations.list_directory_files.requests);
        self.list_objects_requests = self
            .list_objects_requests
            .saturating_add(stats.storage_operations.list_objects.requests);
        self.open_read_micros = self
            .open_read_micros
            .saturating_add(stats.storage_operations.open_read.total_latency_micros);
        self.len_micros = self
            .len_micros
            .saturating_add(stats.storage_operations.len.total_latency_micros);
        self.read_exact_at_owned_micros = self.read_exact_at_owned_micros.saturating_add(
            stats
                .storage_operations
                .read_exact_at_owned
                .total_latency_micros,
        );
        self.read_object_bytes_micros = self.read_object_bytes_micros.saturating_add(
            stats
                .storage_operations
                .read_object_bytes
                .total_latency_micros,
        );
        self.read_current_manifest_micros = self.read_current_manifest_micros.saturating_add(
            stats
                .storage_operations
                .read_current_manifest
                .total_latency_micros,
        );
        self.open_append_micros = self
            .open_append_micros
            .saturating_add(stats.storage_operations.open_append.total_latency_micros);
        self.acquire_writer_lease_micros = self.acquire_writer_lease_micros.saturating_add(
            stats
                .storage_operations
                .acquire_writer_lease
                .total_latency_micros,
        );
        self.list_directory_files_micros = self.list_directory_files_micros.saturating_add(
            stats
                .storage_operations
                .list_directory_files
                .total_latency_micros,
        );
        self.list_objects_micros = self
            .list_objects_micros
            .saturating_add(stats.storage_operations.list_objects.total_latency_micros);
    }

    fn record_delta(&mut self, before: &trine_kv::DbStats, after: &trine_kv::DbStats) {
        self.record_read_path_delta(before, after);
        self.record_storage_request_delta(before, after);
        self.record_storage_latency_delta(before, after);
    }

    fn record_read_path_delta(&mut self, before: &trine_kv::DbStats, after: &trine_kv::DbStats) {
        self.table_probes = self.table_probes.saturating_add(
            after
                .read_path
                .point_table_probes
                .saturating_sub(before.read_path.point_table_probes),
        );
        self.block_metadata_probes = self.block_metadata_probes.saturating_add(
            after
                .read_path
                .point_block_metadata_probes
                .saturating_sub(before.read_path.point_block_metadata_probes),
        );
        self.data_block_reads = self.data_block_reads.saturating_add(
            after
                .read_path
                .point_data_block_reads
                .saturating_sub(before.read_path.point_data_block_reads),
        );
        self.filter_misses = self.filter_misses.saturating_add(
            after
                .read_path
                .point_filter_misses
                .saturating_sub(before.read_path.point_filter_misses),
        );
        self.cache_misses = self.cache_misses.saturating_add(
            after
                .block_cache_misses
                .saturating_sub(before.block_cache_misses),
        );
    }

    fn record_storage_request_delta(
        &mut self,
        before: &trine_kv::DbStats,
        after: &trine_kv::DbStats,
    ) {
        self.open_read_requests = self.open_read_requests.saturating_add(
            after
                .storage_operations
                .open_read
                .requests
                .saturating_sub(before.storage_operations.open_read.requests),
        );
        self.len_requests = self.len_requests.saturating_add(
            after
                .storage_operations
                .len
                .requests
                .saturating_sub(before.storage_operations.len.requests),
        );
        self.read_exact_at_owned_requests = self.read_exact_at_owned_requests.saturating_add(
            after
                .storage_operations
                .read_exact_at_owned
                .requests
                .saturating_sub(before.storage_operations.read_exact_at_owned.requests),
        );
        self.read_object_bytes_requests = self.read_object_bytes_requests.saturating_add(
            after
                .storage_operations
                .read_object_bytes
                .requests
                .saturating_sub(before.storage_operations.read_object_bytes.requests),
        );
        self.read_current_manifest_requests = self.read_current_manifest_requests.saturating_add(
            after
                .storage_operations
                .read_current_manifest
                .requests
                .saturating_sub(before.storage_operations.read_current_manifest.requests),
        );
        self.open_append_requests = self.open_append_requests.saturating_add(
            after
                .storage_operations
                .open_append
                .requests
                .saturating_sub(before.storage_operations.open_append.requests),
        );
        self.acquire_writer_lease_requests = self.acquire_writer_lease_requests.saturating_add(
            after
                .storage_operations
                .acquire_writer_lease
                .requests
                .saturating_sub(before.storage_operations.acquire_writer_lease.requests),
        );
        self.list_directory_files_requests = self.list_directory_files_requests.saturating_add(
            after
                .storage_operations
                .list_directory_files
                .requests
                .saturating_sub(before.storage_operations.list_directory_files.requests),
        );
        self.list_objects_requests = self.list_objects_requests.saturating_add(
            after
                .storage_operations
                .list_objects
                .requests
                .saturating_sub(before.storage_operations.list_objects.requests),
        );
    }

    fn record_storage_latency_delta(
        &mut self,
        before: &trine_kv::DbStats,
        after: &trine_kv::DbStats,
    ) {
        self.open_read_micros = self.open_read_micros.saturating_add(
            after
                .storage_operations
                .open_read
                .total_latency_micros
                .saturating_sub(before.storage_operations.open_read.total_latency_micros),
        );
        self.len_micros = self.len_micros.saturating_add(
            after
                .storage_operations
                .len
                .total_latency_micros
                .saturating_sub(before.storage_operations.len.total_latency_micros),
        );
        self.read_exact_at_owned_micros = self.read_exact_at_owned_micros.saturating_add(
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
        );
        self.read_object_bytes_micros = self.read_object_bytes_micros.saturating_add(
            after
                .storage_operations
                .read_object_bytes
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .read_object_bytes
                        .total_latency_micros,
                ),
        );
        self.read_current_manifest_micros = self.read_current_manifest_micros.saturating_add(
            after
                .storage_operations
                .read_current_manifest
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .read_current_manifest
                        .total_latency_micros,
                ),
        );
        self.open_append_micros = self.open_append_micros.saturating_add(
            after
                .storage_operations
                .open_append
                .total_latency_micros
                .saturating_sub(before.storage_operations.open_append.total_latency_micros),
        );
        self.acquire_writer_lease_micros = self.acquire_writer_lease_micros.saturating_add(
            after
                .storage_operations
                .acquire_writer_lease
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .acquire_writer_lease
                        .total_latency_micros,
                ),
        );
        self.list_directory_files_micros = self.list_directory_files_micros.saturating_add(
            after
                .storage_operations
                .list_directory_files
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .list_directory_files
                        .total_latency_micros,
                ),
        );
        self.list_objects_micros = self.list_objects_micros.saturating_add(
            after
                .storage_operations
                .list_objects
                .total_latency_micros
                .saturating_sub(before.storage_operations.list_objects.total_latency_micros),
        );
    }

    fn push_results(&self, results: &mut Vec<BenchResult>, read_only: bool) {
        let label = if read_only {
            "read pruning cold read-only"
        } else {
            "read pruning cold"
        };
        self.push_results_with_label(results, label);
    }

    fn push_phase_results(
        &self,
        results: &mut Vec<BenchResult>,
        read_only: bool,
        phase: &'static str,
    ) {
        let label = if read_only {
            labelled3("read pruning cold read-only", phase, "phase")
        } else {
            labelled3("read pruning cold", phase, "phase")
        };
        self.push_results_with_label(results, label);
    }

    fn push_results_with_label(&self, results: &mut Vec<BenchResult>, label: &'static str) {
        results.push(BenchResult::diagnostic(
            labelled(label, "point table probes"),
            self.table_probes,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point block metadata probes"),
            self.block_metadata_probes,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point data block reads"),
            self.data_block_reads,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point filter skips"),
            self.filter_misses,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point cache misses"),
            self.cache_misses,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open read requests"),
            self.open_read_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage len requests"),
            self.len_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage read owned requests"),
            self.read_exact_at_owned_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage read object bytes requests"),
            self.read_object_bytes_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage current manifest requests"),
            self.read_current_manifest_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open append requests"),
            self.open_append_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage acquire writer lease requests"),
            self.acquire_writer_lease_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage list directory files requests"),
            self.list_directory_files_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage list objects requests"),
            self.list_objects_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open read micros"),
            self.open_read_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage len micros"),
            self.len_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage read owned micros"),
            self.read_exact_at_owned_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage read object bytes micros"),
            self.read_object_bytes_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage current manifest micros"),
            self.read_current_manifest_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open append micros"),
            self.open_append_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage acquire writer lease micros"),
            self.acquire_writer_lease_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage list directory files micros"),
            self.list_directory_files_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage list objects micros"),
            self.list_objects_micros,
        ));
    }
}

fn extend_prefix_partition_diagnostics(results: &mut Vec<BenchResult>) {
    let dir = temp_dir("read-pruning-prefix");
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

    let before = db.stats();
    let matching_checksum = prefix_scan_checksum(&bucket, 128, false);
    assert!(
        matching_checksum > 0,
        "matching prefix diagnostic must return rows"
    );
    let after_matching = db.stats();
    push_prefix_diagnostics(
        results,
        "read pruning prefix matching",
        &before,
        &after_matching,
    );

    let nonmatching_checksum = prefix_scan_checksum(&bucket, 128, true);
    assert_eq!(
        nonmatching_checksum, 0,
        "nonmatching prefix diagnostic must skip all rows"
    );
    let after_nonmatching = db.stats();
    push_prefix_diagnostics(
        results,
        "read pruning prefix nonmatching",
        &after_matching,
        &after_nonmatching,
    );
    drop(db);
    cleanup_dir(&dir);
}

fn push_prefix_diagnostics(
    results: &mut Vec<BenchResult>,
    name_prefix: &'static str,
    before: &trine_kv::DbStats,
    after: &trine_kv::DbStats,
) {
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "table probes"),
        after
            .read_path
            .prefix_table_probes
            .saturating_sub(before.read_path.prefix_table_probes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "block metadata probes"),
        after
            .read_path
            .prefix_block_metadata_probes
            .saturating_sub(before.read_path.prefix_block_metadata_probes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "data block reads"),
        after
            .read_path
            .prefix_data_block_reads
            .saturating_sub(before.read_path.prefix_data_block_reads),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "filter skips"),
        after
            .read_path
            .prefix_filter_misses
            .saturating_sub(before.read_path.prefix_filter_misses),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "table filter misses"),
        after
            .filters
            .table_prefix_misses
            .saturating_sub(before.filters.table_prefix_misses),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "block filter misses"),
        after
            .filters
            .block_prefix_misses
            .saturating_sub(before.filters.block_prefix_misses),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "cache misses"),
        after
            .block_cache_misses
            .saturating_sub(before.block_cache_misses),
    ));
}

fn bench_runtime_block_decode_reads() -> Vec<BenchResult> {
    vec![
        bench_runtime_block_decode_read(
            "native runtime block decode read",
            "native-runtime-block-decode",
            RuntimeOptions::native_threads(),
        ),
        bench_runtime_block_decode_read(
            "inline runtime block decode read",
            "inline-runtime-block-decode",
            RuntimeOptions::inline(),
        ),
    ]
}

fn bench_runtime_block_decode_read(
    name: &'static str,
    dir_name: &str,
    runtime: RuntimeOptions,
) -> BenchResult {
    let dir = temp_dir(dir_name);
    let mut options = benchmark_persistent_options(&dir);
    options.runtime = runtime;
    options.block_cache_bytes = 0;
    if !runtime.capabilities().background_threads() {
        options.background_worker_count = 0;
    }
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

    let result = measure(name, OPS, || {
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

        let stats = db.stats();
        assert!(
            stats.read_path.point_data_block_reads >= OPS as u64,
            "benchmark must exercise table data-block reads"
        );
        assert_eq!(
            stats.block_cache_hits, 0,
            "benchmark disables the block cache to force decode reads"
        );
        assert!(
            stats.block_cache_misses >= OPS as u64,
            "benchmark must miss the disabled cache before loading blocks"
        );
        checksum
            .saturating_add(stats.read_path.point_data_block_reads)
            .saturating_add(stats.block_cache_misses)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

fn bench_index_seek_policies() -> Vec<BenchResult> {
    let mut results = Vec::new();
    for (size, label) in [(64, "small"), (1_024, "medium"), (8_192, "large")] {
        for (policy, policy_label) in [
            (IndexSearchPolicy::Linear, "linear"),
            (IndexSearchPolicy::Binary, "binary"),
            (IndexSearchPolicy::Auto, "auto"),
        ] {
            results.push(bench_index_seek_policy(size, label, policy, policy_label));
        }
    }
    results
}

fn bench_index_seek_policy(
    size: usize,
    size_label: &'static str,
    policy: IndexSearchPolicy,
    policy_label: &'static str,
) -> BenchResult {
    let bucket_options = BucketOptions {
        index_search_policy: policy,
        // Smaller blocks create enough block-index entries for this tiny
        // harness to exercise the configured lookup policy.
        block_bytes: 512,
        ..BucketOptions::default()
    };
    let (dir, db, bucket) = flushed_persistent_db(
        &format!("index-{policy_label}-{size_label}"),
        size,
        bucket_options,
    );
    let result = measure(
        labelled3("index seek policy", policy_label, size_label),
        OPS,
        || {
            let mut checksum = 0;
            for index in 0..OPS {
                let row = (index * 17) % size;
                checksum += bucket
                    .get_sync(&key(row))
                    .expect("get succeeds")
                    .map_or(0, |value| value.len() as u64);
            }
            black_box(policy);
            checksum
        },
    );
    drop(db);
    cleanup_dir(&dir);
    result
}

fn bench_long_shared_prefix_get() -> BenchResult {
    let dir = temp_dir("long-shared-prefix");
    let bucket_options = BucketOptions {
        block_bytes: 512,
        ..BucketOptions::default()
    };
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = bucket_options;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let keys = (0..ROWS).map(long_shared_prefix_key).collect::<Vec<_>>();

    for (index, key) in keys.iter().enumerate() {
        bucket
            .put_sync(key.as_slice(), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");

    let result = measure("long shared-prefix get", OPS, || {
        let mut checksum = 0;
        for index in 0..OPS {
            let row = (index * 17) % ROWS;
            checksum += bucket
                .get_sync(&keys[row])
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64);
        }
        black_box(&keys);
        checksum
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

fn bench_iterator_advance_to() -> Vec<BenchResult> {
    let items = (0..8192).map(|index| index * 2).collect::<Vec<usize>>();
    vec![
        measure("iterator advance_to near targets", OPS, || {
            let mut current = 0;
            let mut checksum = 0;
            for _ in 0..OPS {
                let target = items[current].saturating_add(2_usize);
                current = search::advance_to(&items, current, &target).unwrap_or(current);
                checksum += current as u64;
            }
            checksum
        }),
        measure("iterator advance_to far targets", OPS, || {
            let mut current = 0;
            let mut checksum = 0;
            for step in 0..OPS {
                let target = (step * 97) % (items.len() * 2);
                current = search::advance_to(&items, current, &target).unwrap_or(current);
                checksum += current as u64;
            }
            checksum
        }),
        measure("iterator advance_to random targets", OPS, || {
            let mut current = 0;
            let mut seed = 0xfeed_f00d_u64;
            let mut checksum = 0;
            for _ in 0..OPS {
                seed = xorshift(seed);
                let target = seed_index(seed, items.len() * 2);
                current = search::advance_to(&items, current, &target).unwrap_or(current);
                checksum += current as u64;
            }
            checksum
        }),
    ]
}

fn bench_codec_comparison() -> Vec<BenchResult> {
    let data_block = repeated_bytes(b"data-block-", 4096);
    let index_block = repeated_bytes(b"index-block-", 2048);
    let tombstone_block = repeated_bytes(b"range-tombstone-", 2048);
    let mut results = Vec::new();
    for (label, bytes) in [
        ("Trine data blocks", data_block),
        ("Trine index blocks", index_block),
        ("Trine range tombstone blocks", tombstone_block),
    ] {
        results.push(bench_codec("codec none", label, CodecBench::None, &bytes));
        results.push(bench_codec(
            "codec fast block compression",
            label,
            CodecBench::FastLz4Block,
            &bytes,
        ));
    }
    results
}

#[derive(Debug, Clone, Copy)]
enum CodecBench {
    None,
    FastLz4Block,
}

fn bench_codec(
    name: &'static str,
    label: &'static str,
    codec: CodecBench,
    bytes: &[u8],
) -> BenchResult {
    measure(labelled(name, label), OPS, || {
        let mut checksum = 0;
        for _ in 0..OPS {
            let encoded = encode_bench_block(codec, bytes);
            let decoded = decode_bench_block(codec, &encoded, bytes.len());
            checksum += (encoded.len() + decoded.len()) as u64;
        }
        checksum
    })
}

fn encode_bench_block(codec: CodecBench, bytes: &[u8]) -> Vec<u8> {
    match codec {
        CodecBench::None => bytes.to_vec(),
        CodecBench::FastLz4Block => lz4_flex::block::compress(bytes),
    }
}

fn decode_bench_block(codec: CodecBench, bytes: &[u8], uncompressed_len: usize) -> Vec<u8> {
    match codec {
        CodecBench::None => {
            assert_eq!(bytes.len(), uncompressed_len);
            bytes.to_vec()
        }
        CodecBench::FastLz4Block => {
            lz4_flex::block::decompress(bytes, uncompressed_len).expect("lz4 block decodes")
        }
    }
}

fn populated_memory_db(rows: usize) -> Db {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    db
}

fn populated_delta_memory_db(rows: usize) -> Db {
    let mut options = DbOptions::memory();
    options.write_buffer_bytes = 1;
    let db = Db::open_sync(options).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    assert_delta_backed_memory_stats(&db);
    db
}

fn populated_active_memtable_db(name: &str, rows: usize) -> (PathBuf, Db, trine_kv::Bucket) {
    let dir = temp_dir(name);
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = 0;
    options.write_buffer_bytes = 64 * 1024 * 1024;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    assert_active_memtable_stats(&db);
    (dir, db, bucket)
}

fn assert_delta_backed_memory_stats(db: &Db) {
    let stats = db.stats();
    assert!(
        stats.memtable_bytes > 0,
        "delta-backed benchmark must keep recent write bytes in memory stats"
    );
    assert_eq!(
        stats.immutable_memtables, 0,
        "delta-backed benchmark must not use immutable memtable queues"
    );
    assert_eq!(
        stats.total_tables, 0,
        "delta-backed benchmark must stay in memory"
    );
}

fn assert_active_memtable_stats(db: &Db) {
    let stats = db.stats();
    assert!(
        stats.memtable_bytes > 0,
        "active memtable benchmark must keep recent write bytes in memory stats"
    );
    assert_eq!(
        stats.immutable_memtables, 0,
        "active memtable benchmark must avoid freeze/flush work"
    );
    assert_eq!(
        stats.total_tables, 0,
        "active memtable benchmark must avoid table reads"
    );
}

fn populated_prefix_db(rows: usize, filters: bool) -> Db {
    let mut options = DbOptions::memory();
    options.default_bucket_options = prefix_options(filters);
    let db = Db::open_sync(options).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(prefix_key(index), value(index))
            .expect("put succeeds");
    }
    db
}

fn random_get_checksum(bucket: &trine_kv::Bucket, rows: usize, ops: usize, mut seed: u64) -> u64 {
    let mut checksum = 0;
    for _ in 0..ops {
        seed = xorshift(seed);
        let index = seed_index(seed, rows);
        checksum += bucket
            .get_sync(&key(index))
            .expect("get succeeds")
            .map_or(0, |value| value.len() as u64);
    }
    checksum
}

fn missing_get_checksum(bucket: &trine_kv::Bucket, ops: usize) -> u64 {
    let mut checksum = 0;
    for index in 0..ops {
        checksum += bucket
            .get_sync(format!("missing-{index:04}").as_bytes())
            .expect("missing get succeeds")
            .map_or(0, |value| value.len() as u64);
    }
    checksum
}

fn sequential_point_batch_checksum(bucket: &trine_kv::Bucket, keys: &[Vec<u8>]) -> u64 {
    let mut checksum = 0;
    for key in keys {
        checksum += bucket
            .get_sync(key)
            .expect("sequential batch point read succeeds")
            .map_or(0, |value| value.len() as u64);
    }
    checksum
}

fn batched_point_read_checksum(
    bucket: &trine_kv::Bucket,
    keys: &[Vec<u8>],
    batch_size: usize,
) -> u64 {
    let mut checksum = 0;
    for batch in keys.chunks(batch_size) {
        checksum += bucket
            .get_many_sync(batch)
            .expect("batched point read succeeds")
            .into_iter()
            .map(|value| value.map_or(0, |value| value.len() as u64))
            .sum::<u64>();
    }
    checksum
}

fn point_read_keys(rows: usize, ops: usize, mut seed: u64) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(ops);
    for _ in 0..ops {
        seed = xorshift(seed);
        keys.push(key(seed_index(seed, rows)));
    }
    keys
}

fn localized_point_read_keys(rows: usize, ops: usize) -> Vec<Vec<u8>> {
    (0..ops).map(|index| key(index % rows)).collect()
}

fn range_scan_checksum(bucket: &trine_kv::Bucket, scans: usize) -> u64 {
    let mut checksum = 0;
    for start in 0..scans {
        let end = start + 32;
        let iter = bucket
            .range_sync(&KeyRange::half_open(key(start), key(end)))
            .expect("range succeeds");
        checksum += iter
            .map(|item| item.expect("range item").value.len() as u64)
            .sum::<u64>();
    }
    checksum
}

fn prefix_scan_checksum(bucket: &trine_kv::Bucket, scans: usize, missing: bool) -> u64 {
    let mut checksum = 0;
    for tenant in 0..scans {
        let prefix = if missing {
            format!("missing:{tenant:02}:")
        } else {
            format!("tenant:{:02}:", tenant % 16)
        };
        let iter = bucket
            .prefix_sync(prefix.as_bytes())
            .expect("prefix succeeds");
        checksum += iter
            .map(|item| item.expect("prefix item").value.len() as u64)
            .sum::<u64>();
    }
    checksum
}

fn flushed_persistent_db(
    name: &str,
    rows: usize,
    bucket_options: BucketOptions,
) -> (PathBuf, Db, trine_kv::Bucket) {
    let dir = temp_dir(name);
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = bucket_options;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");
    (dir, db, bucket)
}

fn large_blob_db(name: &str, rows: usize) -> (PathBuf, Db, trine_kv::Bucket) {
    let dir = temp_dir(name);
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = large_blob_options();
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..rows {
        bucket
            .put_sync(key(index), large_value(index))
            .expect("large put succeeds");
    }
    db.flush_sync().expect("large flush succeeds");
    (dir, db, bucket)
}

fn large_blob_options() -> BucketOptions {
    BucketOptions {
        blob_threshold_bytes: 4 * 1024,
        ..BucketOptions::default()
    }
}

fn prefix_options(filters: bool) -> BucketOptions {
    BucketOptions {
        prefix_extractor: PrefixExtractor::Separator(b':'),
        prefix_filter_policy: if filters {
            PrefixFilterPolicy::Bloom { bits_per_prefix: 8 }
        } else {
            PrefixFilterPolicy::Disabled
        },
        filter_policy: if filters {
            FilterPolicy::Bloom { bits_per_key: 10 }
        } else {
            FilterPolicy::Disabled
        },
        ..BucketOptions::default()
    }
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:08}").into_bytes()
}

fn prefix_key(index: usize) -> Vec<u8> {
    format!("tenant:{:02}:key-{index:08}", index % 16).into_bytes()
}

fn long_shared_prefix_key(index: usize) -> Vec<u8> {
    format!("tenant:analytics:region:us-west-2:dataset:events:shard:000000:key-{index:08}")
        .into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index:08}-{}", index.wrapping_mul(31)).into_bytes()
}

fn large_value(index: usize) -> Vec<u8> {
    let mut seed = (index as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(0x1234_5678_9abc_def0);
    let mut bytes = Vec::with_capacity(LARGE_VALUE_BYTES);
    while bytes.len() < LARGE_VALUE_BYTES {
        seed = xorshift(seed);
        bytes.extend_from_slice(&seed.to_le_bytes());
    }
    bytes.truncate(LARGE_VALUE_BYTES);
    bytes
}

fn repeated_bytes(prefix: &[u8], len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        bytes.extend_from_slice(prefix);
    }
    bytes.truncate(len);
    bytes
}

fn xorshift(mut value: u64) -> u64 {
    value ^= value << 13;
    value ^= value >> 7;
    value ^ (value << 17)
}

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "trine-kv-bench-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn seed_index(seed: u64, len: usize) -> usize {
    let len = u64::try_from(len).expect("length fits in u64");
    usize::try_from(seed % len).expect("seed modulo length fits in usize")
}

fn cleanup_dir(dir: &Path) {
    if let Err(error) = fs::remove_dir_all(dir) {
        if error.kind() != std::io::ErrorKind::NotFound {
            eprintln!("failed to remove {}: {error}", dir.display());
        }
    }
}

fn labelled(name: &'static str, label: &'static str) -> &'static str {
    Box::leak(format!("{name} {label}").into_boxed_str())
}

fn labelled3(name: &'static str, first: &'static str, second: &'static str) -> &'static str {
    Box::leak(format!("{name} {first} {second}").into_boxed_str())
}
