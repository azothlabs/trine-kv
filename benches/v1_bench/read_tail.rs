use super::{
    Arc, AtomicBool, Barrier, BenchResult, Db, DurabilityMode, Instant, KeyRange, Ordering, ROWS,
    WalShardPolicy, benchmark_persistent_options, black_box, cleanup_dir, duration_micros, key,
    labelled, labelled_level, temp_dir, thread, usize_to_u64, value,
};

pub(super) fn extend_read_tail_latency_diagnostic(results: &mut Vec<BenchResult>) {
    // Consolidated measure-first: point-read tail latency (p50/p99/p999) under
    // three conditions decides whether scan-cache isolation and compaction rate
    // limiting are worth building. Idle vs concurrent long scan answers cache
    // pollution; idle vs concurrent compaction answers compaction interference.
    let label = "read tail latency diagnostic";
    let dir = temp_dir(label);
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = 0;
    // A small cache and a dataset larger than it make pollution observable.
    options.block_cache_bytes = 512 * 1024;
    let db = Arc::new(Db::open_sync(options).expect("persistent db opens"));
    let bucket = db.default_bucket_sync().expect("bucket opens");

    let total = 4096_usize;
    for index in 0..total {
        bucket
            .put_sync(key(index), vec![b'v'; 256])
            .expect("put padded value");
    }
    db.flush_sync().expect("flush data");
    db.compact_range_sync(KeyRange::all())
        .expect("settle layout");

    // A small hot point-read set that fits the cache when nothing else churns it.
    let hot: Vec<Vec<u8>> = (0..64).map(|i| key(i * 5)).collect();
    let samples = 2000_usize;

    push_point_tail_row(results, label, "idle", &db, &hot, samples);

    let stop = Arc::new(AtomicBool::new(false));
    let scanner = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let bucket = db.default_bucket_sync().expect("scanner bucket");
            while !stop.load(Ordering::Relaxed) {
                let mut iter = bucket.range_sync(&KeyRange::all()).expect("scan");
                while let Some(row) = iter.next_sync() {
                    let _ = row.expect("scan row");
                }
            }
        })
    };
    push_point_tail_row(results, label, "under scan", &db, &hot, samples);
    stop.store(true, Ordering::Relaxed);
    scanner.join().expect("scanner joins");

    let stop = Arc::new(AtomicBool::new(false));
    let compactor = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let bucket = db.default_bucket_sync().expect("compactor bucket");
            let mut round = 0_usize;
            while !stop.load(Ordering::Relaxed) {
                for i in 0..256_usize {
                    let _ = bucket.put_sync(key(total + round * 256 + i), vec![b'w'; 256]);
                }
                let _ = db.flush_sync();
                let _ = db.compact_range_sync(KeyRange::all());
                round += 1;
            }
        })
    };
    push_point_tail_row(results, label, "under compaction", &db, &hot, samples);
    stop.store(true, Ordering::Relaxed);
    compactor.join().expect("compactor joins");

    drop(bucket);
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn push_point_tail_row(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    phase: &'static str,
    db: &Db,
    hot: &[Vec<u8>],
    samples: usize,
) {
    let bucket = db.default_bucket_sync().expect("reader bucket");
    let mut latencies = Vec::with_capacity(samples);
    for i in 0..samples {
        let probe = &hot[i % hot.len()];
        let started = Instant::now();
        let value = bucket.get_sync(probe).expect("point read");
        black_box(value);
        latencies.push(duration_micros(started.elapsed()));
    }
    latencies.sort_unstable();

    let base = labelled(label, phase);
    results.push(BenchResult::diagnostic(
        labelled(base, "p50 micros"),
        percentile(&latencies, 0.50),
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "p99 micros"),
        percentile(&latencies, 0.99),
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "p999 micros"),
        percentile(&latencies, 0.999),
    ));
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(super) fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (fraction * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

pub(super) fn extend_tombstone_scan_waste_diagnostic(results: &mut Vec<BenchResult>) {
    // Measure-first for Phase 4 (read-path whole-range skip): how much does a
    // range scan wade through deleted keys while a big range tombstone is still
    // on the read path (not yet compacted), versus after compaction drops it?
    let label = "tombstone scan waste diagnostic";
    let dir = temp_dir(label);
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    let total = ROWS;
    for index in 0..total {
        bucket.put_sync(key(index), value(index)).expect("put");
    }
    db.flush_sync().expect("flush data");
    // Delete the middle ~90% as one big range tombstone; keep the edges live.
    let low = key(total / 20);
    let high = key(total - total / 20);
    db.delete_range_sync(KeyRange::half_open(low, high))
        .expect("range delete");
    db.flush_sync().expect("flush tombstone");

    push_scan_waste_row(results, label, "before compaction", &db, &bucket);
    // Phase 3 file-drop cleans the covered tables; scan again for the baseline.
    db.compact_range_sync(KeyRange::all()).expect("compact");
    push_scan_waste_row(results, label, "after compaction", &db, &bucket);

    drop(bucket);
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn push_scan_waste_row(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    phase: &'static str,
    db: &Db,
    bucket: &trine_kv::Bucket,
) {
    let before = db.stats();
    let mut iter = bucket
        .range_sync(&KeyRange::all())
        .expect("range scan opens");
    let mut returned = 0_u64;
    while let Some(row) = iter.next_sync() {
        row.expect("scan row");
        returned += 1;
    }
    drop(iter);
    let after = db.stats();

    let internal = after
        .scan_internal_records
        .saturating_sub(before.scan_internal_records);
    let user = after.scan_user_keys.saturating_sub(before.scan_user_keys);
    let hidden = after
        .scan_tombstone_hidden_keys
        .saturating_sub(before.scan_tombstone_hidden_keys);
    let ratio_x1000 = if user == 0 {
        0
    } else {
        internal.saturating_mul(1_000) / user
    };

    let base = labelled(label, phase);
    results.push(BenchResult::diagnostic(labelled(base, "user keys"), user));
    results.push(BenchResult::diagnostic(
        labelled(base, "internal records"),
        internal,
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "tombstone hidden keys"),
        hidden,
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "internal per user x1000"),
        ratio_x1000,
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "rows returned"),
        returned,
    ));
}

pub(super) fn extend_group_commit_diagnostic(results: &mut Vec<BenchResult>) {
    // Group commit: concurrent durable single-key writers should amortize fsyncs.
    // With one WAL shard, concurrent commits share a lane and the worker batches
    // them under one fsync, so throughput rises with concurrency. With multiple
    // shards, sequence round-robin spreads concurrent commits across lanes and
    // (on single-device storage where fsyncs serialize) batching cannot engage.
    for wal_shards in [1_usize, 4] {
        for concurrency in [1_usize, 8] {
            push_group_commit_diagnostic(results, concurrency, wal_shards);
        }
    }
}

pub(super) fn push_group_commit_diagnostic(
    results: &mut Vec<BenchResult>,
    concurrency: usize,
    wal_shards: usize,
) {
    let base: &'static str = Box::leak(
        format!("group commit sync-data {wal_shards}-shard x{concurrency}").into_boxed_str(),
    );
    let dir = temp_dir(base);
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = 0;
    options.wal_shards = WalShardPolicy::Fixed(wal_shards);
    options = options.with_durability(DurabilityMode::SyncData);
    let db = Arc::new(Db::open_sync(options).expect("group commit db opens"));

    let writes_per_thread = 256_usize;
    let total = concurrency.saturating_mul(writes_per_thread);
    let barrier = Arc::new(Barrier::new(concurrency));

    let before = db.stats();
    let started = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for thread_index in 0..concurrency {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let bucket = db.default_bucket_sync().expect("group commit bucket opens");
            barrier.wait();
            for write_index in 0..writes_per_thread {
                let row = thread_index * writes_per_thread + write_index;
                bucket.put_sync(key(row), value(row)).expect("durable put");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("group commit writer joins");
    }
    let elapsed = duration_micros(started.elapsed());
    let after = db.stats();

    let commits = usize_to_u64(total);
    let persists = after
        .storage_operations
        .persist
        .requests
        .saturating_sub(before.storage_operations.persist.requests);
    let ops_per_sec = if elapsed == 0 {
        0
    } else {
        commits.saturating_mul(1_000_000) / elapsed
    };
    // Commits served per fsync, scaled by 1000; >1000 means group commit batched.
    let commits_per_persist_x1000 = if persists == 0 {
        0
    } else {
        commits.saturating_mul(1_000) / persists
    };

    results.push(BenchResult::diagnostic(
        labelled(base, "total commits"),
        commits,
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "wall micros"),
        elapsed,
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "ops per sec"),
        ops_per_sec,
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "wal persists"),
        persists,
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "commits per persist x1000"),
        commits_per_persist_x1000,
    ));

    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn extend_layered_filter_fpr_diagnostic(results: &mut Vec<BenchResult>) {
    // Measure-first for Monkey-style layered filter allocation: report each
    // level's observed point false-positive rate under a negative-lookup load,
    // so a later per-level bits/key curve has real f_i numbers to optimize.
    let label = "layered filter fpr diagnostic";
    let dir = temp_dir(label);
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = 0;
    options.max_l0_files = 64;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    // Write only even keys, so odd keys are in-range absent keys for negatives.
    let total = ROWS * 4;
    let chunk = total / 4;
    for c in 0..4 {
        for i in 0..chunk {
            let row = (c * chunk + i) * 2;
            bucket.put_sync(key(row), value(row)).expect("write key");
        }
        db.flush_sync().expect("flush L0 table");
        if c < 2 {
            db.compact_range_sync(KeyRange::all())
                .expect("compact toward L1");
        }
    }

    let before = db.stats();
    let mut absent_seen = 0_u64;
    for row in 0..total {
        let odd = row * 2 + 1;
        if bucket.get_sync(&key(odd)).expect("missing read").is_none() {
            absent_seen += 1;
        }
    }
    assert!(absent_seen > 0, "negative lookups must miss");
    let after = db.stats();

    for level in &after.level_filters {
        let fp = level.filters.table_point_false_positives;
        let allowed_absent = fp.saturating_add(level.filters.table_point_misses);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let fpr_ppm = level
            .filters
            .table_point_false_positive_rate()
            .map_or(0, |rate| (rate * 1_000_000.0) as u64);
        results.push(BenchResult::diagnostic(
            labelled_level(label, level.level, "false positive rate ppm"),
            fpr_ppm,
        ));
        results.push(BenchResult::diagnostic(
            labelled_level(label, level.level, "false positives"),
            fp,
        ));
        results.push(BenchResult::diagnostic(
            labelled_level(label, level.level, "filter allowed absent probes"),
            allowed_absent,
        ));
        results.push(BenchResult::diagnostic(
            labelled_level(label, level.level, "tables"),
            usize_to_u64(level.tables),
        ));
        results.push(BenchResult::diagnostic(
            labelled_level(label, level.level, "resident filter bytes"),
            level.filter_resident_bytes,
        ));
    }

    let negative_data_block_reads = after
        .read_path
        .point_data_block_reads
        .saturating_sub(before.read_path.point_data_block_reads);
    results.push(BenchResult::diagnostic(
        labelled(label, "negative lookup data block reads"),
        negative_data_block_reads,
    ));

    drop(bucket);
    drop(db);
    cleanup_dir(&dir);
}
