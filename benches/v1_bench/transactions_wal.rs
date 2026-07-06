use super::{
    BenchResult, Db, DbOptions, DurabilityMode, Instant, OPS, ROWS, TransactionOptions,
    WRITE_DIAGNOSTIC_OPS, WritePathDiagnostics, benchmark_persistent_options, cleanup_dir,
    duration_micros, extend_flush_wall_diagnostics, extend_wal_replay_open_diagnostics, key,
    labelled, measure, populate_wal_replay_dir, populated_memory_db, temp_dir, value,
};

pub(super) fn bench_snapshot_read_under_writes() -> BenchResult {
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

pub(super) fn bench_transaction_commit() -> BenchResult {
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

pub(super) fn bench_transaction_conflict() -> BenchResult {
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

pub(super) fn bench_wal_replay() -> BenchResult {
    let dir = temp_dir("wal-replay");
    let options = benchmark_persistent_options(&dir);
    populate_wal_replay_dir(options.clone());
    let result = measure("WAL replay", ROWS, || {
        let db = Db::open_sync(options.clone()).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        bucket
            .get_sync(&key(ROWS / 2))
            .expect("get succeeds")
            .map_or(0, |value| value.len() as u64)
    });
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_wal_replay_read_only() -> BenchResult {
    let dir = temp_dir("wal-replay-read-only");
    let options = benchmark_persistent_options(&dir);
    populate_wal_replay_dir(options.clone());
    let result = measure("WAL replay read-only", ROWS, || {
        let db =
            Db::open_sync(options.clone().read_only()).expect("read-only persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        bucket
            .get_sync(&key(ROWS / 2))
            .expect("get succeeds")
            .map_or(0, |value| value.len() as u64)
    });
    cleanup_dir(&dir);
    result
}

pub(super) fn extend_wal_replay_diagnostics(results: &mut Vec<BenchResult>) {
    extend_wal_replay_open_diagnostics(results, "WAL replay writable open", false);
    extend_wal_replay_open_diagnostics(results, "WAL replay read-only open", true);
}

pub(super) fn extend_persistent_write_path_diagnostics(results: &mut Vec<BenchResult>) {
    extend_single_key_write_diagnostics(results, DurabilityMode::Buffered, "buffered");
    extend_single_key_write_diagnostics(results, DurabilityMode::Flush, "flush");
    extend_single_key_write_diagnostics(results, DurabilityMode::SyncData, "sync-data");
    extend_single_key_write_diagnostics(results, DurabilityMode::SyncAll, "sync-all");
    extend_explicit_persist_diagnostics(results);
    extend_flush_wall_diagnostics(results);
}

pub(super) fn extend_single_key_write_diagnostics(
    results: &mut Vec<BenchResult>,
    durability: DurabilityMode,
    label: &'static str,
) {
    let mut diagnostics = WritePathDiagnostics::default();
    let mut wall_micros = 0_u64;
    let mut wal_records = 0_u64;
    let mut wal_bytes = 0_u64;
    for index in 0..32 {
        let dir = temp_dir(labelled("write-path-diagnostic", label));
        let db = Db::open_sync(DbOptions::persistent(&dir).with_durability(durability))
            .expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        let before = db.stats();
        let started = Instant::now();
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
        wall_micros = wall_micros.saturating_add(duration_micros(started.elapsed()));
        let after = db.stats();
        diagnostics.record_delta(&before, &after);
        wal_records = wal_records.saturating_add(
            after
                .wal_records_accepted
                .saturating_sub(before.wal_records_accepted),
        );
        wal_bytes = wal_bytes.saturating_add(
            after
                .wal_bytes_accepted
                .saturating_sub(before.wal_bytes_accepted),
        );
        drop(db);
        cleanup_dir(&dir);
    }

    let base = labelled("write path single-key diagnostic", label);
    results.push(BenchResult::diagnostic(
        labelled(base, "wall micros"),
        wall_micros,
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "wal records accepted"),
        wal_records,
    ));
    results.push(BenchResult::diagnostic(
        labelled(base, "wal bytes accepted"),
        wal_bytes,
    ));
    diagnostics.push_results(results, base);
}

pub(super) fn extend_explicit_persist_diagnostics(results: &mut Vec<BenchResult>) {
    let mut commit_diagnostics = WritePathDiagnostics::default();
    let mut persist_diagnostics = WritePathDiagnostics::default();
    let mut commit_wall_micros = 0_u64;
    let mut persist_wall_micros = 0_u64;
    let dir = temp_dir("write-path-explicit-persist-diagnostic");
    let db = Db::open_sync(DbOptions::persistent(&dir).with_durability(DurabilityMode::Buffered))
        .expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    for index in 0..WRITE_DIAGNOSTIC_OPS {
        let before = db.stats();
        let started = Instant::now();
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
        commit_wall_micros = commit_wall_micros.saturating_add(duration_micros(started.elapsed()));
        let after_commit = db.stats();
        commit_diagnostics.record_delta(&before, &after_commit);

        let started = Instant::now();
        db.persist_sync(DurabilityMode::SyncData)
            .expect("persist succeeds");
        persist_wall_micros =
            persist_wall_micros.saturating_add(duration_micros(started.elapsed()));
        let after_persist = db.stats();
        persist_diagnostics.record_delta(&after_commit, &after_persist);
    }
    drop(db);
    cleanup_dir(&dir);

    let commit_label = "write path explicit persist diagnostic commit";
    results.push(BenchResult::diagnostic(
        labelled(commit_label, "wall micros"),
        commit_wall_micros,
    ));
    commit_diagnostics.push_results(results, commit_label);

    let persist_label = "write path explicit persist diagnostic persist";
    results.push(BenchResult::diagnostic(
        labelled(persist_label, "wall micros"),
        persist_wall_micros,
    ));
    persist_diagnostics.push_results(results, persist_label);
}
