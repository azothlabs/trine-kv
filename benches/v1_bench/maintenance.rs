use super::{
    Arc, BACKGROUND_CONTENTION_OPS, BACKGROUND_CONTENTION_ROWS, Barrier, BenchResult,
    ColdReadDiagnostics, Db, Instant, KeyRange, MaintenanceBudget, OPS, Path, ROWS,
    WritePathDiagnostics, bench_level_table_bytes, bench_level_table_count,
    benchmark_persistent_options, cleanup_dir, duration_micros,
    extend_blob_gc_write_amplification_diagnostic,
    extend_blob_level_merge_write_amplification_diagnostic, key, labelled,
    localized_point_read_keys, push_maintenance_write_amp_results, random_get_checksum,
    sequential_point_batch_checksum, temp_dir, thread, usize_to_u64, value,
};

pub(super) fn extend_flush_wall_diagnostics(results: &mut Vec<BenchResult>) {
    let dir = temp_dir("write-path-flush-diagnostic");
    let db = Db::open_sync(benchmark_persistent_options(&dir)).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }

    let before = db.stats();
    let started = Instant::now();
    db.flush_sync().expect("flush succeeds");
    let wall_micros = duration_micros(started.elapsed());
    let after = db.stats();
    let mut diagnostics = WritePathDiagnostics::default();
    diagnostics.record_delta(&before, &after);
    drop(db);
    cleanup_dir(&dir);

    let label = "write path flush diagnostic";
    results.push(BenchResult::diagnostic(
        labelled(label, "wall micros"),
        wall_micros,
    ));
    diagnostics.push_results(results, label);
}

pub(super) fn extend_maintenance_write_amplification_diagnostics(results: &mut Vec<BenchResult>) {
    extend_flush_write_amplification_diagnostic(results);
    extend_compaction_write_amplification_diagnostic(results);
    extend_compaction_scope_comparison_diagnostic(results);
    extend_guard_multi_table_compaction_diagnostic(results);
    extend_blob_gc_write_amplification_diagnostic(results);
    extend_blob_level_merge_write_amplification_diagnostic(results);
}

pub(super) fn extend_background_maintenance_contention_diagnostics(results: &mut Vec<BenchResult>) {
    push_background_maintenance_contention_diagnostic(
        results,
        "foreground maintenance contention diagnostic",
        0,
    );
    push_background_maintenance_contention_diagnostic(
        results,
        "background maintenance contention diagnostic",
        1,
    );
}

pub(super) fn push_background_maintenance_contention_diagnostic(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    background_workers: usize,
) {
    let dir = temp_dir(label);
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = background_workers;
    options.write_buffer_bytes = 512;
    options.max_immutable_memtables = 2;
    options.max_l0_files = 2;
    options.target_table_bytes = 2 * 1024;

    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..BACKGROUND_CONTENTION_ROWS {
        bucket
            .put_sync(key(index), value(index))
            .expect("initial put succeeds");
    }
    db.flush_sync().expect("initial flush succeeds");

    let before = db.stats();
    let start = Arc::new(Barrier::new(2));
    let reader_bucket = bucket.clone();
    let reader_start = Arc::clone(&start);
    let reader = thread::spawn(move || {
        reader_start.wait();
        let started = Instant::now();
        let checksum = random_get_checksum(
            &reader_bucket,
            BACKGROUND_CONTENTION_ROWS,
            BACKGROUND_CONTENTION_OPS,
            0xfeed_beef_cafe_babe,
        );
        (duration_micros(started.elapsed()), checksum)
    });

    start.wait();
    let write_started = Instant::now();
    let mut write_bytes = 0;
    for index in 0..BACKGROUND_CONTENTION_OPS {
        let row = BACKGROUND_CONTENTION_ROWS + index;
        let value = value(row);
        write_bytes += value.len() as u64;
        bucket.put_sync(key(row), value).expect("write succeeds");
    }
    let write_wall_micros = duration_micros(write_started.elapsed());
    let (read_wall_micros, read_bytes) = reader.join().expect("reader joins");
    assert!(
        read_bytes > 0,
        "background contention reader must read rows"
    );
    assert!(
        write_bytes > 0,
        "background contention writer must write rows"
    );

    let after = db.stats();
    let measurement = BackgroundMaintenanceContentionMeasurement {
        read_wall_micros,
        write_wall_micros,
        read_bytes,
        write_bytes,
    };
    push_background_maintenance_contention_results(results, label, &before, &after, measurement);
    drop(bucket);
    drop(db);
    cleanup_dir(&dir);
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BackgroundMaintenanceContentionMeasurement {
    read_wall_micros: u64,
    write_wall_micros: u64,
    read_bytes: u64,
    write_bytes: u64,
}

pub(super) fn push_background_maintenance_contention_results(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    before: &trine_kv::DbStats,
    after: &trine_kv::DbStats,
    measurement: BackgroundMaintenanceContentionMeasurement,
) {
    results.push(BenchResult::diagnostic(
        labelled(label, "read wall micros"),
        measurement.read_wall_micros,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "write wall micros"),
        measurement.write_wall_micros,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "read bytes"),
        measurement.read_bytes,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "write bytes"),
        measurement.write_bytes,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "cooperative yields"),
        after
            .maintenance_cooperative_yields
            .saturating_sub(before.maintenance_cooperative_yields),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "budget exhaustions"),
        after
            .maintenance_budget_exhaustions
            .saturating_sub(before.maintenance_budget_exhaustions),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "compaction runs"),
        after.compaction_runs.saturating_sub(before.compaction_runs),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "compaction input tables"),
        after
            .compaction_input_tables
            .saturating_sub(before.compaction_input_tables),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "compaction output tables"),
        after
            .compaction_output_tables
            .saturating_sub(before.compaction_output_tables),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "remaining immutable memtables"),
        after.immutable_memtables as u64,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "remaining l0 tables"),
        after.l0_tables as u64,
    ));

    let mut diagnostics = WritePathDiagnostics::default();
    diagnostics.record_delta(before, after);
    diagnostics.push_results(results, label);
}

pub(super) fn extend_flush_write_amplification_diagnostic(results: &mut Vec<BenchResult>) {
    let dir = temp_dir("write-amp-flush-diagnostic");
    let db = Db::open_sync(benchmark_persistent_options(&dir)).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }

    let before = db.stats();
    let started = Instant::now();
    db.flush_sync().expect("flush succeeds");
    let after = db.stats();
    push_maintenance_write_amp_results(
        results,
        "write amp flush diagnostic",
        &before,
        &after,
        duration_micros(started.elapsed()),
    );
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn extend_compaction_write_amplification_diagnostic(results: &mut Vec<BenchResult>) {
    let dir = temp_dir("write-amp-compaction-diagnostic");
    let db = Db::open_sync(benchmark_persistent_options(&dir)).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for chunk in 0..4 {
        for index in 0..(ROWS / 4) {
            let row = chunk * (ROWS / 4) + index;
            bucket.put_sync(key(row), value(row)).expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
    }

    let before = db.stats();
    let started = Instant::now();
    db.compact_range_sync(KeyRange::all())
        .expect("compaction succeeds");
    let after = db.stats();
    push_maintenance_write_amp_results(
        results,
        "write amp compaction diagnostic",
        &before,
        &after,
        duration_micros(started.elapsed()),
    );
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn extend_compaction_scope_comparison_diagnostic(results: &mut Vec<BenchResult>) {
    push_compaction_scope_comparison_diagnostic(
        results,
        "write amp local compaction comparison",
        CompactionScope::LocalMaintenance,
    );
    push_compaction_scope_comparison_diagnostic(
        results,
        "write amp broad compaction comparison",
        CompactionScope::BroadManual,
    );
}

#[derive(Debug, Clone, Copy)]
pub(super) enum CompactionScope {
    LocalMaintenance,
    BroadManual,
}

pub(super) fn push_compaction_scope_comparison_diagnostic(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    scope: CompactionScope,
) {
    let dir = temp_dir(label);
    prepare_disjoint_l0_compaction_workload(&dir);
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = 0;
    if matches!(scope, CompactionScope::LocalMaintenance) {
        options.max_l0_files = 1;
    }
    let db = Db::open_sync(options).expect("persistent db reopens");

    let before = db.stats();
    let started = Instant::now();
    match scope {
        CompactionScope::LocalMaintenance => {
            db.run_maintenance_with_budget_sync(MaintenanceBudget::unbounded())
                .expect("local maintenance compaction succeeds");
        }
        CompactionScope::BroadManual => {
            db.compact_range_sync(KeyRange::all())
                .expect("broad compaction succeeds");
        }
    }
    let after = db.stats();
    push_maintenance_write_amp_results(
        results,
        label,
        &before,
        &after,
        duration_micros(started.elapsed()),
    );
    push_compaction_scope_after_read_diagnostics(results, label, &db);
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn push_compaction_scope_after_read_diagnostics(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    db: &Db,
) {
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let keys = localized_point_read_keys(ROWS, OPS);
    let before = db.stats();
    let checksum = sequential_point_batch_checksum(&bucket, &keys);
    assert!(checksum > 0, "comparison workload must read existing rows");
    let after = db.stats();
    let mut diagnostics = ColdReadDiagnostics::default();
    diagnostics.record_delta(&before, &after);
    diagnostics.push_read_path_results(results, labelled(label, "after read"));
}

pub(super) fn prepare_disjoint_l0_compaction_workload(dir: &Path) {
    let mut options = benchmark_persistent_options(dir);
    options.background_worker_count = 0;
    options.max_l0_files = 64;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for chunk in 0..4 {
        for index in 0..(ROWS / 4) {
            let row = chunk * (ROWS / 4) + index;
            bucket.put_sync(key(row), value(row)).expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
    }
    drop(db);
}

pub(super) fn extend_guard_multi_table_compaction_diagnostic(results: &mut Vec<BenchResult>) {
    let label = "write amp guard multi-table compaction";
    let dir = temp_dir(label);
    let db = prepare_guard_multi_table_compaction_workload(&dir);
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let read_keys = localized_point_read_keys(ROWS, OPS);

    let before_read_stats = db.stats();
    let checksum = sequential_point_batch_checksum(&bucket, &read_keys);
    assert!(checksum > 0, "guard multi-table workload must read rows");
    let after_before_read_stats = db.stats();
    let mut before_read_diagnostics = ColdReadDiagnostics::default();
    before_read_diagnostics.record_delta(&before_read_stats, &after_before_read_stats);
    before_read_diagnostics.push_read_path_results(results, labelled(label, "before read"));

    let compaction_before = db.stats();
    let broad_input_tables = bench_level_table_count(&compaction_before, 1);
    let broad_input_bytes = bench_level_table_bytes(&compaction_before, 1);
    assert_eq!(
        broad_input_tables, 4,
        "diagnostic starts with four L1 tables"
    );
    assert_eq!(
        compaction_before.l0_tables, 0,
        "diagnostic starts without L0"
    );

    let started = Instant::now();
    db.compact_range_sync(KeyRange::all())
        .expect("guard-local multi-table compaction succeeds");
    let compaction_after = db.stats();
    let wall_micros = duration_micros(started.elapsed());
    let actual_input_tables = compaction_after
        .compaction_input_tables
        .saturating_sub(compaction_before.compaction_input_tables);
    let actual_input_bytes = compaction_after
        .compaction_input_bytes
        .saturating_sub(compaction_before.compaction_input_bytes);
    let actual_output_bytes = compaction_after
        .compaction_output_bytes
        .saturating_sub(compaction_before.compaction_output_bytes);
    assert_eq!(
        actual_input_tables, 1,
        "guard-local fallback should choose one same-level input"
    );
    assert!(
        actual_input_bytes < broad_input_bytes,
        "guard-local input bytes should be below the broad same-level estimate"
    );
    assert!(
        actual_output_bytes < broad_input_bytes,
        "guard-local output bytes should be below the broad same-level estimate"
    );

    push_maintenance_write_amp_results(
        results,
        label,
        &compaction_before,
        &compaction_after,
        wall_micros,
    );
    results.push(BenchResult::diagnostic(
        labelled(label, "estimated broad input tables"),
        usize_to_u64(broad_input_tables),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "estimated broad input bytes"),
        broad_input_bytes,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "estimated broad output bytes"),
        broad_input_bytes,
    ));

    let before_after_read_stats = db.stats();
    let checksum = sequential_point_batch_checksum(&bucket, &read_keys);
    assert!(checksum > 0, "guard multi-table workload must read rows");
    let after_after_read_stats = db.stats();
    let mut after_read_diagnostics = ColdReadDiagnostics::default();
    after_read_diagnostics.record_delta(&before_after_read_stats, &after_after_read_stats);
    after_read_diagnostics.push_read_path_results(results, labelled(label, "after read"));
    assert!(
        after_read_diagnostics.table_probes <= before_read_diagnostics.table_probes,
        "guard-local compaction must not increase point table probes"
    );

    drop(bucket);
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn prepare_guard_multi_table_compaction_workload(dir: &Path) -> Db {
    let mut options = benchmark_persistent_options(dir);
    options.background_worker_count = 0;
    options.max_l0_files = 64;
    options.target_table_bytes = usize::MAX / 4;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for chunk in 0..4 {
        for index in 0..(ROWS / 4) {
            let row = chunk * (ROWS / 4) + index;
            bucket.put_sync(key(row), value(row)).expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
        db.compact_range_sync(KeyRange::all())
            .expect("move flushed table to L1");
    }
    drop(bucket);
    db
}
