use super::{
    BenchResult, BlobGcRatio, BlobLevelMergePolicy, BucketOptions, CompactionTrigger, Db, Instant,
    KeyRange, LARGE_ROWS, Path, WritePathDiagnostics, benchmark_persistent_options, cleanup_dir,
    duration_micros, key, labelled, labelled_level, labelled_trigger, large_blob_options,
    large_value, temp_dir,
};

pub(super) fn extend_blob_gc_write_amplification_diagnostic(results: &mut Vec<BenchResult>) {
    let dir = temp_dir("write-amp-blob-gc-diagnostic");
    let (db, bucket) = open_blob_maintenance_db(&dir, BlobLevelMergePolicy::Disabled, true);
    prepare_blob_overwrite_workload(&db, &bucket);

    let before = db.stats();
    let started = Instant::now();
    db.compact_range_sync(KeyRange::all())
        .expect("blob GC compaction succeeds");
    let after = db.stats();
    push_maintenance_write_amp_results(
        results,
        "write amp blob GC diagnostic",
        &before,
        &after,
        duration_micros(started.elapsed()),
    );
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn extend_blob_level_merge_write_amplification_diagnostic(
    results: &mut Vec<BenchResult>,
) {
    let dir = temp_dir("write-amp-blob-level-merge-diagnostic");
    let (db, bucket) = open_blob_maintenance_db(&dir, BlobLevelMergePolicy::Always, false);
    prepare_blob_overwrite_workload(&db, &bucket);

    let before = db.stats();
    let started = Instant::now();
    db.compact_range_sync(KeyRange::all())
        .expect("blob level merge compaction succeeds");
    let after = db.stats();
    push_maintenance_write_amp_results(
        results,
        "write amp blob level merge diagnostic",
        &before,
        &after,
        duration_micros(started.elapsed()),
    );
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn open_blob_maintenance_db(
    dir: &Path,
    blob_level_merge_policy: BlobLevelMergePolicy,
    blob_gc_enabled: bool,
) -> (Db, trine_kv::Bucket) {
    let mut options = benchmark_persistent_options(dir);
    options.blob_gc_enabled = blob_gc_enabled;
    options.blob_gc_min_file_bytes = 1;
    options.blob_gc_discardable_ratio = BlobGcRatio::from_millionths(300_000);
    options.default_bucket_options = BucketOptions {
        blob_level_merge_policy,
        ..large_blob_options()
    };
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    (db, bucket)
}

pub(super) fn prepare_blob_overwrite_workload(db: &Db, bucket: &trine_kv::Bucket) {
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
}

pub(super) fn push_maintenance_write_amp_results(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    before: &trine_kv::DbStats,
    after: &trine_kv::DbStats,
    wall_micros: u64,
) {
    let mut diagnostics = WritePathDiagnostics::default();
    diagnostics.record_delta(before, after);
    results.push(BenchResult::diagnostic(
        labelled(label, "wall micros"),
        wall_micros,
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
        labelled(label, "compaction input bytes"),
        after
            .compaction_input_bytes
            .saturating_sub(before.compaction_input_bytes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "compaction output bytes"),
        after
            .compaction_output_bytes
            .saturating_sub(before.compaction_output_bytes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "compaction rewritten bytes"),
        after
            .compaction_input_bytes
            .saturating_sub(before.compaction_input_bytes)
            .saturating_add(
                after
                    .compaction_output_bytes
                    .saturating_sub(before.compaction_output_bytes),
            ),
    ));
    push_compaction_level_diagnostics(results, label, before, after);
    push_compaction_trigger_diagnostics(results, label, before, after);
    results.push(BenchResult::diagnostic(
        labelled(label, "blob GC runs"),
        after.blob_gc_runs.saturating_sub(before.blob_gc_runs),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "blob GC input bytes"),
        after
            .blob_gc_input_bytes
            .saturating_sub(before.blob_gc_input_bytes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "blob GC output bytes"),
        after
            .blob_gc_output_bytes
            .saturating_sub(before.blob_gc_output_bytes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "blob GC discarded bytes"),
        after
            .blob_gc_discarded_bytes
            .saturating_sub(before.blob_gc_discarded_bytes),
    ));
    diagnostics.push_results(results, label);
}

pub(super) fn push_compaction_level_diagnostics(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    before: &trine_kv::DbStats,
    after: &trine_kv::DbStats,
) {
    for level in &after.compaction_levels {
        let before_level = before
            .compaction_levels
            .iter()
            .find(|before_level| before_level.level == level.level);
        let input_tables = level
            .input_tables
            .saturating_sub(before_level.map_or(0, |level| level.input_tables));
        let output_tables = level
            .output_tables
            .saturating_sub(before_level.map_or(0, |level| level.output_tables));
        let input_bytes = level
            .input_bytes
            .saturating_sub(before_level.map_or(0, |level| level.input_bytes));
        let output_bytes = level
            .output_bytes
            .saturating_sub(before_level.map_or(0, |level| level.output_bytes));
        if input_tables == 0 && output_tables == 0 && input_bytes == 0 && output_bytes == 0 {
            continue;
        }
        push_compaction_level_rows(
            results,
            label,
            CompactionLevelDiagnostic {
                level: level.level,
                input_tables,
                output_tables,
                input_bytes,
                output_bytes,
                rewritten_bytes: input_bytes.saturating_add(output_bytes),
            },
        );
    }
}

pub(super) fn push_compaction_trigger_diagnostics(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    before: &trine_kv::DbStats,
    after: &trine_kv::DbStats,
) {
    for trigger in &after.compaction_triggers {
        let before_trigger = before
            .compaction_triggers
            .iter()
            .find(|before_trigger| before_trigger.trigger == trigger.trigger);
        let runs = trigger
            .runs
            .saturating_sub(before_trigger.map_or(0, |trigger| trigger.runs));
        let input_tables = trigger
            .input_tables
            .saturating_sub(before_trigger.map_or(0, |trigger| trigger.input_tables));
        let output_tables = trigger
            .output_tables
            .saturating_sub(before_trigger.map_or(0, |trigger| trigger.output_tables));
        let input_bytes = trigger
            .input_bytes
            .saturating_sub(before_trigger.map_or(0, |trigger| trigger.input_bytes));
        let output_bytes = trigger
            .output_bytes
            .saturating_sub(before_trigger.map_or(0, |trigger| trigger.output_bytes));
        if runs == 0 && input_tables == 0 && output_tables == 0 {
            continue;
        }
        push_compaction_trigger_rows(
            results,
            label,
            CompactionTriggerDiagnostic {
                trigger: trigger.trigger,
                runs,
                input_tables,
                output_tables,
                input_bytes,
                output_bytes,
                rewritten_bytes: input_bytes.saturating_add(output_bytes),
            },
        );
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CompactionTriggerDiagnostic {
    trigger: CompactionTrigger,
    runs: u64,
    input_tables: u64,
    output_tables: u64,
    input_bytes: u64,
    output_bytes: u64,
    rewritten_bytes: u64,
}

pub(super) fn push_compaction_trigger_rows(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    diagnostic: CompactionTriggerDiagnostic,
) {
    results.push(BenchResult::diagnostic(
        labelled_trigger(label, diagnostic.trigger, "compaction runs"),
        diagnostic.runs,
    ));
    results.push(BenchResult::diagnostic(
        labelled_trigger(label, diagnostic.trigger, "compaction input tables"),
        diagnostic.input_tables,
    ));
    results.push(BenchResult::diagnostic(
        labelled_trigger(label, diagnostic.trigger, "compaction output tables"),
        diagnostic.output_tables,
    ));
    results.push(BenchResult::diagnostic(
        labelled_trigger(label, diagnostic.trigger, "compaction input bytes"),
        diagnostic.input_bytes,
    ));
    results.push(BenchResult::diagnostic(
        labelled_trigger(label, diagnostic.trigger, "compaction output bytes"),
        diagnostic.output_bytes,
    ));
    results.push(BenchResult::diagnostic(
        labelled_trigger(label, diagnostic.trigger, "compaction rewritten bytes"),
        diagnostic.rewritten_bytes,
    ));
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CompactionLevelDiagnostic {
    level: u32,
    input_tables: u64,
    output_tables: u64,
    input_bytes: u64,
    output_bytes: u64,
    rewritten_bytes: u64,
}

pub(super) fn push_compaction_level_rows(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    diagnostic: CompactionLevelDiagnostic,
) {
    results.push(BenchResult::diagnostic(
        labelled_level(label, diagnostic.level, "compaction input tables"),
        diagnostic.input_tables,
    ));
    results.push(BenchResult::diagnostic(
        labelled_level(label, diagnostic.level, "compaction output tables"),
        diagnostic.output_tables,
    ));
    results.push(BenchResult::diagnostic(
        labelled_level(label, diagnostic.level, "compaction input bytes"),
        diagnostic.input_bytes,
    ));
    results.push(BenchResult::diagnostic(
        labelled_level(label, diagnostic.level, "compaction output bytes"),
        diagnostic.output_bytes,
    ));
    results.push(BenchResult::diagnostic(
        labelled_level(label, diagnostic.level, "compaction rewritten bytes"),
        diagnostic.rewritten_bytes,
    ));
}

pub(super) fn bench_level_table_count(stats: &trine_kv::DbStats, level: u32) -> usize {
    stats
        .level_tables
        .iter()
        .find(|level_stats| level_stats.level == level)
        .map_or(0, |level_stats| level_stats.tables)
}

pub(super) fn bench_level_table_bytes(stats: &trine_kv::DbStats, level: u32) -> u64 {
    stats
        .level_tables
        .iter()
        .find(|level_stats| level_stats.level == level)
        .map_or(0, |level_stats| level_stats.bytes)
}

pub(super) fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
