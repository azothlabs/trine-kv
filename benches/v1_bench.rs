use std::{
    collections::BTreeMap,
    env, fs,
    hint::black_box,
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use trine_kv::{
    BlobGcRatio, BlobLevelMergePolicy, BucketOptions, CompactionTrigger, Db, DbOptions,
    DurabilityMode, FilterPolicy, IndexSearchPolicy, KeyRange, MaintenanceBudget, PrefixExtractor,
    PrefixFilterPolicy, RuntimeOptions, TransactionOptions, WalShardPolicy, WriteBatch,
    WriteOptions, search,
};

const ROWS: usize = 1_024;
const OPS: usize = 2_048;
const POINT_READ_BATCH: usize = 4;
const LOCALIZED_POINT_READ_BATCH: usize = 16;
const WRITE_DIAGNOSTIC_OPS: usize = 256;
const BACKGROUND_CONTENTION_ROWS: usize = 256;
const BACKGROUND_CONTENTION_OPS: usize = 512;
const LARGE_ROWS: usize = 128;
const LARGE_OPS: usize = 256;
const LARGE_VALUE_BYTES: usize = 16 * 1024;
const WAL_REPLAY_DIAGNOSTIC_RUNS: usize = 32;
const BENCH_RUNS_ENV: &str = "TRINE_BENCH_RUNS";
const BENCH_PROFILE_ENV: &str = "TRINE_BENCH_PROFILE";
const PRODUCTION_GATE_PROFILE: &str = "production-gate";

fn main() {
    let runs = benchmark_runs();
    println!("trine-kv v1 benchmark");
    println!("rows={ROWS} ops={OPS}");
    println!("profile={}", benchmark_profile());

    if runs == 1 {
        print_single_run(run_benchmarks());
        return;
    }

    print_multi_run_summary(runs);
}

fn run_benchmarks() -> Vec<BenchResult> {
    if benchmark_profile() == PRODUCTION_GATE_PROFILE {
        return run_production_gate_benchmarks();
    }

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
        bench_persistent_missing_sequential_point_batch(),
        bench_persistent_missing_batched_point_read(),
        bench_persistent_bounded_missing_sequential_point_batch(),
        bench_persistent_bounded_missing_batched_point_read(),
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
    extend_missing_point_batch_diagnostics(&mut results);
    extend_bounded_missing_point_batch_diagnostics(&mut results);
    results.push(bench_snapshot_read_under_writes());
    results.push(bench_transaction_commit());
    results.push(bench_transaction_conflict());
    results.push(bench_wal_replay());
    results.push(bench_wal_replay_read_only());
    extend_wal_replay_diagnostics(&mut results);
    results.extend(bench_persistent_write_path());
    extend_persistent_write_path_diagnostics(&mut results);
    results.push(bench_flush_throughput());
    results.push(bench_compaction_throughput());
    results.push(bench_large_inline_values());
    results.push(bench_separated_blob_values());
    results.push(bench_blob_point_read());
    results.push(bench_blob_range_scan());
    results.push(bench_blob_range_lazy_keys());
    results.push(bench_blob_gc_rewrite());
    results.push(bench_blob_level_merge());
    extend_maintenance_write_amplification_diagnostics(&mut results);
    extend_background_maintenance_contention_diagnostics(&mut results);
    extend_layered_filter_fpr_diagnostic(&mut results);
    extend_group_commit_diagnostic(&mut results);
    extend_tombstone_scan_waste_diagnostic(&mut results);
    extend_read_tail_latency_diagnostic(&mut results);
    results.push(bench_block_cache_warm_read());
    results.push(bench_block_cache_random_block_read());
    extend_block_cache_decode_diagnostics(&mut results);
    results.push(bench_cold_table_read());
    results.push(bench_cold_table_read_only());
    results.extend(bench_cold_table_open_wall_diagnostics());
    results.extend(bench_read_pruning_diagnostics());
    results.extend(bench_runtime_block_decode_reads());
    results.extend(bench_index_seek_policies());
    results.push(bench_long_shared_prefix_get());
    results.extend(bench_iterator_advance_to());
    results.extend(bench_codec_comparison());
    results
}

fn run_production_gate_benchmarks() -> Vec<BenchResult> {
    vec![
        bench_single_key_put(),
        bench_batch_write(),
        bench_random_get(),
        bench_bounded_range_scan(),
        bench_prefix_scan(),
        bench_wal_replay(),
        bench_flush_throughput(),
        bench_compaction_throughput(),
        bench_separated_blob_values(),
        bench_cold_table_read_only(),
    ]
}

fn benchmark_profile() -> String {
    env::var(BENCH_PROFILE_ENV).unwrap_or_else(|_| "full".to_owned())
}

fn benchmark_runs() -> usize {
    env::var(BENCH_RUNS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|runs| *runs > 0)
        .unwrap_or(1)
}

fn print_single_run(results: Vec<BenchResult>) {
    println!("name,iterations,elapsed_us,units_per_sec,checksum");

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

fn print_multi_run_summary(runs: usize) {
    let mut summaries = BTreeMap::<(&'static str, &'static str), BenchSummary>::new();
    for run_index in 0..runs {
        eprintln!("benchmark run {}/{}", run_index + 1, runs);
        for result in run_benchmarks() {
            let group = benchmark_group(result.name);
            summaries
                .entry((group, result.name))
                .or_insert_with(|| BenchSummary::new(group, result.name, result.iterations))
                .record(&result);
        }
    }

    println!(
        "{}",
        concat!(
            "group,name,runs,iterations,elapsed_us_min,elapsed_us_median,",
            "elapsed_us_max,units_per_sec_median,value_min,value_median,value_max"
        )
    );
    for summary in summaries.values() {
        let (elapsed_min, elapsed_median, elapsed_max) = summary.elapsed_stats();
        let units_median = summary.units_per_second_median();
        let (checksum_min, checksum_median, checksum_max) = summary.checksum_stats();
        println!(
            "{},{},{},{},{},{},{},{},{},{},{}",
            summary.group,
            summary.name,
            summary.runs(),
            summary.iterations,
            elapsed_min,
            elapsed_median,
            elapsed_max,
            units_median,
            checksum_min,
            checksum_median,
            checksum_max
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

struct BenchSummary {
    group: &'static str,
    name: &'static str,
    iterations: usize,
    elapsed_micros: Vec<u64>,
    units_per_second: Vec<u64>,
    checksums: Vec<u64>,
}

impl BenchSummary {
    fn new(group: &'static str, name: &'static str, iterations: usize) -> Self {
        Self {
            group,
            name,
            iterations,
            elapsed_micros: Vec::new(),
            units_per_second: Vec::new(),
            checksums: Vec::new(),
        }
    }

    fn record(&mut self, result: &BenchResult) {
        assert_eq!(
            self.iterations, result.iterations,
            "benchmark iterations changed across runs for {}",
            self.name
        );
        self.elapsed_micros.push(duration_micros(result.elapsed));
        self.units_per_second.push(result.units_per_second());
        self.checksums.push(result.checksum);
    }

    fn runs(&self) -> usize {
        self.elapsed_micros.len()
    }

    fn elapsed_stats(&self) -> (u64, u64, u64) {
        value_stats(&self.elapsed_micros)
    }

    fn units_per_second_median(&self) -> u64 {
        value_median(&self.units_per_second)
    }

    fn checksum_stats(&self) -> (u64, u64, u64) {
        value_stats(&self.checksums)
    }
}

fn value_stats(values: &[u64]) -> (u64, u64, u64) {
    assert!(
        !values.is_empty(),
        "benchmark summary needs at least one run"
    );
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    (
        sorted[0],
        sorted[sorted.len() / 2],
        sorted[sorted.len() - 1],
    )
}

fn value_median(values: &[u64]) -> u64 {
    value_stats(values).1
}

fn benchmark_group(name: &str) -> &'static str {
    if name.contains("diagnostic")
        || name.starts_with("read pruning")
        || name.starts_with("WAL replay writable open")
        || name.starts_with("WAL replay read-only open")
    {
        "diagnostics"
    } else if name.contains("WAL replay") || name.contains("cold table") {
        "startup-recovery"
    } else if name.contains("blob")
        || name.contains("large inline")
        || name.contains("separated blob")
    {
        "blob-large-values"
    } else if name.contains("compaction") {
        "compaction"
    } else if name.contains("put")
        || name == "batch write"
        || name.starts_with("persistent batch write")
        || name.starts_with("write path")
        || name.contains("flush throughput")
    {
        "writes-flush"
    } else if name.contains("range scan") || name.contains("prefix scan") {
        "scans"
    } else if name.contains("transaction") || name.contains("snapshot") {
        "mvcc-transactions"
    } else if name.contains("cache") || name.contains("block decode") {
        "cache-decode"
    } else if name.contains("index seek") || name.contains("shared-prefix") {
        "search-policy"
    } else if name.contains("iterator") {
        "iterator"
    } else if name.contains("codec") {
        "codec"
    } else if name.contains("get") || name.contains("point") || name.contains("missing") {
        "point-reads"
    } else {
        "misc"
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

#[path = "v1_bench/blob_maintenance.rs"]
mod blob_maintenance;
#[path = "v1_bench/bulk_workloads.rs"]
mod bulk_workloads;
#[path = "v1_bench/cache.rs"]
mod cache;
#[path = "v1_bench/cold_reads.rs"]
mod cold_reads;
#[path = "v1_bench/fixtures.rs"]
mod fixtures;
#[path = "v1_bench/maintenance.rs"]
mod maintenance;
#[path = "v1_bench/point_reads.rs"]
mod point_reads;
#[path = "v1_bench/read_tail.rs"]
mod read_tail;
#[path = "v1_bench/runtime_codec.rs"]
mod runtime_codec;
#[path = "v1_bench/transactions_wal.rs"]
mod transactions_wal;
#[path = "v1_bench/write_path.rs"]
mod write_path;
#[path = "v1_bench/writes.rs"]
mod writes;

use blob_maintenance::{
    bench_level_table_bytes, bench_level_table_count,
    extend_blob_gc_write_amplification_diagnostic,
    extend_blob_level_merge_write_amplification_diagnostic, push_maintenance_write_amp_results,
    usize_to_u64,
};
use bulk_workloads::{
    bench_blob_gc_rewrite, bench_blob_level_merge, bench_blob_point_read,
    bench_blob_range_lazy_keys, bench_blob_range_scan, bench_compaction_throughput,
    bench_flush_throughput, bench_large_inline_values, bench_separated_blob_values,
};
use cache::{
    bench_block_cache_random_block_read, bench_block_cache_warm_read,
    extend_block_cache_decode_diagnostics,
};
use cold_reads::{
    ColdReadDiagnostics, bench_cold_table_open_wall_diagnostics, bench_cold_table_read,
    bench_cold_table_read_only, bench_read_pruning_diagnostics,
};
use fixtures::{
    batched_point_read_checksum, bounded_missing_point_read_keys, cleanup_dir,
    flushed_persistent_db, key, labelled, labelled_level, labelled_trigger, labelled3,
    large_blob_db, large_blob_options, large_value, localized_point_read_keys,
    long_shared_prefix_key, missing_point_read_keys, point_read_keys, populated_active_memtable_db,
    populated_delta_memory_db, populated_memory_db, populated_prefix_db, prefix_key,
    prefix_options, prefix_scan_checksum, random_get_checksum, range_scan_checksum, repeated_bytes,
    seed_index, sequential_point_batch_checksum, temp_dir, value, xorshift,
};
use maintenance::{
    extend_background_maintenance_contention_diagnostics, extend_flush_wall_diagnostics,
    extend_maintenance_write_amplification_diagnostics,
};
use point_reads::{
    bench_active_memtable_random_get, bench_active_memtable_range_scan, bench_bounded_range_scan,
    bench_delta_backed_missing_get, bench_delta_backed_random_get, bench_delta_backed_range_scan,
    bench_memory_batched_point_read, bench_memory_sequential_point_batch, bench_missing_get,
    bench_persistent_batched_point_read, bench_persistent_bounded_missing_batched_point_read,
    bench_persistent_bounded_missing_sequential_point_batch,
    bench_persistent_localized_batched_point_read,
    bench_persistent_localized_sequential_point_batch, bench_persistent_missing_batched_point_read,
    bench_persistent_missing_sequential_point_batch, bench_persistent_sequential_point_batch,
    bench_prefix_partition_scans, bench_prefix_scan, bench_random_get,
    extend_bounded_missing_point_batch_diagnostics, extend_localized_point_batch_diagnostics,
    extend_missing_point_batch_diagnostics,
};
use read_tail::{
    extend_group_commit_diagnostic, extend_layered_filter_fpr_diagnostic,
    extend_read_tail_latency_diagnostic, extend_tombstone_scan_waste_diagnostic,
};
use runtime_codec::{
    bench_codec_comparison, bench_index_seek_policies, bench_iterator_advance_to,
    bench_long_shared_prefix_get, bench_runtime_block_decode_reads,
};
use transactions_wal::{
    bench_snapshot_read_under_writes, bench_transaction_commit, bench_transaction_conflict,
    bench_wal_replay, bench_wal_replay_read_only, extend_persistent_write_path_diagnostics,
    extend_wal_replay_diagnostics,
};
use write_path::{
    WritePathDiagnostics, extend_wal_replay_open_diagnostics, populate_wal_replay_dir,
};
use writes::{
    bench_batch_write, bench_persistent_write_path, bench_single_key_put,
    benchmark_persistent_options,
};
