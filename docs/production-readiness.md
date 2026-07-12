# Production Readiness Evidence

Trine separates production evidence into correctness, performance,
cross-platform runtime behavior, and operating history. A green unit-test run
is necessary, but it does not by itself prove every production boundary.

## Bounded Performance Gate

The `production-gate` benchmark profile runs ten representative rows instead of
the full diagnostic suite:

- single-key and batch writes;
- random point reads;
- bounded range and prefix scans;
- WAL replay and cold read-only open;
- flush and compaction;
- separated large values.

Record a grouped local report with:

```text
TRINE_BENCH_PROFILE=production-gate TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
```

Wall time from different machines is not directly comparable. The production
workflow checks out the base and candidate revisions on one GitHub runner, runs
both profiles, and calls:

```text
python3 scripts/compare_benchmarks.py \
  --baseline baseline.csv \
  --current current.csv \
  --output comparison.md \
  --max-regression-percent 20 \
  --absolute-noise-us 500
```

A row fails only when its median slowdown exceeds both the relative limit and
the absolute noise floor. Raw CSV files and the comparison report are retained
as workflow artifacts. The limits are broad regression alarms for shared CI
runners, not application latency objectives. Applications should still measure
their own key/value sizes, concurrency, durability mode, storage device, data
size, and latency percentiles.

## Forced-Process-Exit Recovery

Run the repeated recovery scenario with:

```text
cargo test -q --test production_maturity forced_process_exit_recovery \
  -- --ignored --nocapture --test-threads=1
```

The parent test launches a child test process. The child opens a persistent
database, performs confirmed writes, and calls `process::exit` without closing
the database or running Rust destructors. The parent reopens the database after
every round and verifies every write from the current and earlier rounds.

Configure the number of rounds with `TRINE_MATURITY_CRASH_ROUNDS`. This proves
the application-process exit and stale writer-lease recovery boundary. It does
not emulate sudden power loss, kernel failure, controller-cache loss, or a
damaged filesystem.

## Concurrent Mixed-Load Soak

Run the local default workload with:

```text
cargo test -q --test production_maturity concurrent_mixed_load_soak_reopens_cleanly \
  -- --ignored --nocapture --test-threads=1
```

The workload uses disjoint key sets per writer and a deterministic model. It
combines concurrent puts, deletes, point reads, snapshots, periodic WAL sync,
cooperative maintenance, background maintenance, flush, compaction, close, and
full reopen verification.

Configuration variables:

| variable | default | meaning |
| --- | ---: | --- |
| `TRINE_MATURITY_OPERATIONS` | `10000` | Total mixed operations across writers. |
| `TRINE_MATURITY_THREADS` | `4` | Concurrent writer count. |
| `TRINE_MATURITY_SEED` | fixed | Reproducible random seed printed on failure. |
| `TRINE_MATURITY_BLOB_GC` | `true` | Keep Blob GC in the maintenance workload. |
| `TRINE_MATURITY_BACKGROUND_WORKERS` | `2` | Background maintenance worker count. |
| `TRINE_MATURITY_COOPERATIVE_MAINTENANCE` | `true` | Run a concurrent cooperative maintenance loop. |
| `TRINE_MATURITY_MAX_L0_FILES` | `4` | L0 pressure threshold used by the soak. |
| `TRINE_MATURITY_REPORT` | unset | Optional JSONL report path. |

Scheduled CI raises the operation count and stores one JSONL report per
operating system. Throughput in that report is observational. It is not a
cross-machine performance comparison.

## Runtime Matrix

The production-evidence workflow runs on Linux, macOS, and Windows. Every
runner executes:

- forced-process-exit recovery;
- concurrent mixed-load soak and reopen verification;
- portable `platform-io` tests;
- native-first `platform-io-native` tests.

The normal CI workflow also keeps dedicated Windows and macOS platform-I/O
jobs. Browser OPFS runs separately in real Chrome/ChromeDriver, and WASI
persistence runs under Wasmtime.

## Evidence Still Needed From Deployments

Repository automation cannot establish fleet maturity on its own. Before Trine
is the only copy of irreplaceable data, a deployment should retain evidence for:

- workload-specific P50/P95/P99 latency, throughput, memory, disk growth,
  write amplification, and reopen time;
- a longer soak on the actual filesystem and storage hardware;
- process termination and restart under the real service supervisor;
- low-disk-space and disk-full behavior;
- backup and restore while respecting Trine's lack of online backup semantics;
- alerts derived from `DbStats` and host filesystem capacity;
- upgrade, rollback, and storage-format compatibility;
- incident history and recovery time.

Sudden-power-loss testing requires a host or device harness that can cut power
outside the database process. Object-storage deployments should also run the
ignored live provider suite with their own adapter, region, credentials,
latency, and request-cost limits.
