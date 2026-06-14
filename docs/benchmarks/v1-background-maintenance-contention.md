# V1 Background Maintenance Contention

This note records Phase 185. The phase added benchmark diagnostics that measure
foreground reads and writes while flush and compaction pressure are active.

## What Changed

The benchmark harness now reports two diagnostic workloads:

- `foreground maintenance contention diagnostic`: persistent local-file writes
  with `background_worker_count = 0`.
- `background maintenance contention diagnostic`: the same workload with
  `background_worker_count = 1`.

Both workloads pre-load readable table data, then run a reader thread and a
writer thread at the same time. The writer uses a small memtable target and low
L0 threshold to trigger flush and compaction pressure. The diagnostics record
foreground read/write wall time, cooperative maintenance yields, budget
exhaustions, compaction counters, and storage operation counters.

## Evidence

Validation:

```text
cargo check -q --benches
cargo clippy --bench v1_bench -- -D warnings
cargo fmt --check
TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
```

The grouped benchmark run reported:

| Row | Median value |
| --- | ---: |
| foreground maintenance contention diagnostic read wall micros | 1352 |
| foreground maintenance contention diagnostic write wall micros | 1353150 |
| foreground maintenance contention diagnostic cooperative yields | 0 |
| foreground maintenance contention diagnostic budget exhaustions | 0 |
| foreground maintenance contention diagnostic compaction runs | 46 |
| foreground maintenance contention diagnostic storage publish manifest requests | 69 |
| foreground maintenance contention diagnostic storage persist requests | 92 |
| foreground maintenance contention diagnostic storage sync directory requests | 23 |
| background maintenance contention diagnostic read wall micros | 1445 |
| background maintenance contention diagnostic write wall micros | 2148083 |
| background maintenance contention diagnostic cooperative yields | 129 |
| background maintenance contention diagnostic budget exhaustions | 47 |
| background maintenance contention diagnostic compaction runs | 43 |
| background maintenance contention diagnostic storage publish manifest requests | 88 |
| background maintenance contention diagnostic storage persist requests | 180 |
| background maintenance contention diagnostic storage sync directory requests | 45 |

No cleanup warnings were emitted after the benchmark handle lifetime was fixed.

## Interpretation

Foreground point reads are not the first bottleneck in this pressure model; the
read wall time stayed near the existing hot-read diagnostic range.

The background-worker case is worse for foreground write wall time because it
does more small storage maintenance turns: more manifest publishes, persists,
and directory syncs, plus many cooperative foreground waits and budget
exhaustions. The next optimization should tune background-worker maintenance
budgeting and foreground backpressure waiting before changing compaction
selection or read-path behavior.
