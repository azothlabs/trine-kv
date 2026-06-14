# V1 Background Maintenance Budget Tuning

This note records Phase 186. Phase 185 showed that the one-worker background
maintenance workload had worse foreground write latency because it split flush
and compaction pressure into too many small storage-maintenance turns.

## What Changed

Background maintenance now uses an internal pressure-sized budget instead of
the public cooperative single-unit budget. The public
`MaintenanceBudget::default()` behavior is unchanged.

Foreground write backpressure now tries to clear pressure in the calling writer
before waking and waiting on a background worker. The worker is still used when
another maintenance turn already owns the guard.

Post-commit background flush admission now waits for a useful batch threshold
instead of waking a worker for every frozen memtable. With the default
`max_immutable_memtables = 4`, background flushes are requested once a bucket
has at least 3 immutable memtables. With tiny pressure thresholds such as
`max_immutable_memtables = 2`, pressure maintenance stays foreground-owned so
the worker does not split a two-memtable batch into two manifest publishes.

## Evidence

Validation:

```text
cargo fmt --check
cargo test -q background_maintenance_budget_tracks_pressure_thresholds --lib
cargo test -q background_flush_request_threshold_keeps_tiny_pressure_foreground --lib
cargo test -q write_pressure_maintenance_reports_foreground_progress --lib
cargo test -q persistent_background_workers_flush_and_compact_pressure --lib
cargo check -q --benches
cargo test -q --lib
cargo test -q --all-features
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
```

The grouped benchmark run reported:

| Row | Phase 185 median | Phase 186 median |
| --- | ---: | ---: |
| background maintenance contention diagnostic read wall micros | 1445 | 2227 |
| background maintenance contention diagnostic write wall micros | 2148083 | 1491161 |
| background maintenance contention diagnostic cooperative yields | 129 | 0 |
| background maintenance contention diagnostic budget exhaustions | 47 | 0 |
| background maintenance contention diagnostic compaction runs | 43 | 46 |
| background maintenance contention diagnostic storage publish manifest requests | 88 | 69 |
| background maintenance contention diagnostic storage persist requests | 180 | 92 |
| background maintenance contention diagnostic storage sync directory requests | 45 | 23 |

The foreground row in the same Phase 186 run reported:

| Row | Median value |
| --- | ---: |
| foreground maintenance contention diagnostic read wall micros | 1300 |
| foreground maintenance contention diagnostic write wall micros | 1338208 |
| foreground maintenance contention diagnostic storage publish manifest requests | 69 |
| foreground maintenance contention diagnostic storage persist requests | 92 |
| foreground maintenance contention diagnostic storage sync directory requests | 23 |

## Interpretation

The retained change removes the redundant storage-maintenance turns in the
background-worker contention row: manifest publishes, persists, and directory
syncs now match the foreground-only row. Foreground write wall time improved by
about 31% versus Phase 185, while the remaining gap to the foreground-only row
is small enough to leave for later broader write-path work.

The read-wall median is noisier in this run, but the workload still stays in the
low-millisecond range and the phase target was the write-pressure regression.
