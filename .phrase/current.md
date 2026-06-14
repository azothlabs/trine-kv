# Current Phase

## Status

Complete

## Goal

Measure foreground read/write latency while background flush, compaction, and
maintenance pressure are active, then use that evidence to classify the next
worker-budget and backpressure tuning phase.

## Scope

- Benchmark rows for foreground point reads and writes under maintenance
  pressure.
- Diagnostics for background worker participation, cooperative foreground
  maintenance, budget exhaustion, compaction runs, and storage operation cost.
- Native persistent local-file workloads using existing sync public APIs.
- Evidence sufficient to classify the next blocker before changing engine
  behavior.

## Out Of Scope

- Storage format changes.
- Platform-io backend changes.
- Public API changes.
- Object-store, browser, or WASI maintenance behavior changes.
- New compaction-picking policy unless diagnostics prove it is the current
  blocker.
- Publishing, tagging, pushing, or release workflow changes.

## Acceptance Gate

- Benchmarks report foreground read and write wall time while maintenance
  pressure is present.
- Diagnostics distinguish foreground cooperative maintenance from background
  worker progress.
- Diagnostics include compaction/storage counters needed to explain whether
  latency comes from locks, storage queueing, or maintenance budgeting.
- Focused benchmark run records current behavior.
- Full lib tests, all-feature tests, strict clippy, formatting, and diff
  whitespace checks pass before the phase is closed.

## Active Task Slice

```text
task810 [x] goal:add concurrent maintenance diagnostics | scope:benches/v1_bench.rs | verify:TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
task811 [x] goal:classify first maintenance contention blocker | scope:docs/benchmarks .phrase/evidence.md | verify:benchmark evidence review
```

## Evidence

- Phase 184 completed the remaining LZ4 feature-policy work and recommended
  moving next to concurrent read/write plus background maintenance.
- Existing benchmark rows measure flush, compaction, blob GC, and write
  amplification one operation at a time, but they do not directly measure
  foreground reads/writes while background maintenance is active.
- Existing stats already expose cooperative maintenance yields, maintenance
  budget exhaustions, compaction counters, and per-storage-operation latency.
- The new contention diagnostic compares `background_worker_count = 0` against
  `background_worker_count = 1` under the same small-buffer write pressure.
- `TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench` reported foreground writes
  at 1353150 us median and background-worker writes at 2148083 us median. Read
  wall stayed small in both cases: 1352 us median foreground and 1445 us median
  with a worker.
- The background-worker row reported 129 cooperative yields and 47 budget
  exhaustions median. It also performed more storage work than the foreground
  row: 88 manifest publishes vs 69, 180 persists vs 92, and 45 directory syncs
  vs 23.

## Known Residuals

- Foreground point reads are not the first bottleneck in this pressure model.
- Background maintenance currently uses too many small maintenance turns under
  write pressure, causing extra manifest publishes, persists, directory syncs,
  cooperative waits, and budget exhaustions.
- This phase intentionally did not change coordinator behavior; the evidence now
  justifies a follow-up tuning phase.

## Next Recommendation

- Tune background-worker maintenance budget and foreground backpressure waits,
  then rerun the contention diagnostic.
