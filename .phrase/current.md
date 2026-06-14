# Current Phase

## Status

Complete

## Goal

Reduce foreground write latency under maintenance pressure by tuning
background-worker maintenance budget, foreground backpressure ownership, and
post-commit background flush admission.

## Scope

- Native persistent local-file background maintenance behavior.
- Internal worker budget and post-commit flush request admission.
- Foreground write-pressure maintenance ownership.
- Benchmark evidence from the existing V1 contention diagnostics.

## Out Of Scope

- Storage format changes.
- Platform-io backend changes.
- Public API changes.
- New compaction selection policy.
- Publishing, tagging, pushing, or release workflow changes.

## Acceptance Gate

- Background maintenance contention diagnostic improves write wall time or
  reduces redundant storage maintenance operations without a meaningful read
  regression.
- Tests cover the selected worker-budget and pressure ownership behavior.
- Full lib tests, all-feature tests, strict clippy, formatting, diff whitespace
  checks, and grouped benchmark evidence pass before commit.

## Active Task Slice

```text
task812 [x] goal:tune background maintenance budget/admission | scope:src/db.rs | verify:focused tests + TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
task813 [x] goal:record Phase 186 evidence | scope:docs/benchmarks .phrase | verify:evidence review
```

## Evidence

- Phase 185 background-worker row reported 2148083 us median write wall, 129
  cooperative yields, 47 budget exhaustions, 88 manifest publishes, 180
  persists, and 45 directory syncs.
- Budget-only and shorter-wait experiments were rejected: they did not reduce
  storage operation counts, and the shorter wait raised cooperative yields and
  budget exhaustions.
- The retained change makes pressure maintenance foreground-first, gives
  background workers an internal pressure-sized budget, and delays background
  flush requests until they can batch useful work.
- `TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench` reported the background
  contention row at 1491161 us median write wall, 0 cooperative yields, 0 budget
  exhaustions, 69 manifest publishes, 92 persists, and 23 directory syncs.

## Known Residuals

- The background-worker read-wall median in the Phase 186 run was noisier
  (2227 us) but remained low-millisecond and did not drive the original blocker.
- The remaining write-wall gap to the foreground-only row belongs to broader
  write-path/storage durability tuning, not this worker contention slice.

## Next Recommendation

- Continue the KV optimization queue from fresh grouped benchmark evidence.
  Good next candidates are the broader write path durability costs or the
  remaining cold-open/recovery costs, depending on the next selected phase.
