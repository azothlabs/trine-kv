# Current Phase

## Status

Complete

## Goal

Phase 1 of `.phrase/protocol/layered-filter-allocation.md`: make each LSM level's
observed Bloom false-positive behavior measurable before changing any
`bits_per_key` allocation (measure-first for the Monkey-style direction).

## Design Assessment

Filter counters are already recorded per table, and every table knows its level,
so per-level rollup is a zero-hot-path change: aggregate `table.filter_stats()`
by level when building `DbStats`. No new persisted state, no format change.

## Scope

- `DbStats::level_filters: Vec<LevelFilterStats>` aggregating table filter
  counters by level, plus `FilterStats::table_point_false_positive_rate`.
- A `layered filter fpr diagnostic` benchmark that builds a multi-level layout,
  runs in-range negative lookups, and reports each level's observed FPR.
- Tests: FPR helper unit tests + a persistent test proving per-level rollup
  reconciles with the global totals and table counts.

## Out Of Scope

- Any `bits_per_key` allocation change (that is Phase 2).
- Storage-format or `BucketOptions` change.

## Acceptance Gate

- `DbStats` exposes per-level filter counters distinct from global totals. Met.
- The diagnostic reports per-level `f_i`. Met.
- No read/write/compaction behavior change, no format change. Met.

## Evidence

- `layered filter fpr diagnostic` (TRINE_BENCH_RUNS=3): L0 FPR 9286 ppm
  (19/2046), L1 FPR 10263 ppm (21/2046), each level 2 tables, **negative-lookup
  data block reads = 0**.
- Interpretation: per-level FPR is uniform (~1%, matching the global
  `bits_per_key = 10`), exactly the flat allocation Monkey targets. But Trine's
  two-level filter (table + block) already drives negative-lookup data-block
  reads to zero, so the realizable Monkey gain is in **filter memory and probe
  CPU / block-cache pressure**, not lookup I/O. This sharpens Phase 2's goal:
  cut filter memory at equal leakage, not cut I/O.

## Known Risks

- Per-table counters reset when compaction replaces a table, so `level_filters`
  is "current live tables since open"; adequate for the diagnostic.
- FPR is probabilistic; tests assert structural rollup reconciliation, not exact
  rates (rates are checked via the deterministic helper unit tests).

## Next Recommendation

- Phase 2 (per-output-level `bits_per_key` curve) should target **filter-memory
  reduction at equal negative-lookup leakage** (block-filter probes / bytes),
  since data-block-read I/O is already ~0. Gather a filter-memory-per-level
  metric before committing a curve.
