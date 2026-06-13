# Current Phase

## Status

Complete

## Goal

Refresh the benchmark baseline machinery so later KV optimization phases choose
targets from grouped, multi-run evidence instead of single-run noise.

## Scope

- `benches/v1_bench.rs` output mode for multi-run summaries.
- Workload grouping for existing benchmark rows.
- First grouped baseline evidence for the next optimization phase.
- Documentation that explains how to run the refreshed benchmark check.

## Out Of Scope

- Storage format changes.
- WAL, manifest, compaction, MVCC, table, blob, cache, or platform-io behavior
  changes.
- Optimizing startup/recovery, writes, compaction, scans, cache, blob, or
  concurrency in this phase.
- Publishing, tagging, pushing, or release workflow changes.

## Acceptance Gate

- Default benchmark output remains compatible with the existing single-run CSV.
- `TRINE_BENCH_RUNS=N` runs the full benchmark suite multiple times and reports
  grouped min/median/max summaries.
- Workload groups cover point reads, scans, transactions, recovery, writes,
  blob, cache, cold open/read, search policy, iterator, codec, and diagnostics.
- A local multi-run benchmark result is recorded and used to recommend the next
  optimization phase.
- Formatting, strict clippy, benchmark execution, and diff checks pass.

## Active Task Slice

```text
task780 [x] goal:add multi-run grouped benchmark output | scope:benches/v1_bench.rs | verify:cargo bench --bench v1_bench
task781 [x] goal:record grouped baseline and next target | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
task782 [x] goal:update roadmap for follow-up KV optimization queue | scope:.phrase/roadmap.md | verify:git diff review
```

## Evidence

- Default single-run output remains compatible with the existing CSV shape.
- `TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench` now emits grouped min,
  median, and max summaries.
- The first grouped baseline recorded median rows of 178000 us for compaction,
  170524 us for blob level merge, 169310 us for blob GC rewrite, 109257 us for
  separated blob values, 83592 us for cold table read, and 65977 us for flush
  throughput.
- Future optimization areas include startup/recovery, write fsync/group commit,
  flush/compaction/blob maintenance, range/prefix scans, block cache/decode,
  large-value paths, and concurrent read/write behavior.

## Known Residuals

- The new baseline machinery is not a performance fix; it is the evidence gate
  for choosing the next real optimization target.

## Next Recommendation

- Start the next optimization phase by decomposing compaction and blob
  maintenance write amplification before changing behavior.
