# Current Phase

## Status

Active

## Goal

Make Trine's LSM read-path advantages measurable and improve the first
high-signal batch/negative lookup bottleneck.

## Scope

- Persistent local-file point reads first.
- `get_many` and localized point batches.
- Negative lookup / missing point reads.
- Metadata/data-block cache and filter counters that prove whether reads avoid
  unnecessary table, index, and data-block work.
- Compression read bandwidth is discovery-only until batch/missing evidence is
  clear.

## Out Of Scope

- Storage format changes.
- Public API changes unless existing `get_many` cannot express the optimized
  path.
- Compaction throughput and blob GC write-side tuning.
- Platform-io backend work.
- Publishing, tagging, pushing, or release workflow changes.

## Acceptance Gate

- Benchmark diagnostics distinguish sequential versus batched point reads for
  localized hits and missing keys.
- Evidence reports table probes, block metadata probes, data block reads,
  filter skips, cache misses, and storage read requests.
- A retained optimization improves at least one of: localized batch reads,
  missing batch reads, or metadata/data-block reuse without weakening MVCC or
  read visibility.
- Focused tests cover the changed read-path behavior.
- Formatting, strict clippy, relevant tests, and diff whitespace checks pass
  before commit.

## Active Task Slice

```text
task818 [x] goal:add persistent missing batch benchmark diagnostics | scope:benches/v1_bench.rs | verify:cargo check -q --benches + targeted bench rows
task819 [x] goal:separate out-of-bounds miss from in-bounds filter miss diagnostics | scope:benches/v1_bench.rs | verify:cargo check -q --benches + targeted bench rows
task820 [x] goal:measure read-path diagnostics and choose first retained optimization | scope:src/lsm src/table.rs benches/v1_bench.rs | verify:TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
```

## Evidence

- `get_many` already exists for `Db`, `Bucket`, and `BucketReader`.
- Table point batch reads already group keys by data block and have a regression
  test proving multiple keys in one persistent data block share one data-block
  read.
- Existing localized point diagnostics cover hit-heavy sequential versus batched
  reads, but missing-key persistent batch diagnostics were not separated from
  memory-only `missing get`.
- The first persistent missing diagnostic used `missing-*` keys, which sort
  outside the `key-*` table bounds and therefore measure key-bound rejection
  rather than Bloom/filter negative lookup.
- New bounded missing diagnostics use absent `key-*`-range keys. They report
  2048 point filter skips, 18 block metadata probes, 0 data-block reads, and
  0 storage read-owned requests, proving the in-bounds negative path is filter
  gated.
- Retained optimization: small all-unique `get_many` slices choose the single
  key loop before allocating/deduplicating a `PointReadBatch`; small duplicate
  slices still use the batch path for shared lookup.
- Three-run V1 benchmark evidence after the optimization:
  - `missing batched point read persistent` median 124 us versus 328 us before.
  - `bounded missing batched point read persistent` median 224 us versus 386 us
    before.
  - `localized batched point read persistent` median 1125 us and remains faster
    than localized sequential median 1348 us.

## Known Residuals

- Missing-key advantage must be proven by low data-block reads and high filter
  skips, not just wall-clock time.
- Out-of-bounds missing keys should remain a separate diagnostic because they
  exercise key-bound rejection rather than table filters.

## Next Recommendation

- Run the strict local gate and commit this read-path slice.
