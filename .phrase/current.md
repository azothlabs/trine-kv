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
task821 [x] goal:reduce missing get CPU overhead before table lookup | scope:src/lsm/read.rs benches/v1_bench.rs | verify:TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
task822 [x] goal:protect hot metadata from data-block churn in small cache | scope:src/cache.rs src/table.rs | verify:cache/table tests + TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
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
- Negative lookup follow-up retained two more changes:
  - Single-key and batch reads build the memtable range-tombstone index only
    after a point candidate exists; all-missing unique batches scatter `None`
    without building that index.
  - Memtable point lookup checks the first and last user key before building
    internal key bounds, so range-outside misses skip the BTree range search.
- The `missing get` benchmark now prebuilds missing keys outside the measured
  closure, so it measures database lookup rather than repeated benchmark string
  formatting.
- Three-run V1 benchmark evidence after the negative lookup follow-up:
  - `missing get` median 216 us versus 719 us before this follow-up.
  - `merged delta missing get` median 163 us.
  - `missing batched point read persistent` median 125 us.
  - `bounded missing batched point read persistent` median 221 us.
- Hot/cold cache follow-up retained the existing priority split: index/filter
  metadata is high priority and data/blob blocks are low priority. The cache now
  allows one high-priority entry to exceed its shard target when the total cache
  can hold it, so a large hot index partition is not inserted and immediately
  evicted by a small per-shard budget.
- A new L2 table test proves hot lazy index metadata survives same-partition
  data-block churn under a small cache. Cache tests also prove oversized
  high-priority entries survive low-priority churn.
- Three-run V1 benchmark evidence after the cache follow-up:
  - `block cache random hit diagnostic` remains 2048 cache hits, 0 cache
    misses, and 0 storage read-owned requests.
  - `block decode forced diagnostic` remains 2048 cache misses and 2048 storage
    read-owned requests.
  - `random get` median 775 us, `missing get` median 210 us, and persistent
    missing/bounded-missing rows remain in the same range as the prior slice.

## Known Residuals

- Missing-key advantage must be proven by low data-block reads and high filter
  skips, not just wall-clock time.
- Out-of-bounds missing keys should remain a separate diagnostic because they
  exercise key-bound rejection rather than table filters.

## Next Recommendation

- Run the strict local gate and commit this hot/cold cache slice.
