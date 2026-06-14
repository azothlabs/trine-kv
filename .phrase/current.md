# Current Phase

## Status

Complete

## Goal

Measure and reduce hot-read block-cache and block-decode costs while preserving
MVCC visibility, table/blob formats, prefix/range filters, compression
contracts, and read-path correctness.

## Scope

- Block cache keys, admission, eviction, and hit promotion costs.
- Metadata/data block cache separation and whether metadata survives data churn.
- Repeated data-block decode paths when cache is disabled or undersized.
- Compression-block decode costs and avoidable allocation in benchmark-visible
  paths.
- Benchmark diagnostics that distinguish cache hits, cache misses, table data
  block reads, metadata probes, storage reads, and codec decode work.

## Out Of Scope

- Storage format changes.
- MVCC, snapshot, transaction, range-delete, prefix-filter, manifest, WAL,
  compaction, blob-GC, platform-io, publishing, tagging, pushing, or release
  workflow changes.
- New compression formats beyond the V1 `none` and `fast-lz4-block` contract.
- Broad cache replacement algorithm rewrites without benchmark evidence.

## Acceptance Gate

- Diagnostics classify warm cache hit behavior, forced block decode reads,
  metadata/data cache behavior, and codec decode behavior before optimization.
- Any retained code change is justified by benchmark evidence and keeps table
  reads, filters, cache key separation, and compression behavior unchanged.
- Focused cache/table tests pass.
- Strict clippy passes.
- Single-run and grouped benchmark evidence records before/after behavior.

## Active Task Slice

```text
task795 [x] goal:add block-cache/decode diagnostics | scope:benches/v1_bench.rs | verify:TRINE_BENCH_RUNS=1 cargo bench --bench v1_bench
task796 [x] goal:optimize measured cache/decode bottleneck | scope:src/cache.rs benches/v1_bench.rs | verify:cargo test -q cache --lib
task797 [x] goal:record cache/decode evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Phase 179 completed maintenance write-amplification diagnostics and reduced
  foreground manifest publishes for blob maintenance cleanup.
- Grouped benchmark evidence currently points to cache/decode, point reads,
  startup/recovery, search-policy, and concurrent maintenance as remaining
  optimization candidates.
- Initial audit found block cache keys already include kind/table/block and
  metadata entries use higher eviction priority than data/blob entries.
- Initial audit found cache hits clone the cached value and then attempt
  best-effort recency promotion; promotion currently scans the priority queue
  even when the hot key is already the newest entry.
- Added benchmark rows and diagnostics for random hot block-cache reads, warm
  cache-hit counters, random cache-hit counters, forced block decode counters,
  and codec decode-only costs.
- A/B `TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench` kept the cache change:
  random cached block read median improved from 1532 us to 1488 us, warm cached
  read median improved from 1361 us to 1285 us, random hit diagnostic improved
  from 1681 us to 1657 us, and warm hit diagnostic improved from 1421 us to
  1316 us.
- Cache-hit diagnostics still reported 2048 cache hits, zero misses, and zero
  storage read-owned requests; forced decode diagnostics still reported zero
  hits, 2048 misses, and 2049 storage read-owned requests.

## Known Residuals

- LZ4 block decode remains much more expensive than `none` decode in the
  decode-only rows. Reducing that further likely requires a separate table
  block ownership or compression-boundary phase.

## Next Recommendation

- Move next to concurrent read/write and background maintenance, unless the
  user wants a dedicated LZ4 decode-allocation phase first.
