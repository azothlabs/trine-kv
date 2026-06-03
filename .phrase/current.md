# Current Phase

## Status

Complete

## Goal

Add and measure batched point-read APIs that reuse one point-read snapshot for
many keys while preserving per-key visibility, ordering, and missing-key
semantics.

## Scope

- Default-bucket and bucket-scoped current-value batched point reads.
- Snapshot-bound repeated point reads through `BucketReader`.
- Benchmark rows comparing sequential point reads with batched point reads.
- Rustdoc for new public APIs.

## Out Of Scope

- Storage format, MVCC, WAL, table, blob, compaction, transaction, recovery,
  browser persistence, or release metadata changes.
- Cross-bucket batching.
- Reordering input keys, deduplicating keys, or returning unordered maps.
- Writer-lease or cold reopen changes.

## Acceptance Gate

- Public API returns results in the same order as input keys and uses `None`
  for missing or deleted keys.
- Batched point reads capture one read sequence and reuse one point-read source
  snapshot for the batch.
- Benchmark evidence compares sequential and batched point reads.
- Focused API/benchmark checks pass.
- Rustdoc, doctests, formatting, clippy, full tests, diff checks, and
  forbidden-term scans pass.

## Active Task Slice

```text
task587 [x] goal:add batched point-read APIs | scope:src/db.rs src/bucket.rs | verify:cargo test -q get_many --lib
task588 [x] goal:add benchmark rows | scope:benches/v1_bench.rs docs/benchmarks | verify:cargo bench --bench v1_bench
task589 [x] goal:record phase evidence | scope:.phrase docs README.md | verify:git diff --check
task590 [x] goal:finish local verification | scope:full workspace | verify:cargo rustdoc/test/clippy/diff checks
```

## Known Residuals

- Phase 140 left writer-lease acquisition intact because it is a safety
  boundary.
- Existing `BucketReader` already captures one point-read snapshot for repeated
  point reads under a caller snapshot.
- Benchmark timing is mixed. Persistent flushed point reads showed a small
  batch-size-4 improvement in one run; in-memory random point reads are not a
  stable win yet.

## Evidence

- Earlier benchmark rows include ordinary point reads, missing point reads,
  delta-backed point reads, and long shared-prefix point reads, but no batched
  point-read row.
- The current API has `get_sync`/`get` for one key and `BucketReader` for
  repeated snapshot-bound reads.
- Phase 141 added `get_many_sync`/`get_many` to `Db` and `Bucket`, plus
  batched `BucketReader` methods.
- The second batch-size-4 release run showed persistent flushed point reads at
  1485 us sequential vs 1391 us batched, while memory random point reads were
  1069 us sequential vs 2444 us batched.
- `cargo rustdoc --all-features -- -D warnings` and
  `cargo test --doc --all-features` passed after adding public API docs.
- `cargo clippy --all-targets --all-features -- -D warnings` and
  `cargo test -q --all-targets --all-features` passed with output redirected
  to temporary files.

## Next Recommendation

- Commit this API and mixed benchmark evidence. Further speed work should
  target deeper table/delta grouping before adding more public surface.
