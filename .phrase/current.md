# Current Phase

## Status

Complete

## Goal

Measure and reduce confirmed-write latency and write/flush throughput cost
without weakening durability defaults or changing storage formats.

## Scope

- Persistent write diagnostics for default-bucket puts and explicit
  `WriteBatch` commits.
- WAL append, WAL persist, memtable publication, flush-triggered work, and
  storage operation counters.
- One measured write-path optimization if diagnostics expose a safe dominant
  cost.

## Out Of Scope

- Storage format changes.
- Durability weakening or making confirmed writes less recoverable.
- Manifest, WAL, SSTable, blob, MVCC, transaction, compaction, platform-io
  backend, publishing, tagging, pushing, or release workflow changes.
- Scan, cache, codec, and blob-value optimization.

## Acceptance Gate

- Write diagnostics classify WAL append, WAL persist, publication, and flush
  work before optimization.
- Any retained code change preserves documented durability and storage formats.
- Focused write/WAL tests pass.
- Strict clippy passes.
- Single-run and grouped benchmark evidence records before/after behavior.

## Active Task Slice

```text
task786 [x] goal:add persistent write-path diagnostics | scope:benches/v1_bench.rs | verify:TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
task787 [x] goal:optimize measured write-path bottleneck | scope:src/wal.rs | verify:cargo test -q wal_front_door_persists_only_dirty_shards --lib
task788 [x] goal:record write-path evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Current `single-key put` and `batch write` benchmark rows use an in-memory
  database, so they do not diagnose WAL/fsync behavior.
- The engine already preaccepts normal non-transaction WAL writes before
  entering the global publish barrier when the WAL front door is available.
- The measured dominant avoidable cost was explicit `persist_sync(SyncData)`
  repeatedly syncing clean WAL shards: 256 commits produced 1018 persist
  requests before the fix.
- After tracking per-lane WAL durability, the same diagnostic produced 256
  persist requests and reduced median persist wall time from about 1.21s to
  about 0.49s.

## Known Residuals

- Per-commit `SyncData`/`SyncAll` confirmed writes remain dominated by real
  storage sync cost. Avoiding that requires caller-side batching or a larger
  group-commit design with explicit visibility and durability semantics.

## Next Recommendation

- Move next to the remaining benchmark-backed optimization item selected by
  current evidence, likely scan/search-policy or block-cache/decode work.
