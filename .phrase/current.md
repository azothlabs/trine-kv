# Current Phase

## Status

Complete

## Goal

Stop mirroring in-memory writes into the active memtable by making delta heads
carry in-memory write accounting and read visibility.

## Scope

- Count delta epoch bytes in the LSM memory accounting used by stats.
- Publish in-memory commits through delta heads without replaying the same
  operations into the active memtable.
- Preserve point reads, range/prefix scans, snapshots, and transaction conflict
  checks without the active-memtable mirror.
- Keep persistent commits on the existing active/immutable memtable path.
- Preserve existing public async API shape, blocking API, commit tracker,
  WAL/table/blob/manifest formats, MVCC, compaction, recovery, cleanup, and
  persistent storage behavior.

## Out Of Scope

- Adding WAL shards or changing WAL recovery ordering.
- Removing or narrowing the publish barrier.
- Changing persistent flush/freeze accounting.
- Adding background delta merge workers.
- Moving table open metadata reads onto async owned completions.
- Adding true async OS file I/O or public runtime tuning options.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the in-memory delta-backed write phase.
- In-memory commits publish through delta heads without active-memtable mirror
  writes.
- `DbStats::memtable_bytes` counts in-memory delta bytes.
- Point reads and range/prefix scans keep seeing in-memory writes under MVCC
  visibility rules.
- Snapshots and transaction conflict checks keep working without the mirror.
- Persistent write behavior remains unchanged.
- Focused commit/write/async tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining write-path blockers and the recommended next phase.

## Active Task Slice

```text
task398 [x] goal:start in-memory delta-backed write phase | scope:current roadmap | verify:manual
task399 [x] goal:count delta epoch bytes in LSM memory accounting | scope:src/lsm | verify:stats tests
task400 [x] goal:skip active-memtable mirror for in-memory commits | scope:src/db/commit.rs src/lsm | verify:in-memory tests
task401 [x] goal:prove reads and transaction checks work without mirror | scope:tests src/db/commit.rs | verify:mvcc/iteration/transaction tests
task402 [x] goal:record evidence and run verification | scope:.phrase src tests | verify:full gate
```

## Known Blockers

- The publish barrier still serializes transaction validation, sequence
  assignment, WAL append, delta publication, visibility marking, and persistent
  freeze.
- Persistent commits still use the single WAL writer.
- Native-file async reads still use the bounded blocking adapter rather than
  platform-native async file I/O.
- Table open header/footer reads still use synchronous borrowed reads.
- Runtime tuning options are still internal.

## Evidence

- Phase 92 added writer-local `PreparedCommit` data and current single-shard
  bucket deltas while preserving existing publication behavior.
- `LsmTree` now owns fixed bucket-local delta shards. In-memory commits publish
  immutable delta data into those shards before active-memtable mirror writes.
- Delta-only LSM tests prove point records, range tombstones, and range scans
  can read from delta heads without active memtable data.
- Normal in-memory point reads, snapshot readers, scans, and transaction
  conflict checks avoid scanning mirrored deltas when active/immutable
  memtables already cover the read sequence.
- Phase 93 left delta heads without epoch sealing, merge, retirement, or read
  amplification budgets.
- Delta shards now track open epoch bytes, max chain length, sealed epochs,
  merged epochs, and retired delta counts.
- Over-budget open epochs are sealed and merged into one immutable delta while
  old snapshots keep cloned `Arc` references to pre-merge deltas.
- Delta tests prove merged point records remain readable at both latest and
  older sequences, and merged range tombstones still hide covered point records.
- In-memory commits now publish through delta heads and skip active-memtable
  mirror writes.
- `DbStats::memtable_bytes` includes delta epoch bytes, so in-memory recent
  writes remain visible in memory accounting.
- `delta_mirror_covers` now requires a non-zero mirror sequence; read sequence
  zero must still inspect deltas when the mirror has been removed.
- Verification passed: `cargo test commit --lib`, `cargo test delta --lib`,
  `cargo test --test in_memory_mvcc`, `cargo test --test in_memory_iteration`,
  `cargo test --test in_memory_range_delete`,
  `cargo test --test in_memory_transaction`, `cargo test --test async_api`,
  `cargo test persistent_write_buffer --test persistent_wal`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, and `cargo test --all-targets --all-features`.

## Next Recommendation

- Continue with write-path contention work only after recording/benchmarking
  the new bounded delta read cost. WAL shard front doors remain a later
  persistent write phase.
