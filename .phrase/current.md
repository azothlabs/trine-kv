# Current Phase

## Status

In progress

## Goal

Build the v1 engine in measured slices without silently changing the accepted
protocol.

## Entry Condition

- Phase 2 crate scaffold is complete.
- `cargo fmt --check`, `cargo clippy`, and scaffold tests passed.
- The v1 protocol remains the implementation source of truth.

## Scope

- Implement one behavior slice at a time.
- Keep each slice aligned with MVCC, write batch, snapshot, WAL, SSTable,
  manifest, compaction, transaction, prefix-filter, compression, and
  search-policy contracts.
- Add tests before claiming a slice works.

## Out Of Scope

- Implementing multiple adjacent engine subsystems in one unverified jump.
- Changing public protocol behavior without updating the spec or an ADR.
- Adding external codec crates before codec behavior and fixtures are ready.
- Persistent crash recovery before in-memory MVCC point semantics exist.

## Acceptance Gate

- The v1 acceptance gate in `.phrase/protocol/trine-kv-v1-spec.md` passes.
- Each slice records its verification evidence and remaining blockers.

## Active Task Slice

```text
task003 [x] goal:in-memory MVCC point writes, deletes, and snapshot reads work | scope:src/db.rs,src/keyspace.rs,src/write_batch.rs,src/blob.rs,src/snapshot.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task004 [x] goal:in-memory range and prefix iteration return snapshot-consistent ordered live keys | scope:src/db.rs,src/keyspace.rs,src/iterator.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task005 [x] goal:in-memory range deletes affect point, range, and prefix reads with snapshot safety | scope:src/db.rs,src/write_batch.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task006 [x] goal:optimistic transaction point/range read conflict validation works in memory | scope:src/transaction.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task007 [x] goal:persistent mode writes committed batches to WAL and replays them on reopen | scope:src/db.rs,src/wal.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task008 [x] goal:WAL recovery handles torn tail and fails closed on checksum corruption | scope:src/wal.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task009 [x] goal:manifest persists keyspace creation/options and WAL replay floor | scope:src/manifest.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task010 [x] goal:memtable flush writes readable SSTable files and advances manifest replay floor | scope:src/table.rs,src/db.rs,src/manifest.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task011 [x] goal:SSTable recovery fails closed on missing/corrupt table files | scope:src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task012 [x] goal:manual compaction rewrites flushed tables without changing MVCC visibility | scope:src/db.rs,src/table.rs,src/manifest.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task013 [x] goal:table block/index layout supports checked point/range reads with codec id none | scope:src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task014 [x] goal:lz4_flex-backed fast block compression round-trips table blocks and fails closed on missing codec support | scope:Cargo.toml,src/codec.rs,src/table.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task015 [x] goal:prefix filters skip incompatible table reads without changing MVCC/range-tombstone results | scope:src/filter.rs,src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task016 [x] goal:separated blob values survive reopen, flush, and compaction | scope:src/blob.rs,src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task017 [x] goal:point-key filters skip incompatible point-record reads without changing MVCC/range-tombstone results | scope:src/filter.rs,src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task018 [x] goal:SSTable point/range/prefix reads use block index and restart points for candidate selection | scope:src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task019 [x] goal:persistent recovery fails closed on safe temporary files by default and writes a repair report when repair is explicitly enabled | scope:src/recovery.rs,src/db.rs,src/lib.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task020 [x] goal:manual compaction drops obsolete point versions only when active snapshot pins allow it | scope:src/snapshot.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task021 [x] goal:manual compaction drops point/range tombstones only when the compaction scope proves they no longer hide retained values | scope:src/db.rs,src/manifest.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task022 [x] goal:compaction removes blob files no longer referenced by live tables or active snapshots | scope:src/blob.rs,src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task023 [x] goal:persistent startup detects unreferenced table/blob files and handles them conservatively | scope:src/recovery.rs,src/db.rs,src/blob.rs,src/table.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test + git diff --check
task024 [x] goal:SSTable reads use partitioned filters/indexes so point/range/prefix reads avoid unnecessary whole-table scans | scope:src/filter.rs,src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test + git diff --check
task025 [x] goal:search-policy code is wired into table/block candidate lookup without changing read results | scope:src/search.rs,src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test + git diff --check
task026 [x] goal:v1 protocol acceptance audit identifies remaining gaps and the next measured slice | scope:.phrase/protocol,trine source,tests | verify:manual audit + cargo fmt --check + cargo clippy + cargo test + git diff --check
task027 [x] goal:persistent open takes an exclusive database directory lock and releases it safely | scope:src/db.rs,src/recovery.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test + git diff --check
task028 [x] goal:table metadata records compaction levels and read ordering remains correct across levels | scope:src/manifest.rs,src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test + git diff --check
task029 [x] goal:compaction planning selects level-aware L0 inputs and overlapping lower-level tables | scope:src/compaction.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test + git diff --check
task030 [x] goal:flush path can trigger automatic compaction when L0 pressure exceeds configured limits | scope:src/db.rs,src/compaction.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test + git diff --check
task031 [x] goal:database stats expose table, L0, blob, and compaction counters from live state | scope:src/stats.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test + git diff --check
task032 [ ] goal:block cache records table block hits and misses without changing read results | scope:src/cache.rs,src/table.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test + git diff --check
```

## Known Blockers

- Cache behavior, benchmark output, and durability docs are incomplete.

## Evidence To Record

- Phase 2 scaffold gate results.
- Tombstone cleanup validation results.
- Blob cleanup validation results.
- Obsolete-file detection validation results.
- Partitioned filter/index validation results.
- Search-policy dispatch validation results.
- V1 acceptance audit results.
- Process-lock validation results.
- Compaction-level metadata validation results.
- Level-aware compaction planning validation results.
- Automatic compaction trigger validation results.
- Live stats validation results.
- Remaining blocker category after task031.
