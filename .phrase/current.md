# Current Phase

## Status

Complete

## Goal

Stage the WAL recovery sequence-merge boundary for future WAL shard replay
while keeping the current database on one WAL stream.

## Scope

- Add a WAL batch-stream merge helper that orders batches by commit sequence.
- Reject non-increasing batches inside one stream.
- Reject duplicate commit sequences across streams.
- Route current single-WAL recovery through the merge helper so the boundary is
  exercised without adding more WAL files.
- Keep one WAL file, one append lane, existing WAL frame format, and existing
  recovery behavior for deployed databases.

## Out Of Scope

- Adding multiple WAL shards or new WAL file names.
- Changing WAL append routing, durability mode semantics, or WAL rewrite
  behavior.
- Moving transaction validation, memtable publication, or persistent freeze out
  of the publish barrier.
- Changing public async/blocking API, storage formats, MVCC, compaction, or
  manifest/table/blob formats.
- Adding true async OS file I/O or public runtime tuning options.

## Acceptance Gate

- Roadmap records this as the WAL recovery merge boundary phase.
- WAL stream merge orders batches from multiple sources by commit sequence.
- WAL stream merge rejects duplicate commit sequences across sources.
- WAL stream merge rejects non-increasing sequences inside a source.
- Current persistent open still reads the single WAL stream and replays through
  the same merge boundary.
- Focused WAL/recovery tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining blockers and the recommended next phase.

## Active Task Slice

```text
task428 [x] goal:start WAL recovery merge boundary phase | scope:current roadmap | verify:manual
task429 [x] goal:add sequence-ordered WAL stream merge helper | scope:src/wal.rs | verify:wal tests
task430 [x] goal:route single-WAL recovery through merge helper | scope:src/db.rs | verify:persistent_wal tests
task431 [x] goal:prove duplicate and non-increasing WAL stream rejection | scope:src/wal.rs | verify:wal tests
task432 [x] goal:record evidence and run verification | scope:.phrase src tests | verify:full gate
```

## Known Blockers

- The database still has one WAL append lane and one WAL file.
- WAL shard file discovery and per-shard recovery reads are not implemented.
- The publish barrier still serializes transaction validation, persistent
  memtable publication, and persistent freeze.
- Transaction writes still accept WAL only after read-set validation inside the
  publish barrier.
- Native-file async reads still use the bounded blocking adapter rather than
  platform-native async file I/O.
- Table open header/footer reads still use synchronous borrowed reads.

## Evidence

- Phase 100 moved normal slot visibility completion out of the publish barrier,
  but WAL recovery still assumed one already-ordered WAL batch stream.
- `wal::merge_batch_streams_by_sequence` now validates each source stream and
  sorts all batches by commit sequence.
- The merge helper rejects duplicate sequences across sources and
  non-increasing sequences inside one source.
- Persistent open now feeds its single WAL stream through the merge helper
  before replay, exercising the boundary without changing storage layout.
- Focused tests passed: `cargo test wal_stream_merge --lib`,
  `cargo test wal --lib`, and `cargo test --test persistent_wal`.
- Full verification passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan outside instruction files.

## Next Recommendation

- Continue only after choosing a narrow next slice: either WAL shard file
  discovery with one active lane, or a single-lane front-door worker queue that
  preserves the merge and recovery rules now tested.
