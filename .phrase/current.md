# Current Phase

## Status

Complete

## Goal

Publish in-memory writes into key-sharded delta heads and make in-memory read
paths include those deltas, while preserving the existing active-memtable
mirror and public behavior.

## Scope

- Add bucket-local in-memory delta heads split by a fixed key-shard count.
- Publish accepted in-memory writes into those delta heads after WAL acceptance
  and before the sequence is marked visible.
- Make point reads, range/prefix scans, and transaction conflict checks include
  delta-head records and range tombstones.
- Keep the current active-memtable publication path as a compatibility mirror
  for freeze/stats behavior in this phase.
- Preserve existing public async API shape, blocking API, commit tracker,
  WAL/table/blob/manifest formats, MVCC, compaction, recovery, cleanup, and
  persistent storage behavior.

## Out Of Scope

- Adding WAL shards or changing WAL recovery ordering.
- Removing or narrowing the publish barrier.
- Removing active-memtable publication or changing flush/freeze accounting.
- Moving table open metadata reads onto async owned completions.
- Adding true async OS file I/O or public runtime tuning options.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the in-memory key-sharded delta-head phase.
- In-memory writes publish immutable delta data into bucket-local key shards.
- Point reads and range/prefix scans include in-memory delta heads under MVCC
  visibility and range tombstone rules.
- Transaction conflict checks see records and range tombstones published
  through delta heads.
- Active-memtable publication remains as the current compatibility mirror.
- Focused commit/write/async tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining write-path blockers and the recommended next
  phase.

## Active Task Slice

```text
task387 [x] goal:start in-memory delta-head phase | scope:current roadmap | verify:manual
task388 [x] goal:add key-sharded delta head storage | scope:src/lsm | verify:lsm tests
task389 [x] goal:publish in-memory prepared writes to delta heads | scope:src/db/commit.rs src/lsm | verify:commit/mvcc tests
task390 [x] goal:include delta heads in point/scan/conflict reads | scope:src/lsm | verify:mvcc/iteration/transaction tests
task391 [x] goal:verify focused and full gates | scope:src tests benches | verify:full tests
task392 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap src | verify:git status
```

## Known Blockers

- The publish barrier still serializes transaction validation, sequence
  assignment, WAL append, delta publication, visibility marking, and freeze.
- Active-memtable publication remains as a compatibility mirror until a later
  phase moves freeze/stats behavior onto delta epochs or merge outputs.
- Persistent commits still use the single WAL writer.
- Native-file async reads still use the bounded blocking adapter rather than
  platform-native async file I/O.
- Table open header/footer reads still use synchronous borrowed reads.
- Runtime tuning options are still internal.

## Evidence

- Phase 92 added writer-local `PreparedCommit` data and current single-shard
  bucket deltas while preserving existing publication behavior.
- The foreground write-path protocol lists publishing deltas through
  key-sharded heads as the next staged slice after prepared commits.
- Current freeze and stats behavior still depend on active/immutable memtables,
  so this phase keeps the active memtable as a mirror while proving delta-head
  reads.
- `LsmTree` now owns fixed bucket-local delta shards. In-memory commits publish
  immutable delta data into those shards before active-memtable mirror writes.
- Delta-only LSM tests prove point records, range tombstones, and range scans
  can read from delta heads without active memtable data.
- Normal in-memory point reads, snapshot readers, scans, and transaction
  conflict checks avoid scanning mirrored deltas when active/immutable
  memtables already cover the read sequence.
- Verification passed: `cargo test delta --lib`,
  `cargo test --test in_memory_mvcc`, `cargo test --test in_memory_iteration`,
  `cargo test --test in_memory_transaction`, `cargo test --test async_api`,
  `cargo test persistent_write_buffer --test persistent_wal`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, and `cargo test --all-targets --all-features`.

## Next Recommendation

- Continue by removing the active-memtable mirror from in-memory writes only
  after delta epochs or merge outputs can preserve freeze/stats and bounded
  read amplification. WAL shard front doors should remain a later persistent
  write phase.
