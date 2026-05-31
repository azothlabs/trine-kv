# Current Phase

## Status

Complete

## Goal

Build immutable writer-local prepared commit data before entering the publish
barrier, without changing commit visibility, WAL format, or memtable
publication behavior.

## Scope

- Replace raw writer-local operations with a prepared commit shape.
- Group accepted write operations into current single-shard bucket deltas with
  per-operation batch indexes, coarse key bounds, estimated bytes, WAL payload
  order, and touched LSM trees.
- Keep transaction validation, sequence assignment, WAL append, memtable
  publication, visible marking, and post-commit freeze under the named publish
  barrier.
- Preserve existing public async API shape, blocking API, commit tracker,
  WAL/table/blob/manifest formats, MVCC, compaction, recovery, cleanup, and
  storage behavior.

## Out Of Scope

- Adding WAL shards or changing WAL recovery ordering.
- Publishing through key-sharded heads.
- Removing or narrowing the publish barrier.
- Moving table open metadata reads onto async owned completions.
- Adding true async OS file I/O or public runtime tuning options.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the writer-local prepared-delta phase.
- Accepted non-empty writes prepare bucket delta data before entering the
  publish barrier and remain invisible until publication.
- Prepared deltas preserve WAL operation order, batch indexes, touched bucket
  states, coarse key bounds, and estimated bytes.
- Publish-time validation, sequence assignment, WAL append, memtable
  publication, visible marking, and freeze still run under the publish barrier.
- Focused commit/write/async tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining write-path blockers and the recommended next
  phase.

## Active Task Slice

```text
task382 [x] goal:start writer-local prepared-delta phase | scope:current roadmap | verify:manual
task383 [x] goal:add prepared commit and single-shard delta structs | scope:src/db/commit.rs | verify:commit tests
task384 [x] goal:publish from prepared deltas under existing barrier | scope:src/db/commit.rs | verify:write/async tests
task385 [x] goal:verify focused and full gates | scope:src tests benches | verify:full tests
task386 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap src | verify:git status
```

## Known Blockers

- The current prepared delta uses a single in-memory shard per bucket; real
  key-sharded heads are still a later phase.
- The publish barrier still serializes transaction validation, sequence
  assignment, WAL append, delta publication, visibility marking, and freeze.
- Native-file async reads still use the bounded blocking adapter rather than
  platform-native async file I/O.
- Table open header/footer reads still use synchronous borrowed reads.
- Runtime tuning options are still internal.

## Evidence

- Phase 91 completed awaited async cursor owned-read completions for table
  data-block metadata and body loads.
- The foreground write-path protocol says the next post-tracker slice is to
  introduce immutable prepared commits and shard delta types without changing
  public API behavior.
- Existing code already has a named publish barrier and accepted writer-local
  state, but it still stores raw operations and performs bucket routing, WAL
  payload construction, and touched-tree collection under the barrier.
- Accepted non-empty writes now build a `PreparedCommit` before entering the
  publish barrier.
- Prepared commit data records WAL operation order, current single-shard bucket
  deltas, per-operation batch indexes, coarse key bounds, estimated bytes, and
  touched LSM trees.
- Publish still keeps transaction validation, sequence assignment, WAL append,
  memtable publication, visible marking, and post-commit freeze under the
  named publish barrier.
- Verification passed: `cargo test commit --lib`,
  `cargo test --test async_api`, `cargo test --test in_memory_mvcc`,
  `cargo test --test in_memory_transaction`,
  `cargo test persistent_write_buffer --test persistent_wal`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, and `cargo test --all-targets --all-features`.

## Next Recommendation

- Continue with in-memory key-sharded delta heads before WAL shard front doors,
  because the prepared commit is now available but publication still writes
  directly into the active memtable under the publish barrier.
