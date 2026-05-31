# Current Phase

## Status

Complete

## Goal

Make accepted write state and the durable publish boundary explicit before
changing the writer coordinator shape.

## Scope

- Add a named publish barrier around the current serialized durable commit
  publication boundary.
- Split write acceptance/preflight state from publish-time routing, sequence
  assignment, WAL append, memtable delta publication, and visibility marking.
- Keep accepted write state local to the executing writer task until it enters
  the publish barrier.
- Preserve existing blocking and async write behavior.
- Preserve commit tracker, WAL/table/blob/manifest formats, MVCC, compaction,
  recovery, cleanup, and storage behavior.

## Out Of Scope

- Removing the serialized publish boundary.
- Adding WAL shards or writer-local deltas.
- Adding channels or an external async executor dependency.
- Changing runtime-owned async write execution.
- Changing transaction conflict rules.
- Changing persistent native-file blocking behavior.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the writer-local accepted state and publish barrier
  phase.
- `DbInner` has a named publish barrier instead of an anonymous writer mutex.
- Write acceptance/preflight returns an explicit writer-local state.
- Publish-time routing and commit visibility run under the named publish
  barrier.
- Blocking and async write behavior remains unchanged.
- Focused write/concurrency tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining blockers for WAL partitioning and writer-local
  deltas.

## Active Task Slice

```text
task310 [x] goal:start writer-local publish barrier slice | scope:current roadmap | verify:manual
task311 [x] goal:add named publish barrier around current serialized publish boundary | scope:src/db.rs | verify:focused tests
task312 [x] goal:split accepted write preflight from publish-time local state | scope:src/db/commit.rs | verify:unit tests
task313 [x] goal:preserve blocking and async write behavior | scope:tests | verify:focused tests
task314 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task315 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- True multi-writer execution still needs writer-local deltas and WAL
  partitioning.
- Native-file storage remains blocking behind native-thread runtime execution.

## Evidence

- Phase 76 added cancellation tokens and task join primitives.
- Phase 77 added owned `WriteRequest`, `AcceptedWrite`, and `WriteWaiter`.
- Evidence from Phase 77 recommends moving accepted write execution behind the
  runtime boundary while preserving cancellation-before-poll and
  terminal-after-acceptance behavior.
- Phase 78 moved async accepted writes behind the runtime boundary.
- Phase 78 evidence says accepted writes are runtime-owned after first poll,
  but current durable commit publication is still serialized by the writer
  coordinator.
- `DbInner` now uses a named `PublishBarrier`.
- Commit, flush publish, compaction publish, public flush freezing, and close
  now enter the named publish barrier instead of an anonymous writer mutex.
- Write acceptance/preflight now returns explicit `AcceptedWriteState` and
  `WriterLocalWriteState`.
- Transaction validation, routing, sequence assignment, WAL append, memtable
  delta publication, visibility marking, and post-commit active-memtable freeze
  run under the publish barrier.
- Focused tests cover preflight without publication and direct writer-local
  state publication under the publish barrier.
- Verification passed: `cargo fmt --check`, `cargo clippy --all-targets
  --all-features -- -D warnings`, `cargo test accepted_write --lib`,
  `cargo test writer_local_state --lib`, `cargo test --test async_api`,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan outside the repository instruction file.

## Next Recommendation

- Commit this slice, then choose whether the next phase introduces writer-local
  deltas or WAL partitioning.
