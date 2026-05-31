# Current Phase

## Status

Complete

## Goal

Route native-file owned storage reads through the bounded runtime blocking
adapter when a runtime-enabled backend is used.

## Scope

- Add a result-bearing blocking runtime future for short-lived owned work.
- Allow native-file storage backends to carry a runtime boundary.
- Route native-file whole-object reads and owned read-buffer operations through
  the bounded blocking adapter when the backend/object has a native runtime.
- Preserve inline/no-runtime storage behavior.
- Preserve existing borrowed blocking read paths for current table/blob decode
  code.
- Preserve existing public async API, blocking API, publish barrier, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Adding true async file I/O.
- Adding a public executor dependency.
- Adding public runtime tuning options.
- Converting table/blob/block decode call sites to async advancement.
- Converting storage writes, append, manifest publish, and object listing to
  owned runtime-submittable requests.
- Removing the serialized publish barrier.
- Adding WAL shards or writer-local deltas.
- Changing transaction conflict rules.
- Changing persistent native-file blocking behavior.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the native-file runtime-owned storage read phase.
- Runtime exposes a result-bearing bounded blocking future.
- Native-file backends can be constructed with a runtime.
- Runtime-enabled native-file whole-object reads and owned read-buffer reads
  execute through the bounded blocking adapter.
- Inline/no-runtime storage reads remain immediately pollable.
- Existing borrowed blocking read paths remain unchanged for current decode
  code.
- Focused runtime/storage tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining async blockers.

## Active Task Slice

```text
task328 [x] goal:start native-file runtime-owned read slice | scope:current roadmap | verify:manual
task329 [x] goal:add result-bearing runtime blocking future | scope:src/runtime.rs | verify:runtime tests
task330 [x] goal:attach runtime boundary to native-file backend/object | scope:src/storage.rs | verify:storage tests
task331 [x] goal:route owned native-file reads through runtime when available | scope:src/storage.rs | verify:storage tests
task332 [x] goal:preserve borrowed blocking table/blob read paths | scope:src/storage.rs table/blob tests | verify:full tests
task333 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- True async file I/O is not implemented.
- Runtime tuning options are still internal.
- Table/blob decode paths still use blocking native-file calls.
- Range and prefix cursor advancement still expose async compatibility wrappers
  over synchronous iterator advancement.
- Storage writes, append, manifest publish, and object listing still need owned
  runtime-submittable request/completion wrappers.
- True multi-writer execution still needs writer-local deltas and WAL
  partitioning.
- Default native-file backend construction remains no-runtime until call sites
  are migrated.

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
- Phase 79 evidence recommends defining writer-local delta collection before
  WAL partitioning.
- Async remaining-work review identified the bounded runtime task scheduler as
  the next async foundation before true async read/write I/O.
- Native runtime mode now owns a lazy bounded blocking task pool with a fixed
  worker count and bounded queue.
- Blocking task submission returns `RuntimeBusy` when the queue is full and
  `Closed` when the pool is shutting down.
- Accepted async writes now enter the bounded blocking adapter after first poll
  instead of creating one thread per accepted write.
- Long-lived maintenance workers still use dedicated background tasks.
- Inline runtime async writes still run to completion without background
  threads.
- Verification passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --test async_api`, and
  `cargo test --all-targets --all-features`.
- `git diff --check` passed.
- Forbidden-term scan passed outside the repository instruction file.
- Phase 80 evidence recommends defining a shared storage async boundary before
  public executor or backend-specific work.
- Storage read objects now expose `read_exact_at_owned` returning
  `StorageReadBuffer`.
- Blocking storage read objects now expose `read_exact_at_owned_blocking`.
- Native whole-object reads now use owned read completion internally.
- Existing table/blob block decode call sites still use borrowed blocking reads
  and were not converted in this phase.
- Verification passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test storage --lib`, and
  `cargo test --all-targets --all-features`.
- `git diff --check` passed.
- Forbidden-term scan passed outside the repository instruction file.
- Runtime now exposes `spawn_blocking_result` for bounded blocking tasks that
  return typed results through a future.
- Runtime-enabled native-file backends now report async/background task
  capability and route owned read-buffer operations through the bounded
  blocking adapter.
- Runtime-enabled native-file whole-object reads now route through the bounded
  blocking adapter.
- Inline runtime and no-runtime native-file owned reads remain immediately
  pollable.
- Existing table/blob block decode call sites still use borrowed blocking reads
  and were not converted in this phase.
- Verification passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test runtime --lib`, `cargo test storage --lib`, and
  `cargo test --all-targets --all-features`.
- `git diff --check` passed.
- Forbidden-term scan passed outside the repository instruction file.

## Next Recommendation

- Add owned runtime-submittable wrappers for storage writes, append, manifest
  publish, and object listing before migrating DB call sites.
