# Current Phase

## Status

Complete

## Goal

Route recovery startup checks and safe temporary file repair through explicit
native-file backend boundaries while preserving standalone helper behavior and
fail-closed recovery semantics.

## Scope

- Add internal `with_backend` entry points for recovery process-lock acquire,
  safe temporary file scan/repair/report, missing referenced blob checks,
  invalid referenced blob checks, and unreferenced table/blob scans.
- Add a backend-taking blob validation helper for recovery validation.
- Route persistent `Db` open-time recovery checks through the DB-owned native
  backend.
- Preserve no-runtime behavior for standalone recovery helpers and tests.
- Preserve existing fail-closed recovery behavior and borrowed blocking
  table/blob decode paths.
- Preserve existing public async API, blocking API, publish barrier, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Adding true async file I/O.
- Adding a public executor dependency.
- Adding public runtime tuning options.
- Converting table/blob/block decode call sites to async advancement.
- Changing public recovery report read helper behavior.
- Removing standalone no-runtime table/blob helper wrappers.
- Removing standalone no-runtime recovery helper wrappers.
- Removing the serialized publish barrier.
- Adding WAL shards or writer-local deltas.
- Changing transaction conflict rules.
- Changing persistent native-file blocking behavior.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the recovery backend-boundary migration phase.
- Recovery module exposes crate-internal `with_backend` helpers for process
  lock acquisition, safe temporary file repair, referenced blob validation, and
  unreferenced formal file scanning.
- Persistent `Db` open-time recovery checks use the DB-owned native backend.
- Blob module exposes a backend-taking full-file validation helper for
  recovery.
- Standalone recovery wrappers still construct no-runtime native backends.
- Blocking storage adapters remain direct synchronous paths.
- Existing borrowed blocking read paths remain unchanged for current decode
  code.
- Focused DB/storage tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining async blockers.

## Active Task Slice

```text
task352 [x] goal:start recovery backend-boundary migration slice | scope:current roadmap | verify:manual
task353 [x] goal:add recovery with_backend helpers while preserving standalone wrappers | scope:src/recovery.rs | verify:recovery tests
task354 [x] goal:add backend-taking blob validation helper | scope:src/blob.rs | verify:recovery tests
task355 [x] goal:route persistent open recovery checks through DB-owned backend | scope:src/db.rs | verify:persistent recovery tests
task356 [x] goal:preserve recovery semantics and decode behavior | scope:src/recovery.rs src/db.rs | verify:full tests
task357 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- True async file I/O is not implemented.
- Runtime tuning options are still internal.
- Table/blob decode paths still use blocking native-file calls.
- Range and prefix cursor advancement still expose async compatibility wrappers
  over synchronous iterator advancement.
- True multi-writer execution still needs writer-local deltas and WAL
  partitioning.
- Database read decode paths still use blocking table/blob readers.
- True async table/blob decode is not implemented.
- Public recovery report reads remain standalone no-runtime helpers by design.

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
- Native-file owned storage mutations now share a runtime-owned task helper
  that submits owned closures to the bounded blocking adapter when a native
  runtime is attached.
- Runtime-enabled object writes/deletes, object listing, WAL rewrite,
  writer-lease acquire, directory create/list/sync, manifest read/publish, and
  append open/append/persist now wait behind an occupied blocking worker.
- Native-file blocking storage adapters now call the native synchronous
  operations directly, so synchronous callers are not coupled to runtime queue
  state.
- Blocking trait defaults still provide an async-to-blocking bridge for future
  backends that do not need native-file direct overrides.
- Inline runtime and no-runtime native-file mutation paths remain immediately
  pollable.
- Existing table/blob block decode paths still use borrowed blocking reads and
  were not converted in this phase.
- Verification passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test storage --lib`, and
  `cargo test --all-targets --all-features`.
- `git diff --check` passed.
- Forbidden-term scan passed outside the repository instruction file.
- Persistent `DbInner` now owns a native-file backend constructed with the
  database runtime.
- Persistent open routes manifest store creation, WAL replay reads, and WAL
  append construction through the DB-owned native backend.
- Flush cleanup, compaction cleanup, blob cleanup, directory sync, directory
  create, WAL rewrite/reopen, stats file length reads, and drop-time cleanup
  now use the DB-owned native backend.
- Standalone manifest/WAL helpers preserve no-runtime behavior by constructing
  their own native backend.
- Standalone table/blob/recovery helpers and borrowed blocking decode paths
  were not migrated in this phase.
- Verification passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --lib`, and `cargo test --all-targets --all-features`.
- `git diff --check` passed.
- Forbidden-term scan passed outside the repository instruction file.
- Table helpers now expose crate-internal `with_backend` paths for table file
  listing, writes, and metadata reads while preserving no-runtime wrappers.
- Blob helpers now expose crate-internal `with_backend` paths for blob file
  listing, blob writes, large-value rewrite, inline rewrite, metadata reads,
  and indexed value reads while preserving no-runtime wrappers.
- Persistent `Db` flush, compaction output build, blob GC rewrite/candidate
  reads, reopen table loading, and stale/obsolete blob stats now use the
  DB-owned native-file backend.
- Standalone table/blob no-runtime wrappers are covered by focused unit tests.
- Recovery validation still uses standalone no-runtime scanning helpers.
- Existing table/blob block decode still uses blocking native-file reads.
- Verification passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --lib`, and `cargo test --all-targets --all-features`.
- Recovery now exposes backend-taking process-lock acquire, safe temporary
  repair/report, missing referenced blob checks, invalid referenced blob checks,
  and unreferenced table/blob scan helpers.
- Blob validation now has a backend-taking full-file validation helper used by
  recovery.
- Persistent `Db` open-time recovery paths now use the DB-owned native-file
  backend for process lock acquisition, safe temporary repair, referenced blob
  validation, and unreferenced formal file scanning.
- Standalone recovery wrappers preserve no-runtime behavior by constructing
  their own native backend.
- Added a focused recovery unit test for backend-taking safe temporary file
  repair and recovery report write/read.
- Existing table/blob block decode still uses blocking native-file reads.
- Verification passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test recovery --lib`, `cargo test --lib`, and
  `cargo test --all-targets --all-features`.

## Next Recommendation

- Define the measured table/blob decode async-read phase, including cursor
  advancement shape and recovery validation constraints.
