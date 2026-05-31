# Current Phase

## Status

Complete

## Goal

Attach persistent database construction and DB-owned storage helpers to a
runtime-enabled native-file backend while keeping existing blocking decode paths
explicit.

## Scope

- Add a persistent native-file backend field to `DbInner` and construct it with
  the database runtime.
- Route DB-owned directory create/sync, object cleanup deletes, manifest store,
  WAL read/rewrite, and WAL append construction through that backend.
- Preserve no-runtime behavior for standalone module helpers and tests.
- Preserve existing borrowed blocking read paths for current table/blob decode
  code and standalone table/blob helpers.
- Preserve existing public async API, blocking API, publish barrier, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Adding true async file I/O.
- Adding a public executor dependency.
- Adding public runtime tuning options.
- Converting table/blob/block decode call sites to async advancement.
- Migrating standalone recovery scanning to a runtime-enabled backend.
- Migrating table/blob module standalone helpers to runtime-enabled backends.
- Removing the serialized publish barrier.
- Adding WAL shards or writer-local deltas.
- Changing transaction conflict rules.
- Changing persistent native-file blocking behavior.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the persistent DB runtime-enabled native storage
  migration phase.
- Persistent `DbInner` owns a runtime-enabled native-file backend.
- Persistent manifest store creation and manifest publish use the DB-owned
  native backend.
- Persistent WAL read/rewrite and WAL append construction use the DB-owned
  native backend.
- DB-owned directory create/sync and cleanup deletes use the DB-owned native
  backend.
- Blocking storage adapters remain direct synchronous paths.
- Existing borrowed blocking read paths remain unchanged for current decode
  code.
- Focused DB/storage tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining async blockers.

## Active Task Slice

```text
task340 [x] goal:start persistent DB runtime-enabled storage backend slice | scope:current roadmap | verify:manual
task341 [x] goal:add DB-owned native storage backend constructed with runtime | scope:src/db.rs | verify:db tests
task342 [x] goal:route manifest store and WAL construction through DB backend | scope:src/manifest.rs src/wal.rs src/db.rs | verify:db tests
task343 [x] goal:route DB-owned directory sync/create and cleanup deletes through DB backend | scope:src/db.rs | verify:persistent tests
task344 [x] goal:preserve standalone no-runtime helpers and decode paths | scope:src/table.rs src/blob.rs src/recovery.rs | verify:full tests
task345 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- True async file I/O is not implemented.
- Runtime tuning options are still internal.
- Table/blob decode paths still use blocking native-file calls.
- Range and prefix cursor advancement still expose async compatibility wrappers
  over synchronous iterator advancement.
- True multi-writer execution still needs writer-local deltas and WAL
  partitioning.
- Standalone recovery scanning still constructs no-runtime native-file backends.
- Table/blob standalone helpers still construct no-runtime native-file
  backends.
- Database read decode paths still use blocking table/blob readers.

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

## Next Recommendation

- Finish this slice, then migrate table/blob standalone helpers or recovery
  scanning in a separate measured phase.
