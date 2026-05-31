# Current Phase

## Status

Complete

## Goal

Define an owned async storage read completion boundary so storage reads can
eventually cross runtime and portable backend boundaries without borrowing the
caller's output buffer.

## Scope

- Add an owned read-buffer completion API to storage read objects.
- Keep the existing borrowed blocking read path for current table/blob decode
  code.
- Implement owned read completion for memory and native-file storage objects.
- Add blocking adapter methods over the owned read completion API.
- Preserve existing public async API, blocking API, publish barrier, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Adding true async file I/O.
- Adding a public executor dependency.
- Adding public runtime tuning options.
- Routing native-file reads through the bounded blocking scheduler.
- Converting table/blob/block decode call sites to async advancement.
- Removing the serialized publish barrier.
- Adding WAL shards or writer-local deltas.
- Changing transaction conflict rules.
- Changing persistent native-file blocking behavior.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the owned async storage read completion phase.
- Storage read objects expose an owned read-buffer completion API.
- Memory and native-file storage objects implement the owned read-buffer API.
- Blocking storage read objects expose a blocking adapter for owned read
  completions.
- Existing borrowed blocking read paths remain unchanged for current decode
  code.
- Focused storage tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining async blockers.

## Active Task Slice

```text
task322 [x] goal:start owned async storage read completion slice | scope:current roadmap | verify:manual
task323 [x] goal:add owned read-buffer completion API | scope:src/storage.rs | verify:storage tests
task324 [x] goal:implement memory and native-file owned reads | scope:src/storage.rs | verify:storage tests
task325 [x] goal:preserve existing borrowed blocking read path | scope:src/storage.rs table/blob tests | verify:full tests
task326 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task327 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- True async file I/O is not implemented.
- Runtime tuning options are still internal.
- Storage reads still use blocking native-file calls behind the current decode
  paths.
- Range and prefix cursor advancement still expose async compatibility wrappers
  over synchronous iterator advancement.
- True multi-writer execution still needs writer-local deltas and WAL
  partitioning.
- Native-file storage remains blocking unless a later phase routes owned
  storage requests through the runtime boundary.

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

## Next Recommendation

- Route native-file owned storage requests through the bounded runtime blocking
  adapter, then convert table/blob read call sites and cursor advancement in
  separate measured slices.
