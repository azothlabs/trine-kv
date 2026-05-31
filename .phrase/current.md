# Current Phase

## Status

Complete

## Goal

Add a bounded native blocking task scheduler so runtime-owned async work does
not create an unbounded thread per accepted write.

## Scope

- Add an internal bounded blocking task pool for native runtime mode.
- Keep long-lived background maintenance workers on dedicated background
  threads.
- Route accepted async writes through the bounded blocking adapter instead of
  spawning one thread per accepted write.
- Preserve inline runtime behavior.
- Preserve existing public async API, blocking API, publish barrier, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Adding true async file I/O.
- Adding a public executor dependency.
- Adding public runtime tuning options.
- Removing the serialized publish barrier.
- Adding WAL shards or writer-local deltas.
- Changing transaction conflict rules.
- Changing persistent native-file blocking behavior.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the bounded runtime blocking scheduler phase.
- Native runtime mode has a bounded blocking task pool.
- Blocking adapter submissions return a recoverable error when the queue is
  full or shutting down.
- Async writes use the blocking adapter after first poll.
- Inline runtime async writes still complete without background threads.
- Focused runtime/async tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining async blockers.

## Active Task Slice

```text
task316 [x] goal:start bounded runtime scheduler slice | scope:current roadmap | verify:manual
task317 [x] goal:add native bounded blocking task pool | scope:src/runtime.rs | verify:runtime tests
task318 [x] goal:route accepted async writes through blocking adapter | scope:src/db/commit.rs | verify:async tests
task319 [x] goal:preserve inline runtime and background worker behavior | scope:runtime/db tests | verify:focused tests
task320 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task321 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- True async file I/O is not implemented.
- Runtime tuning options are still internal.
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

## Next Recommendation

- Move to the next async foundation: define the storage async boundary so
  native-file I/O, memory mode, and future non-threaded backends can share one
  API shape before public executor integration or WASM backend work.
