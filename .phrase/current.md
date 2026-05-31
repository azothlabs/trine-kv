# Current Phase

## Status

Complete

## Goal

Introduce an owned block-read seam so table/blob block decode reads through an
owned, `Arc`-backed completion (`StorageReadBuffer`) instead of a borrowed
`&mut [u8]`, decoupling read completion from decode without changing scheduling
for today's synchronous callers.

## Scope

- Add `read_exact_at_owned` to the `BlockReadSource` trait with a borrowed
  fallback default that copies into an owned buffer.
- Route `BlockManager::read_checked_from_source` and
  `read_checked_at_source_offset` through the owned read completion before
  decode.
- Add `StorageReadBuffer::from_vec` and `as_slice` accessors so decode can run
  on the owned buffer.
- Override `read_exact_at_owned` in `StorageReadSource` and
  `NativeFileReadSource` to use the storage object's owned blocking read.
- Add focused block tests covering the owned default fallback and owned-seam
  decode for both block read entry points.
- Preserve existing public async API, blocking API, publish barrier, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Adding true async block decode or cursor advancement.
- Coupling synchronous decode callers to the runtime blocking queue.
- Converting table header/footer metadata reads to the owned seam.
- Adding a public executor dependency or public runtime tuning options.
- Changing public storage formats or recovery protocol.
- Removing standalone no-runtime table/blob/recovery helper wrappers.

## Acceptance Gate

- Roadmap records this as the owned block-read seam phase.
- `BlockReadSource` exposes `read_exact_at_owned` returning a
  `StorageReadBuffer`, with a borrowed fallback default.
- Both block decode entry points read owned buffers before decode.
- Native-file sources override the owned read to use the object's owned
  blocking read; synchronous callers remain decoupled from the runtime queue.
- Borrowed `read_exact_at` remains available for non-block reads.
- Focused block tests, formatting, clippy, full tests, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records remaining async blockers.

## Active Task Slice

```text
task358 [x] goal:add StorageReadBuffer owned accessors | scope:src/storage.rs | verify:build
task359 [x] goal:add owned BlockReadSource path and route both decode entry points | scope:src/block.rs | verify:block tests
task360 [x] goal:override owned read in native sources | scope:src/storage.rs | verify:storage tests
task361 [x] goal:add focused owned-seam block tests | scope:src/block.rs | verify:block tests
task362 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- True async file I/O is not implemented.
- Runtime tuning options are still internal.
- Block decode now reads owned completions but still runs synchronously on the
  calling thread; it does not yet await through the runtime owned-read boundary.
- Table header/footer metadata reads still use the borrowed `read_exact_at`
  path.
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
- `BlockReadSource` now exposes `read_exact_at_owned`, returning an
  `Arc`-backed `StorageReadBuffer`, with a borrowed fallback default for
  generic sources.
- `BlockManager::read_checked_from_source` and `read_checked_at_source_offset`
  now read owned completions before decode; the offset-addressed path reads the
  header and the full block as two owned reads.
- `StorageReadSource` and `NativeFileReadSource` override the owned read to use
  the storage object's owned blocking read, so the seam exists without coupling
  synchronous decode callers to the runtime blocking queue (consistent with the
  Phase 83 rule that synchronous callers stay off the queue).
- Added focused block tests: owned default-fallback equivalence, and owned-seam
  decode for both block read entry points.
- Table header/footer metadata reads were intentionally left on the borrowed
  path.
- Verification passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test block --lib`, `cargo test storage --lib`,
  `cargo test table --lib`, and `cargo test --all-targets --all-features`.
- `git diff --check` passed. Forbidden-term scan passed outside the repository
  instruction file.

## Next Recommendation

- The owned read seam is now the single choke point for block decode reads. The
  next phase should measure (trace/benchmark) block-decode read latency under a
  runtime to decide whether to drive decode through the async
  `read_exact_at_owned` boundary, and define the cursor advancement shape that
  would let an async caller await the owned completion before decode.
