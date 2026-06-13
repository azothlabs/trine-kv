# Current Phase

## Status

Complete

## Goal

Revalidate engine write, flush, compaction, maintenance, cleanup, and close
paths against the corrected cross-platform platform-io contract, then choose
the next async phase from evidence.

## Scope

- Use public platform-io diagnostics and storage operation counters to validate
  the current async write and public flush paths.
- Audit compaction, maintenance, cleanup, and close paths for the storage
  operations they depend on and whether those operations are currently awaited
  through platform-io or still run behind runtime/task boundaries.
- Record the next phase from fresh evidence instead of carrying forward an old
  Linux-only residual list.
- Keep this phase diagnostic and decision-focused unless a small missing test or
  doc update is required to prove the classification.

## Backend Boundary Receipt

- Owned public surface: `DbStats` storage operation counters and platform-io
  operation counters.
- Owned internal paths: write/WAL append, public flush, compaction table write
  and cleanup, cooperative maintenance, blob cleanup, and database close.
- Operation names remain: length lookup, owned random read, whole-object read,
  temporary write plus rename publish, append open, append, persist/fsync, WAL
  rewrite, delete, directory create, directory sync, directory listing, and
  writer lease.
- Current platform-io class meanings remain unchanged.
- Leak-check scope: KV engine code should depend on Trine storage operations
  and runtime capabilities, not OS-specific async mechanisms.
- Verification gate: targeted tests for write/flush diagnostics, audit evidence
  for compaction/maintenance/cleanup/close, and full local gates if code or docs
  change.

## Out Of Scope

- Implementing new OS-specific platform backends.
- Changing storage formats, WAL/manifest/table semantics, MVCC, transaction
  semantics, or durability semantics.
- Rewriting compaction or maintenance in this phase unless evidence shows a
  small local correction is required for diagnostics.
- Publishing, tagging, pushing, or creating a GitHub release.

## Acceptance Gate

- Existing native async write and flush paths are validated against the
  operation table across available platforms.
- Compaction, maintenance, cleanup, and close have recorded storage-operation
  dependencies and current async/fallback class limits.
- The next engine async phase is chosen from evidence.
- Phase completion is recorded in evidence and committed before starting the
  next phase.

## Active Task Slice

```text
task704 [x] goal:start engine path revalidation phase | scope:current roadmap | verify:phase brief
task705 [x] goal:audit write and flush diagnostics | scope:src/db.rs tests/async_api.rs | verify:targeted tests
task706 [x] goal:audit compaction and maintenance storage dependencies | scope:src/db.rs src/storage.rs | verify:evidence table
task707 [x] goal:audit cleanup and close storage dependencies | scope:src/db.rs src/storage.rs | verify:evidence table
task708 [x] goal:choose next async phase from evidence | scope:evidence roadmap current | verify:updated docs
task709 [x] goal:verify and commit Phase 159 | scope:tests docs git | verify:commit
```

## Evidence

- Phase 158 made operation-level platform-io totals public and tested on Linux
  and local non-Linux fallback paths.
- Existing async write and flush tests assert Linux platform-io operation
  counters for append, temporary write plus rename publish, and WAL rewrite.
- `flush_native_async()` awaits async table writes, directory sync, manifest
  publish, and WAL rewrite through storage operations, but cleanup still uses
  synchronous delete helpers.
- Native `compact_range`, budgeted compaction, and native maintenance still wrap
  sync implementations in `run_native_blocking_task`.
- Native compaction output table/blob writes use synchronous table/blob writers.
- Native close still wraps `close_sync()` in `run_native_blocking_task` and
  performs shutdown, publish-barrier waiting, cleanup, and writer lease release
  as one lifecycle boundary.
- Docker Linux platform-io diagnostics prove compaction writes storage output
  tables without increasing the true-platform-async temporary write/rename
  counter.

## Known Residuals

- Real Windows, FreeBSD, and illumos runtime diagnostics remain external to this
  local host.
- Real Windows, FreeBSD, and illumos runtime diagnostics remain external to this
  local host.
- Close remains a lifecycle boundary for a later phase because it couples worker
  shutdown, publish-barrier waiting, cleanup, and writer lease release.

## Next Recommendation

- Commit Phase 159, then start Phase 160: native async compaction output and
  cleanup.
