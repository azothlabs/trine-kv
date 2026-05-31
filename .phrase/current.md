# Current Phase

## Status

Complete

## Goal

Route persistent writer lease acquisition and release through the storage
backend writer-lease operation.

## Scope

- Add a writer-lease storage object kind.
- Add backend writer-lease acquisition traits.
- Implement native-file writer leases using the existing `LOCK` marker
  semantics.
- Keep fail-closed behavior when a writer lease marker already exists.
- Keep release safety: only remove the lease marker when the stored owner text
  still matches this handle.
- Preserve read-only open behavior, close/drop release behavior, recovery,
  manifest, WAL, table/blob formats, MVCC visibility, compaction, and public API
  behavior.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Changing the native `LOCK` file name, owner text format, or stale-lock repair
  policy.
- Moving WAL rewrite-after-flush, manifest publish, table reads,
  parent-directory sync, or production in-memory object routing into different
  backend operations.
- Implementing browser/WASI writer leases.
- Changing WAL format, table/blob formats, MVCC visibility, compaction planning,
  recovery policy, or public API behavior.

## Acceptance Gate

- Roadmap records the writer lease backend phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- Native-file backend reports writer lease capability.
- Native-file backend acquires writer leases through a backend operation and
  rejects an existing lease marker fail-closed.
- Native-file backend releases only the lease marker it owns.
- Persistent writable open uses the backend writer-lease operation.
- Read-only open still does not acquire a writer lease.
- Existing writer-lock, recovery, WAL, and persistent tests pass.
- `cargo fmt --check`, focused Rust tests, clippy, `cargo test --all-targets
  --all-features`, `git diff --check`, and forbidden-term scan pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task233 [x] goal:start writer lease backend slice | scope:current roadmap | verify:manual
task234 [x] goal:add storage writer lease traits and native-file implementation | scope:src/storage.rs | verify:storage tests
task235 [x] goal:route ProcessLock through backend writer lease | scope:src/recovery.rs src/db.rs | verify:writer-lock tests
task236 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this writer lease slice.
- Public async API, async runtime selection, parent-directory sync routing, WAL
  rewrite routing, and production in-memory object routing remain later phases.

## Evidence

- Phase 61 routed WAL append and persist through backend append operations.
- The async storage protocol requires writer lease support for persistent
  writable open.
- Current persistent writable open uses `recovery::ProcessLock` and native
  lock-file operations directly.
- Existing tests cover held-lock failure, stale-lock fail-closed behavior, and
  read-only open not taking a writer lock.
- `StorageObjectKind::WriterLease`, `StorageWriterLeaseBackend`, and
  `NativeFileWriterLease` now name the writer lease boundary.
- Native-file writer leases preserve the existing `LOCK` marker, fail-closed
  behavior, owner text sync, and owner-check release semantics.
- `recovery::ProcessLock` now wraps the backend writer lease instead of owning
  native file operations directly.
- Verification passed: `cargo test storage --lib`, focused writer-lock tests,
  `cargo fmt --check`, the full clippy gate, and
  `cargo test --all-targets --all-features`.

## Next Recommendation

- Route parent-directory sync or WAL rewrite maintenance through storage
  backend operations.
