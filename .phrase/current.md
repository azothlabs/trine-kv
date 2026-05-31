# Current Phase

## Status

Complete

## Goal

Route WAL rewrite-after-flush through a storage backend operation.

## Scope

- Add a backend operation for atomic WAL rewrite using an explicit temporary WAL
  object.
- Preserve the existing `trine.wal.tmp` temporary file name so recovery keeps
  recognizing safe WAL rewrite leftovers.
- Preserve WAL frame bytes, replay filtering, checksum behavior, torn-tail
  handling, and writer reopen behavior.
- Keep WAL append on the append backend operation.
- Keep parent-directory sync behind the storage backend.
- Preserve manifest, table/blob formats, MVCC visibility, compaction planning,
  recovery policy, cleanup semantics, and public API behavior.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Changing WAL record format or WAL rewrite retention policy.
- Changing WAL replay reads into a new optional-read backend operation.
- Changing table/blob object write batching or directory sync cadence.
- Moving production in-memory object routing into backend operations.

## Acceptance Gate

- Roadmap records the WAL rewrite backend phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- Native-file backend reports an atomic WAL rewrite capability.
- Native-file backend exposes async and blocking WAL rewrite operations.
- WAL rewrite rejects non-WAL objects and mismatched temporary object kinds.
- `rewrite_batches_after` builds replacement WAL bytes and uses the backend WAL
  rewrite operation.
- The rewrite temporary file remains `trine.wal.tmp`.
- Existing WAL, recovery, storage, persistent flush, and WAL corruption tests
  pass.
- `cargo fmt --check`, focused Rust tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records how this adapter closes the WAL append/rewrite split.

## Active Task Slice

```text
task241 [x] goal:start WAL rewrite backend slice | scope:current roadmap | verify:manual
task242 [x] goal:add storage WAL rewrite trait and native-file implementation | scope:src/storage.rs | verify:storage tests
task243 [x] goal:route rewrite_batches_after through backend | scope:src/wal.rs | verify:WAL/persistent tests
task244 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this WAL rewrite slice.
- Public async API, async runtime selection, WAL replay optional-read routing,
  and production in-memory object routing remain later phases.

## Evidence

- Phase 61 routed WAL append/persist through backend operations.
- Phase 63 routed parent-directory sync through backend operations.
- `rewrite_batches_after` still writes `trine.wal.tmp`, syncs it, renames it
  over `trine.wal`, and reopens append using direct native-file writes.
- Recovery treats `trine.wal.tmp` as a safe temporary file that may be repaired
  under the explicit repair policy.
- `StorageWalRewriteBackend` and `BlockingStorageWalRewriteBackend` now name
  the WAL rewrite boundary.
- Native-file backend now reports `StorageCapability::AtomicWalRewrite` and
  rewrites WAL bytes using explicit final and temporary WAL objects.
- WAL rewrite rejects non-WAL objects, same final/temp paths, and temporary
  objects outside the final WAL parent directory.
- `rewrite_batches_after` now filters/encodes retained WAL batches and delegates
  the temp write, file sync, rename, and parent-directory sync to the backend.
- Verification passed: `cargo test storage --lib`, `cargo test wal --lib`,
  focused persistent WAL/flush/recovery tests.

## Next Recommendation

- Route remaining direct native-file recovery-report write or WAL replay reads
  through storage backend operations.
