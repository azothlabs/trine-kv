# Current Phase

## Status

Complete

## Goal

Route recovery safe temporary file scanning/deletion and referenced blob
existence checks through storage backend operations.

## Scope

- Add a backend operation for listing regular files in a native-file directory.
- Route recovery safe temporary file scanning through backend directory
  listing.
- Route recovery safe temporary file repair deletion through backend object
  deletion.
- Route referenced blob existence checks through backend object open.
- Preserve missing-directory behavior for safe temporary file scanning.
- Preserve WAL/table/blob/manifest formats, recovery policy, MVCC visibility,
  compaction behavior, stats behavior, cleanup behavior, and public API
  behavior.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Moving production in-memory object routing into backend operations.
- Changing recovery policy for unreferenced formal table/blob files.
- Changing recovery report format.

## Acceptance Gate

- Roadmap records the recovery directory-list backend phase at phase
  granularity.
- Current phase records the recovery policy boundaries and out-of-scope items.
- Native-file backend reports directory-list capability.
- Native-file backend exposes async and blocking directory-file listing.
- Safe temporary file scanning uses backend directory listing.
- Safe temporary file deletion uses backend object deletion.
- Referenced blob existence checks use backend object open.
- Existing fail-closed and repair-safe-temporary recovery behavior remains the
  same.
- `cargo fmt --check`, focused storage/recovery tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records remaining direct native-file operations after the slice.

## Active Task Slice

```text
task264 [x] goal:start recovery directory-list backend slice | scope:current roadmap | verify:manual
task265 [x] goal:add directory-file listing operation | scope:src/storage.rs | verify:storage tests
task266 [x] goal:route recovery temp scan/delete and blob existence checks | scope:src/recovery.rs | verify:persistent recovery tests
task267 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task268 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this recovery directory-list slice.
- Public async API, async runtime selection, and production in-memory object
  routing remain later phases.

## Evidence

- Recovery safe temporary file scan still uses direct native directory reads.
- Repair-safe-temporary recovery still deletes safe temporary files directly.
- Referenced blob existence checks still use direct native path inspection.
- Existing storage object delete and random-read operations are sufficient for
  deletion and existence checks.
- A directory-file listing operation is the missing backend boundary for
  recovery scan.
- Native-file storage now reports `StorageCapability::DirectoryListing`.
- Native-file storage now exposes async and blocking directory-file listing for
  regular files.
- Recovery safe temporary file scanning now uses backend directory listing.
- Repair-safe-temporary recovery now deletes temporary objects through backend
  object deletion.
- Referenced blob existence checks now use backend object open.
- Focused verification passed: `cargo test storage --lib` and
  `cargo test persistent_recovery --test persistent_wal`.
- Full verification passed: `cargo fmt --check`, `cargo clippy
  --all-targets --all-features -- -D warnings`, `cargo test --all-targets
  --all-features`, `git diff --check`, and forbidden-term scan.
- Direct native-file operation audit outside storage/durability now shows
  remaining matches as test setup/cleanup or method names rather than
  production recovery/stats file operations.

## Next Recommendation

- Commit this slice, then reassess the next production boundary from fresh
  evidence.
