# Current Phase

## Status

Complete

## Goal

Route native-file directory metadata sync through a storage backend operation.

## Scope

- Add a storage backend operation for syncing a directory after one or more
  atomic renames.
- Keep native-file directory sync behavior exactly aligned with the existing
  durability helper.
- Route WAL rewrite, recovery report publish, table/blob output publish
  barriers, compaction output publish barriers, and blob-GC output publish
  barriers through the backend directory-sync operation.
- Keep manifest publish inside the storage backend and make its parent-directory
  sync use the same backend-owned native-file helper.
- Preserve public API behavior, WAL frame bytes, manifest format, table/blob
  formats, MVCC visibility, compaction planning, recovery policy, and cleanup
  semantics.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Changing table, blob, WAL, manifest, or recovery report file formats.
- Changing WAL rewrite semantics beyond the directory-sync call boundary.
- Changing table/blob object write batching or forcing one directory sync per
  output file.
- Moving production in-memory object routing into backend operations.
- Adding browser/WASI directory sync implementations beyond current
  best-effort behavior.

## Acceptance Gate

- Roadmap records the directory-sync backend phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- Native-file backend reports a directory-sync capability.
- Native-file backend exposes async and blocking directory-sync operations.
- Native-file manifest publish uses a backend-owned directory sync helper after
  `SyncAll` rename publish.
- WAL rewrite and recovery report publish no longer call durability directory
  sync helpers directly.
- Flush, compaction, and blob-GC publish barriers no longer call durability
  directory sync helpers directly.
- Parent-directory sync batching for table/blob outputs remains a single
  directory sync before manifest publish.
- Existing recovery, WAL, storage, persistent, table/blob, and compaction tests
  pass.
- `cargo fmt --check`, focused Rust tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records how this adapter prepares the storage backend migration.

## Active Task Slice

```text
task237 [x] goal:start directory sync backend slice | scope:current roadmap | verify:manual
task238 [x] goal:add storage directory sync trait and native-file implementation | scope:src/storage.rs | verify:storage tests
task239 [x] goal:route WAL/recovery/db directory sync callers through backend | scope:src/wal.rs src/recovery.rs src/db.rs | verify:focused tests
task240 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this directory-sync slice.
- Public async API, async runtime selection, WAL rewrite storage-object routing,
  and production in-memory object routing remain later phases.

## Evidence

- Phase 62 routed persistent writer lease acquisition/release through backend
  operations.
- The remaining direct metadata durability calls are parent/directory syncs
  after atomic renames in WAL rewrite, recovery report publish, and table/blob
  output publish barriers.
- Table/blob output paths intentionally batch one directory sync after one or
  more renames and before manifest publish.
- `StorageDirectoryId`, `StorageDirectorySyncBackend`, and
  `BlockingStorageDirectorySyncBackend` now name the directory-sync boundary.
- Native-file backend now reports `StorageCapability::DirectorySync` and routes
  directory sync through the existing platform-specific durability helper.
- WAL rewrite, recovery report publish, flush output publish barriers,
  compaction output publish barriers, and blob-GC output publish barriers now
  call the backend directory-sync operation.
- Manifest publish remains inside the storage backend and uses the same
  backend-owned native-file directory-sync helper after `SyncAll` rename
  publish.
- Verification passed: `cargo test storage --lib`, `cargo test wal --lib`,
  `cargo test recovery --all-targets`, focused persistent flush/compaction/blob
  GC tests.

## Next Recommendation

- Route WAL rewrite maintenance or production in-memory object routing through
  storage backend operations after this directory-sync boundary is complete.
