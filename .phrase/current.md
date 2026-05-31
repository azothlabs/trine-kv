# Current Phase

## Status

Complete

## Goal

Route persistent database directory creation through a storage backend
operation.

## Scope

- Add a backend operation for creating a native-file directory tree.
- Route persistent open directory creation through the backend operation.
- Keep existing create-if-missing and read-only behavior.
- Preserve WAL/table/blob/manifest formats, recovery policy, MVCC visibility,
  compaction, stats, and public API behavior.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Changing database path validation semantics.
- Routing safe temporary file listing/deletion.
- Routing stats metadata reads.
- Moving production in-memory object routing into backend operations.

## Acceptance Gate

- Roadmap records the directory-create backend phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- Native-file backend reports directory-create capability.
- Native-file backend exposes async and blocking directory-create operations.
- Persistent create-if-missing uses backend directory creation.
- Read-only missing database path still fails.
- Existing persistent open/create, storage, recovery, and full tests pass.
- `cargo fmt --check`, focused Rust tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records remaining direct native-file operations after the slice.

## Active Task Slice

```text
task256 [x] goal:start directory create backend slice | scope:current roadmap | verify:manual
task257 [x] goal:add directory create storage operation | scope:src/storage.rs | verify:storage tests
task258 [x] goal:route persistent open create-if-missing through backend | scope:src/db.rs src/wal.rs | verify:persistent tests
task259 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this directory-create slice.
- Public async API, async runtime selection, safe temporary file
  listing/deletion routing, stats metadata routing, and production in-memory
  object routing remain later phases.

## Evidence

- Persistent writable open creates a missing database directory before taking
  the writer lease.
- That creation still routes through `wal::ensure_parent_dir` and direct
  `create_dir_all`.
- Missing read-only database path already fails before directory creation.
- `StorageDirectoryCreateBackend` and
  `BlockingStorageDirectoryCreateBackend` now name the directory-create
  boundary.
- Native-file backend now reports `StorageCapability::DirectoryCreate`.
- Persistent create-if-missing now calls the backend directory-create operation
  before taking the writer lease.
- The old WAL-module directory creation helper was removed.
- Verification passed: `cargo test storage --lib` and focused persistent open
  coverage.

## Next Recommendation

- Reassess remaining direct native-file operations and choose between safe
  temporary file repair and stats metadata reads.
