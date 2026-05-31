# Current Phase

## Status

Complete

## Goal

Route blob file creation through the storage backend object write operation.

## Scope

- Add blob storage objects to the internal storage object kind model.
- Let native-file object writes handle blob objects through the existing
  complete-object write operation.
- Route `write_blob_file` output-file creation through the backend operation.
- Preserve current blob write ordering: encode bytes, write temporary file,
  sync the file, rename to the final blob path, and return the same indexes.
- Keep parent-directory sync batching owned by existing flush/compaction
  callers.
- Keep blob read paths, blob file format, table format, cache behavior, stats,
  manifest publish, WAL, MVCC, compaction, transaction, and public API behavior
  unchanged.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing public async APIs.
- Moving blob reads, blob listing, WAL append, cleanup deletion, or manifest
  publish into a different backend operation in this slice.
- Changing parent-directory sync batching or manifest publish ordering.
- Defining full storage lease, cleanup, runtime, or public async API traits.
- Changing blob file format, compression, checksums, record layout, or table
  `BlobIndex` metadata.
- Moving MVCC visibility, table version lifetime, compaction planning, blob GC,
  manifest state transitions, or public API behavior.

## Acceptance Gate

- Roadmap records the blob object write phase at phase granularity.
- Current phase records which protocol stages are complete and which storage
  operations remain.
- Storage object kinds include blob objects.
- Native-file backend writes blob objects through the generic object write
  operation and still rejects manifest objects.
- `write_blob_file` uses the backend write operation while preserving bytes,
  temporary-file naming, file sync, final rename, and returned indexes.
- Existing persistent large-value flush, compaction, GC, and recovery tests
  pass.
- `cargo fmt --check`, focused Rust tests, clippy, and `git diff --check`
  pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task213 [x] goal:start blob object write backend slice | scope:current roadmap evidence protocol | verify:manual
task214 [x] goal:add blob storage object kind | scope:src/storage.rs | verify:storage tests
task215 [x] goal:route blob output creation through backend | scope:src/blob.rs | verify:blob persistent tests
task216 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this blob object write slice.
- Public async API, async runtime selection, WAL append, blob reads, blob
  listing, lease handling, cleanup deletion routing, parent-directory sync
  routing, and production in-memory table/blob-object routing remain later
  phases.

## Evidence

- Phase 56 routed table output-file creation through the backend object write
  operation.
- Blob file creation had the same native-file write shape as table output
  creation: complete byte buffer, temporary file, file sync, final rename.
- `StorageObjectKind::Blob` now names blob objects.
- `NativeFileBackend` writes blob objects through `StorageObjectWriteBackend`
  and still rejects manifest objects.
- `write_blob_file` now routes encoded blob bytes through the backend write
  operation and returns the same encoded record indexes.
- Parent-directory sync batching remains in existing flush/compaction callers.
- Verification passed: `cargo test storage --lib`, `cargo test blob --lib`,
  `cargo test table --lib`, `cargo test persistent --all-targets`,
  `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan.

## Next Recommendation

- Route cleanup deletion or blob read/list operations through storage backend
  operations, keeping public API behavior and storage formats unchanged.
