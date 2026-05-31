# Current Phase

## Status

Complete

## Goal

Route table output writes through the storage backend contract.

## Scope

- Add an internal storage object write backend operation for complete object
  bytes.
- Implement native-file table object writes through the backend operation.
- Route `write_table` output-file creation through the backend operation.
- Preserve current table write ordering: encode bytes, write temporary file,
  sync the file, rename to the final table path, then reopen through the read
  backend.
- Keep parent-directory sync batching owned by the existing flush/compaction
  callers.
- Keep block cache behavior, stats, cache keys, and SSTable format unchanged.
- Keep SSTable, WAL, manifest, blob, compaction, transaction, and public API
  behavior unchanged.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing public async APIs.
- Introducing an extent allocator or disk-space reuse layer.
- Changing SSTable block format, codec ids, checksums, footer layout, or cache
  key semantics.
- Moving WAL append, blob reads/writes, or file cleanup to the new adapter in
  this slice.
- Changing parent-directory sync batching or manifest publish ordering.
- Defining or routing full write, lease, cleanup, runtime, or public async API
  traits in this slice.
- Reworking in-memory DB flush behavior or making memory mode create SSTable
  storage objects in production paths.
- Moving MVCC visibility, table version lifetime, compaction planning, blob GC,
  manifest state transitions, manifest publish, or public API behavior.

## Acceptance Gate

- Roadmap records the async storage implementation track at phase granularity,
  not as far-future task details.
- Current phase records which protocol stages are complete and which storage
  operations remain.
- Native-file backend reports object write capability before table writes use
  that operation.
- Native-file backend exposes a complete-object write operation for table
  objects.
- `write_table` uses the backend write operation while preserving bytes,
  temporary-file naming, file sync, final rename, and reopen behavior.
- Persistent table open/header/footer/startup metadata reads still go through a
  native-file storage object and adapter keyed by a storage object id.
- Persistent table checked-block reads continue to go through the same adapter.
- Existing manifest format, table read/write behavior, cleanup behavior, and
  storage format remain unchanged.
- Existing persistent/table/block-cache tests pass.
- `cargo fmt --check`, focused Rust tests, clippy, and `git diff --check`
  pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task209 [x] goal:start table object write backend slice | scope:current roadmap evidence protocol | verify:manual
task210 [x] goal:add native-file object write operation | scope:src/storage.rs | verify:storage tests
task211 [x] goal:route table output creation through backend | scope:src/table.rs | verify:table tests
task212 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this table object write slice.
- Public async API, async runtime selection, WAL append, blob files, lease
  handling, cleanup deletion routing, parent-directory sync routing, and
  production in-memory table-object routing remain later phases.

## Evidence

- Async storage protocol core operations include read, append, temp object
  write, object sync, manifest publish/read, object listing, deletion, and
  close.
- Protocol staging says to introduce async API/adapters, capabilities, memory
  backend, native-file backend, manifest publish, async cursors, blocking native
  adapter, WASM memory checks, then WASI/browser backends.
- Completed implementation slices cover block read source extraction,
  native-file table read objects, async read trait shape, capability/error
  types, memory read backend, native-file manifest publish, and native-file
  manifest read.
- Phase 53 routed manifest publish through the native-file storage backend.
- Phase 54 routed current-manifest reads through the native-file storage
  backend.
- The next smallest spec operation is native-file object listing for table
  objects, because recovery and cleanup already need table-id discovery and can
  keep the same validation behavior.
- `src/storage.rs` now has `StorageObjectListRequest`,
  `StorageObjectListBackend`, and `BlockingStorageObjectListBackend`.
- `NativeFileBackend` reports object listing capability, lists file objects
  under a native-file root, can filter by file extension, skips directories, and
  returns stable sorted object ids.
- `src/table.rs` now routes `list_table_file_ids` through the backend listing
  operation while keeping table filename validation in the table layer.
- The next smallest backend operation is table output write because `write_table`
  already owns one complete byte buffer and currently performs a direct native
  file write before reopening through the read backend.
- Existing flush/compaction callers batch parent-directory sync after one or
  more table/blob renames and before manifest publish; this slice must preserve
  that responsibility.
- `src/storage.rs` now has `StorageObjectWriteBackend` and
  `BlockingStorageObjectWriteBackend`.
- `NativeFileBackend` reports object write capability and writes table objects
  through the existing native-file sequence: create parent directory, write a
  temporary file, sync the file, and rename to the final object path.
- Manifest objects still use the manifest publish operation instead of generic
  object write.
- `src/table.rs` now routes encoded table bytes through the backend write
  operation and then reopens through the read backend.
- Parent-directory sync batching remains in the existing flush/compaction
  callers.
- Verification passed: `cargo test storage --lib`, `cargo test table --lib`,
  `cargo test block --all-targets`, `cargo test persistent --all-targets`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, `git diff --check`, and forbidden-term scan.

## Next Recommendation

- After table output writes land, route deletion cleanup or blob file writes
  through storage backend operations.
