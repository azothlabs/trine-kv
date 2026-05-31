# Current Phase

## Status

Complete

## Goal

Reconcile async storage implementation plan and route object listing through
the storage backend contract.

## Scope

- Lift the async storage protocol staging into the active implementation track.
- Map completed storage-boundary phases to the protocol's staged plan.
- Add an internal storage object listing backend operation.
- Implement native-file object listing for table objects.
- Route table file id listing through the backend operation.
- Keep block cache behavior, stats, cache keys, and SSTable format unchanged.
- Keep SSTable, WAL, manifest, blob, compaction, transaction, and public API
  behavior unchanged.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing public async APIs.
- Introducing an extent allocator or disk-space reuse layer.
- Changing SSTable block format, codec ids, checksums, footer layout, or cache
  key semantics.
- Moving table writes, WAL append, blob reads, or file cleanup to the new
  adapter in this slice.
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
- Native-file backend exposes an object listing operation for storage object
  kinds.
- Native-file backend reports object listing capability before table discovery
  uses that operation.
- Table file id listing uses the backend object listing operation.
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
task205 [x] goal:reconcile async storage implementation track | scope:current roadmap protocol | verify:manual
task206 [x] goal:add native-file object listing operation | scope:src/storage.rs | verify:storage tests
task207 [x] goal:route table file listing through backend | scope:src/table.rs | verify:table tests
task208 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this object listing slice.
- Public async API, async runtime selection, table writes, manifest, WAL, blob
  files, lease handling, cleanup deletion routing, and production in-memory
  table-object routing remain later phases.

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
- Verification passed: `cargo test storage --lib`, `cargo test table --lib`,
  `cargo test block --all-targets`, `cargo test persistent --all-targets`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, `git diff --check`, and forbidden-term scan.

## Next Recommendation

- Route table output writes through storage backend operations next; deletion
  routing can follow after output object creation has a backend-owned shape.
