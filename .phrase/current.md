# Current Phase

## Status

Complete

## Goal

Route memory storage objects through the async read contract.

## Scope

- Add a volatile memory storage read backend and read object using the same
  internal async read trait shape as native-file storage.
- Add memory backend capability reporting for volatile random reads.
- Exercise table-byte decoding through a memory storage object so checked
  table reads can use the same read contract.
- Keep block cache behavior, stats, cache keys, and SSTable format unchanged.
- Keep SSTable, WAL, manifest, blob, compaction, transaction, and public API
  behavior unchanged.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing public async APIs.
- Introducing an extent allocator or disk-space reuse layer.
- Changing SSTable block format, codec ids, checksums, footer layout, or cache
  key semantics.
- Moving table writes, manifest publish, WAL append, blob reads, or file cleanup
  to the new adapter in this slice.
- Defining or routing full write, manifest publish, lease, cleanup, runtime, or
  public async API traits in this slice.
- Reworking in-memory DB flush behavior or making memory mode create SSTable
  storage objects in production paths.
- Moving MVCC visibility, table version lifetime, compaction planning, manifest
  publish, blob GC, or public API behavior.

## Acceptance Gate

- Memory storage backend implements the same read backend/object traits as
  native-file storage.
- Memory backend reports volatile random-read capability and does not claim
  persistence or write/publish guarantees.
- Table-byte decode coverage reads via the memory storage object and checked
  block source path.
- Persistent table open/header/footer/startup metadata reads still go through a
  native-file storage object and adapter keyed by a storage object id.
- Persistent table checked-block reads continue to go through the same adapter.
- Existing table read/write behavior and storage format remain unchanged.
- Existing persistent/table/block-cache tests pass.
- `cargo fmt --check`, focused Rust tests, clippy, and `git diff --check`
  pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task193 [x] goal:start memory storage read slice | scope:current roadmap protocol | verify:manual
task194 [x] goal:add volatile memory read backend/object | scope:src/storage.rs | verify:storage tests
task195 [x] goal:exercise table-byte reads through memory object | scope:src/table.rs | verify:table tests
task196 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this memory storage read slice.
- Public async API, async runtime selection, table writes, manifest, WAL, blob
  files, lease handling, cleanup, and production in-memory table-object routing
  remain later phases.

## Evidence

- Phase 50 defined the first async read trait shape and native-file blocking
  adapter.
- Phase 51 added typed capability checks and unsupported backend/durability
  errors.
- The async-first protocol names the memory backend as volatile, immediately
  completable, and the baseline for WASM logical correctness tests.
- The next useful slice is a memory read backend that proves the same storage
  read contract can serve non-file byte objects without changing public API or
  production in-memory DB behavior.
- `src/storage.rs` now has `MemoryStorageBackend` and `MemoryStorageObject`
  implementing the same read traits as `NativeFileBackend`.
- Memory storage reports volatile random-read capability and rejects persistent
  capability checks.
- `src/table.rs` test decode now opens table bytes through a memory storage
  object and reads header, footer, checked blocks, filters, and indexes through
  `StorageReadSource`.
- Persistent table reads now use `StorageReadSource` for already-opened native
  read objects, while the native-file fallback source remains for lazy paths
  that do not hold an opened object.
- Verification passed: `cargo test storage --lib`, `cargo test table --lib`,
  `cargo test block --all-targets`, `cargo test persistent --all-targets`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, `git diff --check`, and forbidden-term scan.

## Next Recommendation

- After memory read coverage lands, add write/publish trait methods behind the
  capability checks.
