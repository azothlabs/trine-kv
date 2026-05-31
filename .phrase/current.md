# Current Phase

## Status

Complete

## Goal

Move table open, length, header, footer, and startup metadata reads behind the
native-file storage adapter.

## Scope

- Add an opened native-file storage object that owns the shared file handle.
- Route `read_table` open, header read, file length check, footer read,
  properties read, top-level index read, pinned filter read, and pinned index
  metadata reads through the storage adapter.
- Keep block cache behavior, stats, and cache keys unchanged.
- Keep SSTable, WAL, manifest, blob, compaction, transaction, and public API
  behavior unchanged.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing async APIs or async storage traits.
- Introducing an extent allocator or disk-space reuse layer.
- Changing SSTable block format, codec ids, checksums, footer layout, or cache
  key semantics.
- Moving table writes, manifest publish, WAL append, blob reads, or file cleanup
  to the new adapter in this slice.
- Moving MVCC visibility, table version lifetime, compaction planning, manifest
  publish, blob GC, or public API behavior.

## Acceptance Gate

- Persistent table open/header/footer/startup metadata reads go through a
  native-file storage object and adapter keyed by a storage object id.
- Persistent table checked-block reads continue to go through the same adapter.
- Existing table read/write behavior and storage format remain unchanged.
- Existing persistent/table/block-cache tests pass.
- `cargo fmt --check`, focused Rust tests, clippy, and `git diff --check`
  pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task181 [x] goal:start table-open storage boundary slice | scope:current roadmap | verify:manual
task182 [x] goal:add opened native-file storage object | scope:src/storage.rs | verify:focused tests
task183 [x] goal:route table open/header/footer/metadata reads through adapter | scope:src/table.rs | verify:table tests
task184 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None for this table-open storage boundary slice.
- Async storage traits and public async API remain later phases.

## Evidence

- Previous phase added a table storage object id plus
  `NativeFileReadSource`, but `read_table` still directly opened native files
  and read header/footer/metadata bytes.
- That direct path is now the main remaining native-file dependency in table
  reads before async backend traits can replace the adapter.
- `src/storage.rs` now owns `NativeFileObject`, which opens a storage object,
  reports its length, and serves locked random reads.
- `read_table` now opens a table storage object and reads header, footer,
  properties, top-level index, pinned filters, pinned index metadata, lazy
  index partitions, range tombstones, and data blocks through
  `NativeFileReadSource`.
- Verification passed: `cargo test table --lib`, `cargo test block
  --all-targets`, `cargo test persistent --all-targets`, `cargo clippy
  --all-targets --all-features -- -D warnings`, `cargo fmt --check`,
  `git diff --check`, and forbidden-term scan.

## Next Recommendation

- Define the first async storage trait shape next; keep public async API
  migration and table-write routing as separate slices.
