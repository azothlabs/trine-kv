# Current Phase

## Status

Complete

## Goal

Route manifest reads through the storage backend contract.

## Scope

- Add an internal current-manifest read backend operation.
- Implement native-file current-manifest reads through the backend operation.
- Route `ManifestStore::open_or_create` and `read_manifest` through the backend
  operation.
- Keep manifest encoding, checksum, version, optional missing-manifest handling,
  and state transition rules unchanged.
- Keep block cache behavior, stats, cache keys, and SSTable format unchanged.
- Keep SSTable, WAL, manifest, blob, compaction, transaction, and public API
  behavior unchanged.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing public async APIs.
- Introducing an extent allocator or disk-space reuse layer.
- Changing SSTable block format, codec ids, checksums, footer layout, or cache
  key semantics.
- Moving table writes, WAL append, blob reads, object listing, or file cleanup
  to the new adapter in this slice.
- Defining or routing full write, lease, cleanup, runtime, or public async API
  traits in this slice.
- Reworking in-memory DB flush behavior or making memory mode create SSTable
  storage objects in production paths.
- Moving MVCC visibility, table version lifetime, compaction planning, blob GC,
  manifest read/recovery, or public API behavior.

## Acceptance Gate

- Native-file backend exposes a current-manifest read operation that returns
  `None` for a missing manifest and bytes for an existing manifest.
- `ManifestStore::open_or_create` uses the backend read operation to choose
  existing state, create-if-missing state, or empty non-created state.
- Public `read_manifest` uses the backend read operation and reports a missing
  manifest as an error.
- Persistent table open/header/footer/startup metadata reads still go through a
  native-file storage object and adapter keyed by a storage object id.
- Persistent table checked-block reads continue to go through the same adapter.
- Existing manifest format, table read/write behavior, and storage format remain
  unchanged.
- Existing persistent/table/block-cache tests pass.
- `cargo fmt --check`, focused Rust tests, clippy, and `git diff --check`
  pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task201 [x] goal:start manifest read backend slice | scope:current roadmap protocol | verify:manual
task202 [x] goal:add native-file current-manifest read operation | scope:src/storage.rs | verify:storage tests
task203 [x] goal:route manifest open/read through backend | scope:src/manifest.rs | verify:manifest tests
task204 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this manifest read backend slice.
- Public async API, async runtime selection, table writes, manifest, WAL, blob
  files, lease handling, cleanup, object listing, and production in-memory
  table-object routing remain later phases.

## Evidence

- Phase 53 routed manifest publish through the native-file storage backend.
- The async-first protocol also says recovery reads the current manifest
  through the storage backend contract.
- Existing `ManifestStore::open_or_create` has a three-way decision:
  read existing manifest, create a missing manifest when allowed, or use empty
  state when missing and not creating. This slice preserves that behavior while
  routing the read through the backend.
- `src/storage.rs` now has `StorageManifestReadBackend` and
  `BlockingStorageManifestReadBackend`.
- `NativeFileBackend` now reads the current manifest object and returns `None`
  only for a missing native-file manifest.
- `src/manifest.rs` routes `ManifestStore::open_or_create` and
  `read_manifest` through the backend read operation while keeping decode logic
  unchanged.
- Verification passed: `cargo test storage --lib`, `cargo test manifest --lib`,
  `cargo test table --lib`, `cargo test block --all-targets`,
  `cargo test persistent --all-targets`, `cargo clippy --all-targets
  --all-features -- -D warnings`, `cargo fmt --check`, `git diff --check`,
  and forbidden-term scan.

## Next Recommendation

- After manifest reads land, route object listing or table write output through
  storage backend operations.
