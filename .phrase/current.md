# Current Phase

## Status

Complete

## Goal

Route manifest publish through the storage backend contract.

## Scope

- Add an internal manifest publish backend operation.
- Implement native-file manifest publish through the backend operation.
- Route `ManifestStore` publish through the backend operation.
- Keep manifest encoding, checksum, version, and state transition rules
  unchanged.
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
  manifest read/recovery, or public API behavior.

## Acceptance Gate

- Native-file storage backend reports atomic manifest publish and strict sync
  capabilities honestly.
- Native-file backend exposes a manifest publish operation that writes the
  manifest bytes, syncs them, atomically publishes the path, and syncs the
  parent directory for strict publish.
- `ManifestStore` uses the backend manifest publish operation and does not
  advance in-memory state when backend publish fails.
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
task197 [x] goal:start manifest publish backend slice | scope:current roadmap protocol | verify:manual
task198 [x] goal:add native-file manifest publish operation | scope:src/storage.rs | verify:storage tests
task199 [x] goal:route manifest publish through backend | scope:src/manifest.rs | verify:manifest tests
task200 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this manifest publish backend slice.
- Public async API, async runtime selection, table writes, manifest, WAL, blob
  files, lease handling, cleanup, manifest read/recovery routing, and
  production in-memory table-object routing remain later phases.

## Evidence

- Phase 51 added capability checks and typed unsupported backend/durability
  errors.
- Phase 52 proved the read contract can serve volatile memory storage objects.
- The async-first protocol says manifest publish is a backend operation, not an
  engine-level rename.
- Existing `ManifestStore` already keeps in-memory state unchanged until file
  publish succeeds; this slice preserves that rule while moving the publish
  operation behind native-file backend capability checks.
- `src/storage.rs` now has `StorageManifestPublishBackend` and
  `BlockingStorageManifestPublishBackend`.
- `NativeFileBackend` now reports atomic manifest publish plus strict sync
  capabilities and publishes manifest bytes through its backend operation.
- `src/manifest.rs` keeps manifest byte encoding local but delegates final
  publish to `NativeFileBackend`.
- Verification passed: `cargo test storage --lib`, `cargo test manifest --lib`,
  `cargo test table --lib`, `cargo test block --all-targets`,
  `cargo test persistent --all-targets`, `cargo clippy --all-targets
  --all-features -- -D warnings`, `cargo fmt --check`, `git diff --check`,
  and forbidden-term scan.

## Next Recommendation

- Route manifest read/listing or table write output through storage backend
  operations.
