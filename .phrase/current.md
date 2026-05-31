# Current Phase

## Status

Complete

## Goal

Add explicit storage capability and unsupported-capability error types.

## Scope

- Add an internal storage capability vocabulary for backend guarantees.
- Add capability checking helpers for storage operations and durability modes.
- Add typed unsupported backend and unsupported durability errors.
- Route the existing table random-read requirement through the typed
  capability helper.
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
- Defining or routing full write, manifest publish, lease, cleanup, or runtime
  traits in this slice.
- Moving MVCC visibility, table version lifetime, compaction planning, manifest
  publish, blob GC, or public API behavior.

## Acceptance Gate

- Internal storage capability types cover current read guarantees and named
  later write/publish/durability guarantees.
- Unsupported backend capability and unsupported durability errors are explicit
  variants.
- Current table random-read capability check uses the typed helper.
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
task189 [x] goal:start storage capability/error slice | scope:current roadmap protocol | verify:manual
task190 [x] goal:add capability and unsupported error types | scope:src/storage.rs src/error.rs src/options.rs | verify:focused tests
task191 [x] goal:use typed capability check in table read path | scope:src/table.rs | verify:table tests
task192 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None for this storage capability/error slice.
- Public async API, async runtime selection, table writes, manifest, WAL, blob
  files, lease handling, and cleanup remain later phases.

## Evidence

- Phase 50 defined the first async read trait shape and native-file blocking
  adapter.
- The async-first protocol requires honest backend capabilities and typed
  unsupported durability/capability errors before write/publish routing.
- The current native-file read backend only needs to claim persistent random
  read in this slice; write and publish capabilities should be named but not
  claimed yet.
- `src/storage.rs` now defines `StorageCapability` and `StorageCapabilities`,
  including current read capability and later write, publish, durability,
  lease, background, and runtime capability names.
- `src/error.rs` now has explicit `UnsupportedBackend` and
  `UnsupportedDurability` variants.
- `src/table.rs` now requires `StorageCapability::RandomRead` through the
  typed capability helper before opening a table read object.
- Verification passed: `cargo test storage --lib`, `cargo test table --lib`,
  `cargo test block --all-targets`, `cargo test persistent --all-targets`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, `git diff --check`, and forbidden-term scan.

## Next Recommendation

- Route memory mode through the same async read contract, or add write/publish
  trait methods behind the capability checks.
