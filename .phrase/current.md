# Current Phase

## Status

Complete

## Goal

Route WAL append and WAL persist through the storage backend append operation.

## Scope

- Add a WAL storage object kind.
- Add backend append-object traits for sequential append and durability persist.
- Implement native-file append objects for WAL files.
- Route `WalWriter::open_append`, `WalWriter::append_batch`,
  `WalWriter::persist`, and `WalWriter::reopen_append` through the backend.
- Preserve WAL frame bytes, replay semantics, commit visibility ordering,
  durability mode behavior, public API behavior, MVCC, manifest, compaction,
  recovery, and storage formats.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Implementing writer leases.
- Moving WAL rewrite-after-flush, manifest publish, table reads, parent-directory
  sync, or production in-memory object routing into different backend
  operations.
- Changing WAL format, table/blob formats, MVCC visibility, compaction planning,
  recovery policy, or public API behavior.
- Adding mmap, direct I/O, fd cache policy, or new compression codecs.

## Acceptance Gate

- Roadmap records the WAL append backend phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- Native-file backend reports append capability.
- Native-file backend can open a WAL append object and append bytes while
  preserving `Buffered`, `Flush`, `SyncData`, and `SyncAll` behavior.
- Non-WAL objects are rejected by the append-object path.
- `WalWriter` uses the backend append object for append and persist operations.
- WAL replay, torn-tail handling, checksum corruption handling, flush WAL
  rewrite behavior, and persistent write tests pass.
- `cargo fmt --check`, focused Rust tests, clippy, `cargo test --all-targets
  --all-features`, `git diff --check`, and forbidden-term scan pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task229 [x] goal:start WAL append backend slice | scope:current roadmap | verify:manual
task230 [x] goal:add storage append object traits and native-file implementation | scope:src/storage.rs | verify:storage tests
task231 [x] goal:route WalWriter through backend append object | scope:src/wal.rs | verify:wal persistent tests
task232 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this WAL append slice.
- Public async API, async runtime selection, writer lease handling,
  parent-directory sync routing, WAL rewrite routing, and production in-memory
  object routing remain later phases.

## Evidence

- Table/blob object writes, blob/table cleanup deletes, blob reads, and blob
  listing now route through backend operations.
- The next direct native-file operation on the primary write path is WAL append.
- Existing commit ordering already appends WAL before applying operations to
  memtables and publishing the commit sequence.
- `StorageObjectKind::Wal`, `StorageAppendBackend`, and
  `StorageAppendObject` now name the WAL append boundary.
- Native-file append objects support WAL append and requested durability
  persist, and reject non-WAL objects.
- `WalWriter` now opens, appends, persists, and reopens through the backend
  append object.
- WAL frame bytes, replay, torn-tail handling, checksum failure behavior, WAL
  rewrite-after-flush, and commit visibility ordering remain unchanged.
- Verification passed: `cargo test storage --lib`, `cargo test wal --lib`,
  focused persistent WAL tests, `cargo fmt --check`, the full clippy gate, and
  `cargo test --all-targets --all-features`.

## Next Recommendation

- Handle writer lease before broader public async API work.
