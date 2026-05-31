# Current Phase

## Status

Complete

## Goal

Route blob file reads through the storage backend random-read operation.

## Scope

- Use the native-file storage backend to open blob objects for reads.
- Keep blob file format validation inside `src/blob.rs`.
- Route full blob reads through a backend read object.
- Route blob properties reads through a backend read object without decoding
  record payloads.
- Route indexed blob record reads through a backend read object without reading
  unrelated records.
- Preserve blob checksums, indexed-read validation, missing/corrupt blob error
  behavior, public API behavior, storage formats, MVCC visibility, recovery,
  compaction, and cleanup semantics.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Moving WAL append, manifest publish, table reads, blob listing, writer lease
  handling, parent-directory sync, or production in-memory object routing into
  different backend operations.
- Changing table or blob object formats.
- Changing blob GC candidate policy, compaction planning, MVCC visibility, WAL
  recovery, or public API behavior.
- Adding mmap, direct I/O, fd cache policy, or new compression codecs.

## Acceptance Gate

- Roadmap records the blob object read phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- Blob full-file read opens and reads the blob object through
  `StorageReadBackend`/`StorageReadObject`.
- Blob properties read opens and reads the blob object through the backend while
  preserving the properties-only execution shape.
- Indexed blob record read opens and reads the blob object through the backend
  while preserving target-record-only execution shape.
- Existing blob corruption, properties-only, indexed-read, recovery, compaction,
  and persistent large-value tests pass.
- `cargo fmt --check`, focused Rust tests, clippy, `cargo test --all-targets
  --all-features`, `git diff --check`, and forbidden-term scan pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task221 [x] goal:start blob object read backend slice | scope:current roadmap | verify:manual
task222 [x] goal:route full blob reads through backend | scope:src/blob.rs | verify:blob tests
task223 [x] goal:route properties/indexed blob reads through backend | scope:src/blob.rs | verify:blob persistent tests
task224 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this blob read slice.
- Public async API, async runtime selection, WAL append, blob listing, writer
  lease handling, parent-directory sync routing, and production in-memory object
  routing remain later phases.

## Evidence

- Phase 56 routed table output-file creation through backend object writes.
- Phase 57 routed blob file creation through backend object writes.
- Phase 58 routed table/blob cleanup deletion through backend object deletes.
- `StorageReadBackend` and `StorageReadObject` already expose random-read and
  length operations that can support blob reads without moving blob format logic
  into storage.
- Blob full-file reads, properties-only reads, raw value reads, and indexed
  record reads now open blob objects through `NativeFileBackend`.
- Blob format validation, checksum verification, full validation, properties-only
  execution shape, and target-record-only indexed read shape remain in
  `src/blob.rs`.
- Verification passed: `cargo test blob --lib`, focused persistent blob/reopen
  tests, `cargo fmt --check`, the full clippy gate, and
  `cargo test --all-targets --all-features`.

## Next Recommendation

- Route blob listing through the backend so blob object discovery and blob
  object bytes share the same storage boundary.
