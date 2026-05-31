# Current Phase

## Status

Complete

## Goal

Route blob object listing through the storage backend object listing operation.

## Scope

- Use the native-file storage backend to list candidate blob objects.
- Keep blob file-id parsing and malformed-name corruption behavior inside
  `src/blob.rs`.
- Preserve current filtering behavior for directories, non-blob extensions, and
  non-blob filename prefixes.
- Preserve recovery, stats, blob GC, public API behavior, MVCC visibility,
  storage formats, and cleanup semantics.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Moving WAL append, manifest publish, table reads, writer lease handling,
  parent-directory sync, or production in-memory object routing into different
  backend operations.
- Changing table or blob object formats.
- Changing blob GC candidate policy, compaction planning, MVCC visibility, WAL
  recovery, or public API behavior.
- Adding mmap, direct I/O, fd cache policy, or new compression codecs.

## Acceptance Gate

- Roadmap records the blob object listing phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- `list_blob_file_ids` lists candidate blob objects through
  `StorageObjectListBackend`.
- Blob file-id parsing remains in `src/blob.rs`.
- Case-insensitive `.trineb` extension matching, directory skipping, non-blob
  prefix skipping, and malformed blob filename corruption behavior are
  preserved.
- Existing recovery, stats, blob GC, and persistent blob tests pass.
- `cargo fmt --check`, focused Rust tests, clippy, `cargo test --all-targets
  --all-features`, `git diff --check`, and forbidden-term scan pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task225 [x] goal:start blob object listing backend slice | scope:current roadmap | verify:manual
task226 [x] goal:route blob listing through backend | scope:src/blob.rs | verify:blob tests
task227 [x] goal:cover blob listing edge cases | scope:src/blob.rs | verify:blob tests
task228 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this blob listing slice.
- Public async API, async runtime selection, WAL append, writer lease handling,
  parent-directory sync routing, and production in-memory object routing remain
  later phases.

## Evidence

- Phase 56 routed table output-file creation through backend object writes.
- Phase 57 routed blob file creation through backend object writes.
- Phase 58 routed table/blob cleanup deletion through backend object deletes.
- Phase 59 routed blob object reads through backend random-read operations.
- `StorageObjectListBackend` already supports native-file object listing and is
  used by table file-id discovery.
- Blob object discovery now uses `StorageObjectListBackend`.
- Blob file-id parsing and malformed blob filename corruption behavior remain
  in `src/blob.rs`.
- Added blob listing coverage for uppercase extensions, wrong extensions,
  wrong prefixes, directories, and malformed blob names.
- Verification passed: `cargo test blob --lib`, recovery/stat/blob focused
  tests, `cargo fmt --check`, the full clippy gate, and
  `cargo test --all-targets --all-features`.

## Next Recommendation

- Reassess the remaining storage operations before moving into WAL append or
  writer lease work.
