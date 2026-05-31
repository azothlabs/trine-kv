# Current Phase

## Status

Complete

## Goal

Route WAL replay reads through a storage backend optional object-read operation.

## Scope

- Add a backend operation for reading complete object bytes where absence is a
  normal result.
- Implement the operation for native-file and in-memory storage backends.
- Route `read_batches_after` through the backend operation.
- Keep a missing WAL equivalent to an empty replay.
- Preserve WAL frame bytes, replay floor filtering, checksum behavior,
  torn-tail handling, WAL rewrite, WAL append, recovery policy, and public API
  behavior.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Changing WAL record format.
- Changing table/blob/manifest read paths to use the new operation.
- Moving production in-memory object routing into backend operations.

## Acceptance Gate

- Roadmap records the WAL replay read backend phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- Native-file and in-memory backends report object-read capability.
- Native-file optional object read returns `None` for missing files and bytes
  for existing files.
- In-memory optional object read returns `None` for missing objects and bytes
  for existing objects.
- `read_batches_after` no longer uses direct native-file read/open/existence
  checks.
- Existing WAL replay, WAL corruption, storage, recovery, and persistent tests
  pass.
- `cargo fmt --check`, focused Rust tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records the remaining storage-boundary work.

## Active Task Slice

```text
task249 [x] goal:start WAL replay read backend slice | scope:current roadmap | verify:manual
task250 [x] goal:add optional object read backend operation | scope:src/storage.rs | verify:storage tests
task251 [x] goal:route WAL replay reads through backend | scope:src/wal.rs | verify:WAL/persistent tests
task252 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this WAL replay read slice.
- Public async API, async runtime selection, and production in-memory object
  routing remain later phases.

## Evidence

- WAL append, WAL rewrite, directory sync, writer lease, table/blob object
  lifecycle, recovery report write, and manifest publish already route through
  backend operations.
- `read_batches_after` still uses `path.exists`, `File::open`, and
  `read_to_end` directly.
- A missing WAL is normal for an empty or fully flushed persistent database.
- `StorageObjectReadBackend` and `BlockingStorageObjectReadBackend` now expose
  optional whole-object reads.
- Native-file and in-memory storage backends now report `StorageCapability::ObjectRead`.
- Native-file optional object read returns `None` for missing files.
- In-memory optional object read returns `None` for missing objects.
- `read_batches_after` now reads WAL bytes through the backend and keeps missing
  WAL equivalent to an empty replay.
- Verification passed: `cargo test storage --lib`, `cargo test wal --lib`,
  focused persistent WAL and flush tests.

## Next Recommendation

- Reassess remaining direct native-file operations and choose the next
  production backend-boundary slice from evidence.
