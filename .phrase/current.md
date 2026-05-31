# Current Phase

## Status

Complete

## Goal

Route persistent statistics object-length reads through the storage backend
random-read operation.

## Scope

- Replace production `fs::metadata(...).len()` usage in stats and compaction
  byte accounting with backend object-open plus object length.
- Preserve the existing fail-open stats behavior: missing or unreadable files
  contribute zero bytes instead of failing `Db::stats`.
- Reuse existing storage read capabilities instead of adding a new backend
  trait for this small slice.
- Preserve WAL/table/blob/manifest formats, recovery policy, MVCC visibility,
  compaction behavior, cleanup behavior, and public API behavior.

## Out Of Scope

- Routing safe temporary file listing/deletion.
- Routing table/blob object listing that is already backend-owned.
- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Moving production in-memory object routing into backend operations.

## Acceptance Gate

- Roadmap records the stats object-length backend phase at phase granularity.
- Current phase records the stats fail-open behavior and out-of-scope items.
- Table stats byte accounting opens table objects through the storage backend.
- Obsolete blob stats byte accounting opens blob objects through the storage
  backend.
- Existing stats tests still prove table bytes, compaction bytes, and obsolete
  blob bytes are reported.
- `cargo fmt --check`, focused stats/persistent tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records remaining direct native-file operations after the slice.

## Active Task Slice

```text
task260 [x] goal:start stats object-length backend slice | scope:current roadmap | verify:manual
task261 [x] goal:route stats length reads through storage backend | scope:src/db.rs | verify:persistent stats tests
task262 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task263 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this stats length slice.
- Safe temporary file listing/deletion, public async API, async runtime
  selection, and production in-memory object routing remain later phases.

## Evidence

- Stats table-byte accounting still used direct `fs::metadata` for table files.
- Obsolete blob-byte accounting still used direct `fs::metadata` for blob files.
- The existing storage read backend already exposes native-file open and object
  length operations.
- Existing persistent stats coverage checks table bytes, compaction bytes, and
  obsolete blob bytes.
- `table_file_bytes` now opens table storage objects through
  `NativeFileBackend` and reads object length.
- Obsolete blob byte stats now open blob storage objects through
  `NativeFileBackend` and read object length.
- Existing fail-open stats behavior is preserved: unreadable objects contribute
  zero bytes.
- Verification passed: `cargo fmt --check`, `cargo test storage --lib`,
  `cargo test persistent_stats_report_tables_blobs_and_compactions --test
  persistent_wal`, `cargo clippy --all-targets --all-features -- -D
  warnings`, `cargo test --all-targets --all-features`, `git diff --check`,
  and forbidden-term scan.

## Next Recommendation

- Reassess whether safe temporary file repair should get explicit backend
  directory-list/delete operations.
