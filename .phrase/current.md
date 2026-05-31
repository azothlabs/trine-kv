# Current Phase

## Status

Complete

## Goal

Route recovery report publish through storage backend operations.

## Scope

- Add a recovery-report storage object kind.
- Route recovery report bytes through the existing native-file object write
  operation.
- Keep the existing `RECOVERY_REPORT.tmp` temporary file name.
- Keep parent-directory sync behind the storage backend directory-sync
  operation.
- Preserve recovery report text format, repair policy, safe temporary file
  classification, WAL/table/blob/manifest formats, MVCC visibility, compaction,
  and public API behavior.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Changing recovery report text format.
- Changing safe temporary file policy.
- Changing WAL replay reads into a backend optional-read operation.
- Moving production in-memory object routing into backend operations.

## Acceptance Gate

- Roadmap records the recovery report write backend phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- Storage object kinds include recovery report objects.
- Native-file object write supports recovery report objects while still
  rejecting manifest objects.
- `write_recovery_report` uses storage object write and backend directory sync.
- The temporary file remains `RECOVERY_REPORT.tmp`.
- Existing recovery report, recovery repair, storage, and persistent recovery
  tests pass.
- `cargo fmt --check`, focused Rust tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records which direct native-file recovery writes remain.

## Active Task Slice

```text
task245 [x] goal:start recovery report write backend slice | scope:current roadmap | verify:manual
task246 [x] goal:add recovery report storage object kind | scope:src/storage.rs | verify:storage tests
task247 [x] goal:route write_recovery_report through storage backend | scope:src/recovery.rs | verify:recovery tests
task248 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this recovery report write slice.
- Public async API, async runtime selection, WAL replay optional-read routing,
  and production in-memory object routing remain later phases.

## Evidence

- Phase 63 routed recovery report parent-directory sync through the backend.
- `write_recovery_report` still creates `RECOVERY_REPORT.tmp`, writes report
  bytes, syncs the file, and renames it directly in `recovery.rs`.
- `StorageObjectWriteBackend` already preserves the same temp naming for a
  final path named `RECOVERY_REPORT`.
- `StorageObjectKind::RecoveryReport` now names recovery report objects.
- Native-file object write can write recovery report objects while manifest
  objects remain reserved for manifest publish.
- `write_recovery_report` now encodes report text and delegates temp write,
  file sync, and rename to backend object write, then uses backend directory
  sync for the parent directory.
- Verification passed: `cargo test storage --lib` and
  `cargo test recovery --all-targets`.

## Next Recommendation

- Route WAL replay reads through a backend optional-read operation or continue
  production in-memory object routing.
