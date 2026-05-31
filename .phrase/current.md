# Current Phase

## Status

Complete

## Goal

Route recovery report reads through the storage backend optional object-read
operation.

## Scope

- Route `read_recovery_report` through backend optional object read.
- Keep missing recovery report behavior as a `NotFound` I/O error.
- Preserve recovery report text format, UTF-8 error behavior, repair policy,
  safe temporary file classification, WAL/table/blob/manifest formats, MVCC
  visibility, compaction, and public API behavior.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing or renaming public async APIs.
- Changing recovery report text format.
- Changing safe temporary file listing/deletion behavior.
- Changing directory creation or stats metadata reads.
- Moving production in-memory object routing into backend operations.

## Acceptance Gate

- Roadmap records the recovery report read backend phase at phase granularity.
- Current phase records the storage operation boundary and out-of-scope items.
- `read_recovery_report` uses storage object read instead of direct file open.
- Missing recovery reports still return `NotFound`.
- Recovery report decode and repair tests pass.
- `cargo fmt --check`, focused Rust tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records remaining direct native-file operations after the slice.

## Active Task Slice

```text
task253 [x] goal:start recovery report read backend slice | scope:current roadmap | verify:manual
task254 [x] goal:route read_recovery_report through backend | scope:src/recovery.rs | verify:recovery tests
task255 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this recovery report read slice.
- Public async API, async runtime selection, directory creation backend routing,
  safe temporary file listing/deletion routing, stats metadata routing, and
  production in-memory object routing remain later phases.

## Evidence

- Phase 65 routed recovery report writes through backend object write.
- Phase 66 added optional whole-object reads and routed WAL replay through that
  operation.
- `read_recovery_report` still opens and reads the report file directly.
- `read_recovery_report` now reads through `StorageObjectReadBackend`.
- Missing recovery reports still return an I/O `NotFound` error.
- Invalid UTF-8 still returns an I/O `InvalidData` error before report decode.
- Verification passed: `cargo test recovery --all-targets`.

## Next Recommendation

- Reassess remaining direct native-file operations and choose whether directory
  creation, safe temporary file repair, or stats metadata should be routed next.
