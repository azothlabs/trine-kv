# Current Phase

## Status

Complete

## Goal

Audit and harden production-facing operational behavior after API polish.

## Entry Condition

- Phase 5 API polish is complete.
- V1 tests, quickstart, docs, and benchmark baseline are present.

## Scope

- Operational failure modes around recovery, file cleanup, locks, WAL replay,
  flush/compaction publish, resource limits, and diagnostics.
- Focused code changes only after an audit exposes a concrete risk.
- Tests that reproduce the risk or prove the hardened path.

## Out Of Scope

- Changing v1 storage contracts without an ADR or protocol update.
- Packaging and release automation.
- Performance tuning that is not tied to a hardening risk.

## Acceptance Gate

- At least one production-hardening audit result is recorded.
- Any code change has a focused regression test.
- `cargo fmt --check`, `cargo clippy`, `cargo test`, and `git diff --check`
  pass.

## Active Task Slice

```text
task038 [x] goal:production hardening audit identifies the first concrete operational risk and fixes it if local | scope:src,recovery/table/blob/wal/db tests,.phrase/current.md,.phrase/evidence.md | verify:manual audit + focused test + cargo fmt --check + cargo clippy + cargo test + git diff --check
task039 [x] goal:WAL decode rejects impossible operation counts before allocation | scope:src/wal.rs,.phrase/current.md,.phrase/evidence.md | verify:manual audit + focused test + cargo fmt --check + cargo clippy + cargo test + git diff --check
task040 [x] goal:continue hardening audit for startup cleanup and manifest/table decode resource bounds | scope:src/recovery.rs,src/manifest.rs,src/table.rs,tests,.phrase/current.md,.phrase/evidence.md | verify:manual audit + focused tests if local risk appears + cargo fmt --check + cargo clippy + cargo test + git diff --check
task041 [x] goal:audit flush/compaction cleanup and diagnostics after partial file writes or publish failures | scope:src/db.rs,src/table.rs,src/blob.rs,src/manifest.rs,tests,.phrase/current.md,.phrase/evidence.md | verify:manual audit + focused tests if local risk appears + cargo fmt --check + cargo clippy + cargo test + git diff --check
```

## Known Blockers

- None recorded for Phase 6.
- Manifest publish failure no longer advances in-memory manifest state.
- WAL, manifest, and table decoders reject impossible count fields before large
  allocation.
- Startup cleanup fail-closed behavior is covered by existing tests.
- Failed flush/compaction publish removes unpublished table/blob output files.

## Evidence To Record

- Audit result with risk category.
- Fix and regression test if the risk is local.
- WAL resource-bound audit result.
- Follow-up startup cleanup and manifest/table decode resource-bound audit
  result.
- Flush/compaction cleanup and diagnostics audit result.

## Next Recommendation

- Choose the next phase from release packaging, CI/release verification,
  integration examples, or another targeted hardening audit based on fresh
  user priority.
