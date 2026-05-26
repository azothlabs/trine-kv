# Current Phase

## Status

Complete

## Goal

Harden one concrete pre-publish durability risk without changing public API or
the v1 storage contract.

## Entry Condition

- Phase 9 CI and publishing workflow is complete.
- User asked for targeted hardening before publishing.

## Scope

- Atomic file publish paths for manifest, table, blob, and recovery report
  writes.
- Focused tests for the new durability helper.
- Local release gate after the hardening change.

## Out Of Scope

- Changing the table, manifest, WAL, blob, or recovery file formats.
- Adding new public API.
- Publishing the crate.
- Broad performance tuning.

## Acceptance Gate

- Atomic publish paths sync the new file contents before rename and sync the
  parent directory after rename on Unix platforms.
- Existing persistent behavior remains unchanged.
- Focused helper coverage passes.
- Local verification passes for `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, examples, package list, package
  verification, publish dry-run, and `git diff --check`.

## Active Task Slice

```text
task045 [x] goal:atomic file publish paths sync parent directory after rename | scope:src/{durability,manifest,table,blob,recovery}.rs,docs/durability.md,.phrase | verify:focused helper test + full release gate
```

## Known Blockers

- GitHub Actions cannot be executed locally in this environment; remote CI must
  run after push.
- Parent-directory fsync is Unix-specific here; non-Unix builds keep the
  previous behavior because portable directory syncing is not available through
  `std`.

## Evidence To Record

- Audit result for atomic publish paths.
- Focused helper test result.
- Full local release gate result.

## Next Recommendation

- If this gate passes, configure the publish secret/environment and run the
  `Publish` workflow with `mode=dry-run` before any real publish.
