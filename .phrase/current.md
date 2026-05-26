# Current Phase

## Status

Complete

## Goal

Close the Windows side of the pre-publish directory-sync durability hardening
without changing public API or the v1 storage contract.

## Entry Condition

- Phase 10 synced parent directories after atomic file publish on Unix.
- User asked to handle non-Unix, with Windows as the concrete supported target.

## Scope

- Windows implementation of parent-directory sync after rename.
- Durability documentation for Unix, Windows, and other targets.
- Local release gate after the hardening change.
- Best available Windows target validation in this environment.

## Out Of Scope

- Changing the table, manifest, WAL, blob, or recovery file formats.
- Adding new public API.
- Publishing the crate.
- Platform-specific directory sync for targets other than Unix and Windows.

## Acceptance Gate

- Atomic publish paths sync the parent directory after rename on Unix and
  Windows.
- Other targets are explicitly documented as best-effort because no portable
  `std` directory sync is available.
- Existing persistent behavior remains unchanged.
- Local verification passes for `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, examples, package list, package
  verification, publish dry-run, and `git diff --check`.

## Active Task Slice

```text
task046 [x] goal:Windows parent-directory sync is implemented and documented | scope:src/durability.rs,docs/durability.md,.phrase | verify:local release gate + Windows target check
```

## Known Blockers

- GitHub Actions cannot be executed locally in this environment; remote CI must
  run after push.
- The Windows branch was compile-checked with `x86_64-pc-windows-gnu`, but not
  executed on a real Windows filesystem in this environment.
- Targets other than Unix and Windows remain best-effort for parent-directory
  sync.

## Evidence To Record

- Windows target validation result.
- Full local release gate result.

## Next Recommendation

- If this gate passes, configure the publish secret/environment and run the
  `Publish` workflow with `mode=dry-run` before any real publish.
