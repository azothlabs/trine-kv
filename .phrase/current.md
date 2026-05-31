# Current Phase

## Status

Complete

## Goal

Introduce a minimal runtime boundary for background execution so later owned
async commit work does not depend on direct thread spawning from database core
logic.

## Scope

- Add runtime option, capability, and task-handle types.
- Route background maintenance worker spawning through the runtime boundary.
- Validate that persistent background workers require runtime background-thread
  capability.
- Preserve current default native-thread behavior.
- Preserve the writer coordinator, commit tracker, WAL/table/blob/manifest
  formats, MVCC, compaction, recovery, cleanup, and storage behavior.

## Out Of Scope

- Selecting or integrating a concrete async runtime crate.
- Moving commits onto owned runtime tasks.
- Adding async channels, timers, or cancellation tokens.
- Removing the writer coordinator mutex.
- Changing maintenance scheduling semantics.
- Changing persistent native-file blocking behavior.

## Acceptance Gate

- Roadmap records this as the runtime boundary phase.
- Runtime options and capabilities are public and default to native-thread
  behavior.
- Background maintenance worker spawning goes through the runtime boundary.
- Inline/no-background-thread runtime mode rejects persistent writable opens
  when background workers are requested.
- Existing background worker behavior remains unchanged under default options.
- Focused runtime tests, formatting, clippy, full tests, `git diff --check`,
  and forbidden-term scan pass.
- Evidence records remaining runtime features needed before owned async commit
  execution.

## Active Task Slice

```text
task289 [x] goal:start runtime boundary slice | scope:current roadmap | verify:manual
task290 [x] goal:add runtime option/capability/task boundary | scope:src/runtime.rs src/options.rs src/lib.rs | verify:unit tests
task291 [x] goal:route maintenance worker spawn through runtime | scope:src/db.rs | verify:persistent background tests
task292 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task293 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- None for this runtime boundary slice.
- Owned async commit execution still needs spawn for async tasks, cancellation
  tokens, shutdown joins, and target-specific blocking policy.

## Evidence

- Background maintenance currently calls native thread spawn directly from
  database core logic.
- The async-first protocol requires a runtime boundary before accepted writes
  can safely move to owned async execution.
- A default native-thread runtime preserves current behavior while creating a
  clear capability check for targets that cannot provide background threads.
- `RuntimeOptions`, `RuntimeMode`, and `RuntimeCapabilities` are now public.
- `DbOptions` now carries a runtime option with native-thread default behavior.
- Background maintenance worker spawning now routes through `Runtime`.
- Persistent writable open now rejects requested background workers when the
  selected runtime lacks background-thread capability.
- Verification passed: `cargo fmt --check`, `cargo test runtime --lib`,
  `cargo test persistent_background_workers_flush_and_compact_pressure --test
  persistent_wal`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `git diff --check`, and the
  forbidden-term scan outside the repository instruction file.

## Next Recommendation

- Commit this runtime boundary slice, then expand the boundary only where the
  next implementation step actually needs it.
