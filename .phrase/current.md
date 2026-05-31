# Current Phase

## Status

Complete

## Goal

Add runtime cancellation and task-join primitives, then wire the cancellation
token into background maintenance shutdown.

## Scope

- Add a cloneable runtime cancellation token.
- Expose runtime capabilities for cancellation tokens and task join.
- Keep native-thread background task join behavior behind the runtime boundary.
- Store a database shutdown token and cancel it when background workers are
  stopped.
- Make background maintenance workers observe the runtime cancellation token in
  addition to the existing maintenance shutdown state.
- Preserve current default native-thread behavior, writer coordinator, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Selecting or integrating a concrete async runtime crate.
- Moving commits onto owned runtime tasks.
- Adding async channels, timers, sleeps, or executor yield.
- Removing the writer coordinator mutex.
- Changing maintenance scheduling semantics.
- Changing persistent native-file blocking behavior.

## Acceptance Gate

- Roadmap records this as the runtime cancellation primitive phase.
- Runtime exposes cancellation token capability and task-join capability.
- Cancellation token clones share state and are test-covered.
- Native-thread background tasks can observe cancellation and join in tests.
- Database background worker shutdown cancels the runtime token.
- Existing default background worker behavior remains unchanged.
- Focused runtime/background tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining runtime primitives needed before owned async commit
  execution.

## Active Task Slice

```text
task294 [x] goal:start runtime cancellation slice | scope:current roadmap | verify:manual
task295 [x] goal:add cancellation token and capability flags | scope:src/runtime.rs src/lib.rs | verify:runtime unit tests
task296 [x] goal:wire db background shutdown to cancellation token | scope:src/db.rs | verify:focused db/runtime tests
task297 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task298 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- None for this runtime primitive slice.
- Owned async commit execution still needs an owned request/task shape, waiter
  result delivery, and a target-specific blocking policy.

## Evidence

- Phase 75 introduced runtime-owned background task spawning and task handles.
- Evidence from Phase 75 says the runtime boundary still lacks cancellation
  tokens and shutdown joins before owned async commit execution.
- Current background worker shutdown already wakes workers through the
  maintenance coordinator, so the runtime cancellation token can be added
  without changing scheduling semantics.
- `CancellationToken` is now public, cloneable, and backed by shared atomic
  state.
- Runtime capabilities now use method-based capability queries instead of
  growing public bool fields.
- Native-thread runtime tasks can observe cancellation and join in focused
  tests.
- `DbInner` now stores a runtime shutdown token, shutdown paths cancel it, and
  background maintenance workers check it after wakeup.
- Verification passed: `cargo fmt --check`, `cargo test runtime --lib`,
  `cargo test persistent_background_workers_flush_and_compact_pressure --test
  persistent_wal`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `git diff --check`, and the
  forbidden-term scan outside the repository instruction file.

## Next Recommendation

- Commit this slice, then define the owned write request/task shape on top of
  the runtime boundary.
