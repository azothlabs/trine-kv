# Current Phase

## Status

Complete

## Goal

Complete the async transaction compatibility surface and codify current async
write cancellation behavior on top of the explicit commit tracker.

## Scope

- Add async compatibility methods for transaction point reads, range reads, and
  commit.
- Extend async API tests to exercise transaction read tracking and commit.
- Add a focused cancellation-before-acceptance test for an unpolled async write
  future.
- Add a focused accepted-write test proving the current async write future
  reaches a visible terminal commit when polled.
- Preserve the current no-runtime async compatibility model, writer
  coordinator, commit tracker behavior, WAL/table/blob/manifest formats, MVCC,
  compaction, recovery, cleanup, and storage behavior.

## Out Of Scope

- Selecting a concrete async runtime.
- Spawning accepted writes onto an owned runtime task.
- Removing the writer coordinator mutex.
- Introducing WAL shards or writer-local deltas.
- Changing transaction conflict rules.
- Changing persistent native-file blocking behavior.

## Acceptance Gate

- Roadmap records this as the async transaction and cancellation test phase.
- Transaction exposes async compatibility methods for reads and commit.
- Async API smoke covers transaction async reads and commit.
- Dropping an unpolled async write future has no write side effect.
- Polling an async write future reaches a visible terminal commit.
- `cargo fmt --check`, focused async API tests, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records that true owned async commit execution still waits on the
  runtime boundary.

## Active Task Slice

```text
task284 [x] goal:start async transaction/cancellation slice | scope:current roadmap | verify:manual
task285 [x] goal:add transaction async compatibility methods | scope:src/transaction.rs | verify:async smoke test
task286 [x] goal:add write cancellation behavior tests | scope:tests/async_api.rs | verify:focused tests
task287 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task288 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- None for the compatibility/test slice.
- Owned async commit execution still requires a runtime boundary with spawn,
  cancellation token, shutdown join, and target-specific blocking policy.

## Evidence

- The async-first protocol lists transaction reads and commit in the async API
  shape.
- Current transaction methods are blocking-only.
- Current async write methods contain no await points, so dropping before poll
  must be a no-op and polling must complete the commit synchronously to a
  terminal state.
- `Transaction` now exposes async compatibility methods for point reads, range
  reads, and commit.
- The async API smoke test now covers default-bucket and named-bucket
  transaction async reads and commit.
- Focused tests now prove that dropping an unpolled async write future leaves no
  write side effect, and polling an async write future reaches a visible
  terminal commit.
- Verification passed: `cargo fmt --check`, `cargo test --test async_api`,
  `cargo clippy --all-targets --all-features -- -D warnings`, and
  `cargo test --all-targets --all-features`, `git diff --check`, and the
  forbidden-term scan outside the repository instruction file.

## Next Recommendation

- Commit this slice, then continue toward an explicit runtime boundary before
  moving accepted writes to owned async execution.
