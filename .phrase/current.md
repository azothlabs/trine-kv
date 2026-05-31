# Current Phase

## Status

Complete

## Goal

Move accepted async writes behind the runtime boundary so a polled write future
hands execution to Trine-owned work instead of running the commit on the caller
poll stack.

## Scope

- Add a pollable internal write future backed by `AcceptedWrite` and
  `WriteWaiter`.
- Let native-thread runtime mode execute accepted writes on a runtime task.
- Keep inline runtime mode as a synchronous compatibility path.
- Route default-bucket, named-bucket, batch, and transaction async writes
  through the same owned execution boundary.
- Preserve existing blocking `Db::write` and transaction `commit` semantics.
- Preserve writer coordinator, commit tracker, WAL/table/blob/manifest formats,
  MVCC, compaction, recovery, cleanup, and storage behavior.

## Out Of Scope

- Removing the writer coordinator mutex.
- Adding WAL shards, writer-local deltas, or a publish barrier.
- Adding channels or an external async executor dependency.
- Changing transaction conflict rules.
- Changing persistent native-file blocking behavior.
- Changing public storage formats or recovery protocol.

## Acceptance Gate

- Roadmap records this as the runtime-owned write execution phase.
- Dropping an unpolled async write future still has no side effect.
- Once an async write future is polled and accepted in native-thread runtime
  mode, dropping the future does not cancel the internal commit.
- Batch, default-bucket, named-bucket, and transaction async writes use the
  owned execution boundary.
- Inline runtime mode still completes async writes without requiring background
  threads.
- Focused async/write/runtime tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining blockers for true multi-writer execution.

## Active Task Slice

```text
task304 [x] goal:start runtime-owned write execution slice | scope:current roadmap | verify:manual
task305 [x] goal:add pollable write future and waiter wake path | scope:src/db/commit.rs | verify:unit tests
task306 [x] goal:route async db/bucket/transaction writes through owned execution | scope:src/db.rs src/bucket.rs src/transaction.rs | verify:async api tests
task307 [x] goal:add accepted-after-poll and inline runtime tests | scope:tests/async_api.rs | verify:focused tests
task308 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task309 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- True multi-writer execution still needs writer-local deltas, WAL partitioning,
  and a publish barrier.
- Native-file storage remains blocking behind native-thread runtime execution.

## Evidence

- Phase 76 added cancellation tokens and task join primitives.
- Phase 77 added owned `WriteRequest`, `AcceptedWrite`, and `WriteWaiter`.
- Evidence from Phase 77 recommends moving accepted write execution behind the
  runtime boundary while preserving cancellation-before-poll and
  terminal-after-acceptance behavior.
- `Db::write_async` now returns a pollable internal future that accepts the
  write on first poll.
- Native-thread runtime mode runs accepted writes on a runtime task and wakes
  the waiting future through `WriteCompletion`.
- Inline runtime mode executes through the same accepted-write path without
  requiring background threads.
- Default-bucket, named-bucket, batch, and transaction async writes now route
  through the owned execution boundary.
- Tests cover unpolled cancellation, polled accepted write completion after
  future drop, terminal commit, and inline runtime completion.
- Verification passed: `cargo fmt --check`, `cargo clippy --all-targets
  --all-features -- -D warnings`, `cargo test accepted_write --lib`,
  `cargo test --test async_api`, `cargo test --all-targets --all-features`,
  `git diff --check`, and forbidden-term scan outside the repository
  instruction file.

## Next Recommendation

- Commit this slice, then choose the next multi-writer concurrency step from
  the remaining blockers.
