# Current Phase

## Status

Complete

## Goal

Introduce an owned write request and completion waiter so accepted write
execution can later move behind the runtime boundary without changing commit
semantics.

## Scope

- Add an internal owned write request type for batch and transaction commits.
- Add an internal accepted-write/completion waiter shape.
- Route current synchronous write and transaction commit paths through the
  owned request/completion path while still executing inline.
- Preserve cancellation-before-poll behavior for public async compatibility
  methods.
- Preserve current default native-thread behavior, writer coordinator, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Moving accepted writes onto runtime tasks.
- Adding channels or async executor integration.
- Removing the writer coordinator mutex.
- Changing transaction conflict rules.
- Introducing WAL shards or writer-local deltas.
- Changing persistent native-file blocking behavior.

## Acceptance Gate

- Roadmap records this as the owned write request/completion phase.
- Batch writes and transaction commits build an owned write request.
- Current write execution completes through an internal accepted-write waiter.
- The waiter delivers both successful and failed commit results without cloning
  commit errors.
- Existing async cancellation tests still pass.
- Focused write/async tests, formatting, clippy, full tests, `git diff --check`,
  and forbidden-term scan pass.
- Evidence records that moving execution to runtime tasks remains a later phase.

## Active Task Slice

```text
task299 [x] goal:start owned write request slice | scope:current roadmap | verify:manual
task300 [x] goal:add owned write request and completion types | scope:src/db/commit.rs | verify:unit tests
task301 [x] goal:route write and transaction commit through owned request | scope:src/db/commit.rs | verify:write tests
task302 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task303 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- None for this request/completion shape slice.
- Moving accepted writes to runtime tasks still needs a scheduling path and
  waiter wake strategy.

## Evidence

- Phase 76 added cancellation tokens and task join primitives.
- Evidence from Phase 76 says owned async commit execution still needs an owned
  write request/task shape and waiter result delivery.
- The current write path already has explicit commit tracker terminal states,
  so this phase can change ownership shape without changing commit visibility.
- Batch writes and transaction commits now build `WriteRequest`.
- Current inline execution now passes through `AcceptedWrite` and `WriteWaiter`.
- Completion waiter tests cover successful and failed commit results without
  cloning commit errors.
- Existing async cancellation tests still pass.
- Verification passed: `cargo fmt --check`, `cargo test accepted_write --lib`,
  `cargo test --test async_api`, clippy with all targets/features and warnings
  denied, `cargo test --all-targets --all-features`, `git diff --check`, and
  the forbidden-term scan outside the repository instruction file.

## Next Recommendation

- Commit this slice, then move accepted write execution behind a runtime-owned
  task in the next phase.
