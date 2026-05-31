# Current Phase

## Status

Complete

## Goal

Introduce an explicit commit tracker behind the current writer coordinator so
write acceptance, terminal commit state, and read-boundary advancement are named
protocol steps instead of an ad-hoc sequence store.

## Scope

- Add a commit slot tracker with `Open`, `Visible`, and `Skipped` terminal
  transitions.
- Route successful write sequence publication through the tracker.
- Mark pre-publication failures as skipped after a sequence slot has been
  accepted.
- Reinitialize the tracker from WAL replay state during persistent open.
- Add focused tests for slot advancement over skipped and visible states.
- Preserve the existing writer coordinator, WAL format, table/blob formats,
  bucket routing, MVCC read behavior, compaction behavior, stats behavior, and
  public blocking/async compatibility API names.

## Out Of Scope

- Removing the writer coordinator mutex.
- Introducing WAL shards.
- Moving foreground writes to writer-local deltas.
- Changing transaction validation policy.
- Changing native-file blocking behavior.
- Adding a concrete async runtime dependency.
- Changing table, blob, manifest, or WAL on-disk formats.

## Acceptance Gate

- Roadmap records this as the commit-tracker phase.
- Current phase states the affected write path and the preserved coordinator
  boundary.
- Commit slots have explicit `Open`, `Visible`, and `Skipped` states.
- The public read boundary is advanced by commit tracker transitions.
- WAL replay resets the tracker to the recovered durable boundary.
- Failed accepted writes before delta publication mark their slot skipped.
- Successful writes mark their slot visible only after deltas are published.
- Focused commit-tracker tests cover skipped and visible advancement.
- `cargo fmt --check`, focused tests, clippy, full tests, `git diff --check`,
  and forbidden-term scan pass.
- Evidence records remaining work for true lock-free writer-local deltas and
  cancellation-safe async write execution.

## Active Task Slice

```text
task279 [x] goal:start commit tracker slice | scope:current roadmap | verify:manual
task280 [x] goal:add commit slot tracker state machine | scope:src/db.rs | verify:unit tests
task281 [x] goal:route write/replay sequence publication through tracker | scope:src/db/commit.rs | verify:write/recovery tests
task282 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task283 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap | verify:git status
```

## Known Blockers

- None for this protocol slice.
- True multi-writer execution still requires writer-local deltas, WAL
  partitioning, and a publish barrier after this tracker exists.

## Evidence

- The async-first protocol requires accepted writes to reach a terminal state
  even if a future is cancelled after acceptance.
- The current write path uses one writer mutex and a single `last_sequence`
  atomic, which hides commit acceptance and terminal state behind an unnamed
  store.
- Adding the tracker behind the existing writer coordinator gives later
  async/cancellation work a concrete protocol boundary without changing storage
  formats or public read semantics.
- `CommitTracker` now owns sequence reservation, `Open -> Visible`, and
  `Open -> Skipped` transitions.
- `Db::last_committed_sequence` now reads the tracker boundary instead of a raw
  sequence atomic.
- The write path reserves a slot after validation/routing, appends WAL, applies
  deltas, and marks the slot visible after successful publication.
- WAL replay resets the tracker boundary to the recovered replay result.
- Verification passed: `cargo fmt --check`, `cargo test commit_tracker --lib`,
  `cargo test memory_async_compatibility_surface_smoke --test async_api`,
  `cargo clippy --all-targets --all-features -- -D warnings`, and
  `cargo test --all-targets --all-features`, `git diff --check`, and the
  forbidden-term scan outside the repository instruction file.

## Next Recommendation

- Commit this slice, then continue toward cancellation-safe async write
  execution on top of the explicit commit states.
