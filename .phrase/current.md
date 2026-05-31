# Current Phase

## Status

Complete

## Goal

Introduce an internal awaitable cursor-advancement path for async range and
prefix callers while preserving the existing synchronous iterator behavior.

## Scope

- Change `Iter::next_async` and `LazyIter::next_async` so lazy scans advance
  through async internal methods instead of calling synchronous `Iterator::next`
  at the public wrapper.
- Add async advancement methods through `LazyScan`, `RecordSource`,
  `SourceCursor`, `MemtableCursor`, and `TablePointCursor`.
- Keep table block loading and decode synchronous inside the new async
  advancement hook for now; this phase only creates the awaitable path where a
  later read phase can await owned storage completions.
- Add persistent async range/prefix coverage that flushes data into tables and
  advances through async iterators.
- Preserve existing public async API shape, blocking iterator behavior,
  publish barrier, commit tracker, WAL/table/blob/manifest formats, MVCC,
  compaction, recovery, cleanup, and storage behavior.

## Out Of Scope

- Adding true async block decode.
- Moving synchronous iterator callers onto async advancement.
- Adding true async file I/O.
- Adding public runtime tuning options.
- Changing table header/footer metadata reads.
- Changing public storage formats or recovery protocol.
- Narrowing the publish barrier or adding WAL partitioning.

## Acceptance Gate

- Roadmap records this as the async cursor advancement phase.
- Async range and prefix `next_async` calls advance through internal async
  scan/source/table cursor methods.
- Synchronous `Iterator::next` behavior remains unchanged.
- Persistent async range and prefix tests pass after data has been flushed into
  table files.
- Focused async/table tests, formatting, clippy, full tests, `git diff --check`,
  and forbidden-term scan pass.
- Evidence records remaining async blockers and the recommended next phase.

## Active Task Slice

```text
task367 [x] goal:start async cursor advancement phase | scope:current roadmap | verify:manual
task368 [x] goal:add internal async lazy-scan/source advancement | scope:src/iterator.rs | verify:async tests
task369 [x] goal:add table cursor async advancement hook | scope:src/table.rs | verify:table/async tests
task370 [x] goal:add persistent async range/prefix table coverage | scope:tests/async_api.rs | verify:async tests
task371 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap src tests | verify:git status
```

## Known Blockers

- The new async advancement path will still call synchronous table block decode
  until the next read phase moves block loading behind an awaited owned-read
  completion.
- Table header/footer metadata reads still use the borrowed `read_exact_at`
  path.
- True async file I/O is not implemented.
- Runtime tuning options are still internal.
- True multi-writer execution still needs writer-local deltas and WAL
  partitioning.
- Public recovery report reads remain standalone no-runtime helpers by design.

## Evidence

- Phase 88 measured cache-disabled block-decode point reads under native and
  inline runtime modes. Runtime mode did not materially change today's
  synchronous decode cost.
- Existing `next_async` methods were compatibility wrappers over
  `Iterator::next`, so async callers did not have an internal await point for
  cursor advancement.
- `Iter::next_async` and `LazyIter::next_async` now route lazy scans through
  async internal scan/source/cursor advancement instead of calling
  `Iterator::next` at the public wrapper.
- `TablePointCursor` now has async group/record/block advancement hooks. The
  block-load hook still calls the synchronous table block loader by design, so
  synchronous decode scheduling is unchanged.
- Persistent async range/prefix coverage now flushes rows into table files and
  advances forward, lazy prefix, and reverse lazy prefix through async
  iterators.
- Verification passed: `cargo test --test async_api`, `cargo test table --lib`,
  `cargo test iterator --lib`, `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan outside the repository instruction file.

## Next Recommendation

- Start the async table block-load phase. The awaitable cursor path now exists,
  so the next slice can move async table cursor block loading behind an awaited
  owned-read completion while leaving synchronous iterators on their current
  path.
