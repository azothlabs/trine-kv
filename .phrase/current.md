# Current Phase

## Status

Complete

## Goal

Add async compatibility advancement for range/prefix cursors and lazy values
while preserving existing blocking iterator behavior.

## Scope

- Add async compatibility methods for `Iter` and `LazyIter` advancement.
- Add async compatibility methods for `LazyValue` and `LazyKeyValue` value
  reads/conversion.
- Keep existing `Iterator` implementations unchanged.
- Extend the focused async smoke test to consume rows through async cursor
  advancement.
- Preserve WAL/table/blob/manifest formats, recovery policy, MVCC visibility,
  compaction behavior, stats behavior, cleanup behavior, and storage behavior.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Moving production in-memory object routing into backend operations.
- Making native-file storage non-blocking.
- Renaming existing blocking methods or removing `Iterator` implementations.
- Changing write-path concurrency or cancellation semantics.
- Changing Blob read storage routing.

## Acceptance Gate

- Roadmap records the async cursor compatibility phase at phase granularity.
- Current phase records that this is additive cursor advancement, not a
  non-blocking storage migration.
- `Iter` exposes async compatibility advancement returning
  `Result<Option<KeyValue>>`.
- `LazyIter` exposes async compatibility advancement returning
  `Result<Option<LazyKeyValue>>`.
- `LazyValue` and `LazyKeyValue` expose async compatibility read/conversion
  methods.
- A focused memory-mode async smoke test consumes normal and lazy iterators
  through async cursor methods without requiring an external runtime crate.
- Existing blocking API tests continue to pass.
- `cargo fmt --check`, focused async API tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records remaining async-first blockers after the slice.

## Active Task Slice

```text
task274 [x] goal:start async cursor compatibility slice | scope:current roadmap | verify:manual
task275 [x] goal:add cursor and lazy value async methods | scope:src/iterator.rs | verify:async smoke test
task276 [x] goal:extend async API smoke test for cursor advancement | scope:tests/async_api.rs | verify:focused test
task277 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task278 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this async cursor compatibility slice.
- Async runtime selection, non-blocking native-file execution,
  cancellation-safe write acceptance, Blob read storage routing, and production
  in-memory object routing remain later phases.

## Evidence

- Phase 71 added async construction methods for range/prefix iterators, but
  cursor advancement itself is still blocking-only through `Iterator::next`.
- The async-first protocol expects cursor advancement to have an async public
  shape.
- A compatibility method returning `Result<Option<_>>` matches the target API
  shape without removing existing iterator behavior.
- `Iter::next_async` now returns `Result<Option<KeyValue>>`.
- `LazyIter::next_async` now returns `Result<Option<LazyKeyValue>>`.
- `LazyValue` and `LazyKeyValue` now expose async compatibility read/conversion
  methods.
- The async API smoke test now consumes normal and lazy cursors through async
  advancement.
- Verification passed: `cargo fmt --check`, `cargo test
  memory_async_compatibility_surface_smoke --test async_api`, `cargo clippy
  --all-targets --all-features -- -D warnings`, `cargo test --all-targets
  --all-features`, `git diff --check`, and forbidden-term scan.

## Next Recommendation

- Commit this slice, then reassess cancellation-safe write acceptance.
