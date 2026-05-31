# Current Phase

## Status

Complete

## Goal

Introduce the first public async compatibility surface for opening databases
and common point/write operations while preserving the existing blocking API.

## Scope

- Add async compatibility methods for database open helpers.
- Add async compatibility methods for default-bucket point reads, writes,
  deletes, batch writes, persistence, flush, compaction, and close.
- Add async compatibility methods for bucket point reads, writes, deletes, and
  basic range/prefix iterator construction.
- Keep existing blocking methods unchanged.
- Preserve WAL/table/blob/manifest formats, recovery policy, MVCC visibility,
  compaction behavior, stats behavior, cleanup behavior, and storage behavior.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Moving production in-memory object routing into backend operations.
- Renaming existing blocking methods or changing `Db::open`'s current return
  type in this compatibility slice.
- Making native-file storage non-blocking.
- Converting cursors so `next` itself is async.
- Changing write-path concurrency or cancellation semantics.

## Acceptance Gate

- Roadmap records the async compatibility API phase at phase granularity.
- Current phase records that this is an additive compatibility surface, not a
  runtime or non-blocking storage migration.
- `Db` exposes async compatibility methods for open, point read/write/delete,
  batch write, persist, flush, compaction, and close.
- `Bucket` exposes async compatibility methods for point read/write/delete and
  iterator construction.
- A focused memory-mode async smoke test passes without requiring an external
  runtime crate.
- Existing blocking API tests continue to pass.
- `cargo fmt --check`, focused async API tests, clippy,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records remaining async-first blockers after the slice.

## Active Task Slice

```text
task269 [x] goal:start async compatibility API slice | scope:current roadmap | verify:manual
task270 [x] goal:add Db async compatibility methods | scope:src/db.rs | verify:async smoke test
task271 [x] goal:add Bucket async compatibility methods | scope:src/bucket.rs | verify:async smoke test
task272 [x] goal:run verification gate | scope:workspace | verify:fmt clippy tests diff
task273 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this async compatibility slice.
- Async runtime selection, non-blocking native-file execution, async cursor
  advancement, cancellation-safe write acceptance, and production in-memory
  object routing remain later phases.

## Evidence

- The async-first protocol requires async public database and bucket APIs.
- Current public database and bucket APIs are blocking-only.
- Storage backend operations already expose internal async-shaped futures plus
  blocking adapters.
- A first public compatibility layer can be additive and runtime-free while
  preserving the existing blocking API.
- `Db` now exposes async compatibility methods for open helpers, default-bucket
  point operations, batch writes, iterator construction, persistence, flush,
  compaction, and close.
- `Bucket` now exposes async compatibility methods for point operations and
  iterator construction.
- The compatibility surface does not choose a runtime and does not claim
  native-file storage is non-blocking.
- Focused verification passed: `cargo test
  memory_async_compatibility_surface_smoke --test async_api`.
- Full verification passed: `cargo fmt --check`, `cargo clippy
  --all-targets --all-features -- -D warnings`, `cargo test --all-targets
  --all-features`, `git diff --check`, and forbidden-term scan.

## Next Recommendation

- Commit this slice, then reassess async cursor advancement and
  cancellation-safe writes.
