# Current Phase

## Status

Complete

## Goal

Fix public maintenance API barrier semantics so foreground `flush()` and
`compact_range()` cannot report success just because background maintenance
already owns a non-blocking guard.

## Entry Condition

- Phase 42 completed persistent read-path resource policy.
- User identified that public `Db::flush()` reused the same best-effort helper
  as background flush, and that `compact_range()` had the same silent-success
  risk on reservation conflicts.

## Scope

- Keep background workers and write-pressure maintenance non-blocking.
- Make public `Db::flush()` capture the call sequence boundary under the writer
  coordinator and wait until all immutable memtables at or below that boundary
  have been published.
- Add maintenance wait helpers for pending flush/compaction requests and active
  guards.
- Make public `compact_range()` wait and retry on overlapping compaction
  reservations while preserving the existing one-pass/no-work public behavior.
- Update protocol and user docs to state the barrier semantics.
- Add focused regression tests for flush guard contention, default-background
  flush completion, and compact-range contention.

## Out Of Scope

- Changing in-memory mode flush behavior.
- Making `compact_range()` exhaust every future level after one successful
  compaction pass.
- Reworking compaction picker behavior beyond conflict waiting.

## Acceptance Gate

- `Db::flush()` returns `Ok(())` only after writes committed before the call are
  out of active and immutable memtables in persistent mode.
- Background workers still use best-effort helpers.
- `Db::compact_range()` does not silently succeed when the requested range is
  blocked by an active compaction reservation.
- Protocol, usage docs, roadmap, current phase, and evidence are updated.
- Full local Rust verification passes.

## Active Task Slice

```text
task149 [x] goal:flush public barrier | scope:src/db.rs src/lsm/write.rs | verify:flush guard/default worker tests
task150 [x] goal:compact_range conflict barrier | scope:src/db.rs | verify:reservation contention test
task151 [x] goal:protocol/docs/evidence update | scope:.phrase docs | verify:full Rust verification
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.

## Evidence

- Rust skill, concurrency skill, SPEC-AGENTS context, and current roadmap files
  were read before implementation.
- Audit confirmed `Db::flush()` returned after `run_flush_once` reported
  `false` when another flush guard was active.
- `Db::flush()` now freezes active memtables at the captured
  `last_committed_sequence` under the writer coordinator, then waits until no
  immutable memtable at or below that sequence remains.
- `compact_range()` now retries after overlapping active compaction
  reservations, while returning after one successful compaction pass or no
  available plan to preserve its prior public shape.
- Verification passed: focused maintenance tests,
  `cargo test --all-targets --all-features`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --all --check`, `git diff --check`, and the forbidden-term scan.

## Next Recommendation

- Commit Phase 43, then use remote CI as the external release signal.
