# Current Phase

## Status

Complete

## Goal

Make persistent background maintenance the normal path and give writes clear
pressure behavior when immutable memtables, L0 files, or compaction debt build
up.

## Entry Condition

- Phase 40 completed table read-path index hardening.
- User identified background flush/compaction scheduling, write backpressure,
  writer-lock scope, compaction picker locality, concurrent compaction bounds,
  and long-running compaction validation as the next risks.

## Scope

- Start persistent databases with a default background maintenance worker while
  keeping `background_worker_count == 0` as an explicit manual-maintenance mode.
- Replace the single maintenance request bit with flush/compaction requests,
  in-flight state, progress notification, and error propagation.
- Make writes wait or help maintenance when immutable memtables or L0 files are
  over configured limits.
- Keep writer coordinator work focused on sequence/WAL/memtable commit and
  short publish cutovers; table building and compaction merge work should run
  outside that lock.
- Pick compaction inputs by local key span, especially for L0 pressure, instead
  of rewriting every overlapping table when a narrower span is enough.
- Prevent concurrent compactions of overlapping ranges in the same bucket while
  allowing non-overlapping ranges to proceed.
- Add tests for level non-overlap, MVCC retention, range-delete preservation,
  default workers, and backpressure behavior.

## Out Of Scope

- Changing public read/write APIs.
- Adding async runtime dependencies.
- Rewriting blob GC scheduling beyond keeping existing safety.
- New user-facing tuning knobs unless evidence shows the derived thresholds are
  insufficient.

## Acceptance Gate

- Persistent default options start one background worker; in-memory and read-only
  databases still do not start workers.
- Writes do not perform table flush or compaction build work while holding the
  writer coordinator.
- Writes apply bounded pressure handling before accepting more work when
  immutable memtables or L0 tables exceed configured limits.
- Background maintenance failures surface through later writes, `flush()`, or
  `compact_range()`.
- L0 compaction can select a local overlapping group and leave unrelated L0
  files for later passes.
- Concurrent compaction reservations reject overlapping same-bucket key ranges
  and allow non-overlapping ranges.
- Full local Rust verification passes.

## Active Task Slice

```text
task139 [x] goal:maintenance coordinator queue/progress/errors | scope:src/db.rs | verify:background maintenance tests
task140 [x] goal:writer backpressure and shorter lock scope | scope:src/db.rs src/db/commit.rs | verify:pressure tests
task141 [x] goal:local compaction picker spans | scope:src/compaction.rs src/lsm/compact.rs | verify:picker tests
task142 [x] goal:compaction reservation boundaries | scope:src/db.rs tests | verify:concurrent compaction tests
task143 [x] goal:protocol/docs/evidence update | scope:.phrase docs README | verify:full Rust verification
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.

## Evidence

- Rust skill, concurrency skill, SPEC-AGENTS context, and the coding module
  were read before implementation.
- Initial code audit found that persistent background workers exist but are
  disabled by default, maintenance requests are a single coalesced bit, writes
  can flush immutable memtables while holding the writer coordinator, and
  compaction holds the writer coordinator across input selection and output
  table construction.
- Persistent default options now start one maintenance worker, while
  `background_worker_count == 0`, in-memory open, and read-only open keep
  maintenance manual.
- Maintenance coordination now tracks separate flush/compaction requests,
  in-flight flush state, in-flight compaction key ranges, progress, shutdown,
  and the last background error.
- Writes apply pressure handling before taking the writer coordinator. They can
  wait for background progress or help with one foreground flush/compaction
  pass, and pressure flush remains bucket-local.
- Flush table writing and compaction output construction now run outside the
  writer coordinator; the lock is kept for commit sequencing, freeze cutovers,
  manifest publish, state install, and WAL replay-floor rewrite.
- Automatic L0 compaction can choose a local seed span; explicit
  `compact_range` keeps requested-range behavior.
- Compaction reservations reject overlapping ranges in the same bucket and
  allow non-overlapping ranges or different buckets to proceed.
- Verification passed: `cargo test --all-targets --all-features`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --all --check`, `git diff --check`, and the forbidden-term scan.

## Next Recommendation

- Commit Phase 41, then use remote CI as the external release signal.
