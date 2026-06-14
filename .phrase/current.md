# Current Phase

## Status

Active

## Goal

Start Phase 188: use the Phase 187 read-path and rewrite-cost counters to make
guard-aware compaction policy changes measurable before retaining behavior
changes.

## Design Assessment

The next useful boundary is compaction choice, not another broad rewrite.
Phase 187 proved that Trine can measure L0 read candidates and per-level
rewrite bytes. The picker now needs the same level of explanation: why a
compaction ran, which level or overlap shape caused it, and whether the selected
work reduced local read or rewrite cost without increasing lower-level churn.

The first slice must remain diagnostic. The picker already has local L0 and
overfull-level paths, so changing behavior before the reason counters are
visible would make later benchmark changes hard to explain.

## Scope

- Compaction trigger/reason stats for successful compaction inputs.
- Benchmark rows that split compaction rewritten bytes by trigger reason.
- Local/full compaction comparison workloads that use the new trigger and
  per-level counters.
- Guard-aware picker changes only after diagnostics show the selected workload
  has avoidable rewrite or candidate cost.

## Out Of Scope

- Persisted guard metadata.
- Manifest, WAL, SSTable, or table-format changes.
- Public read/write API naming changes.
- Durability, recovery, writer lease, platform I/O, publishing, tagging, or
  pushing changes.
- Bottom-level lazy/tiered policy changes before trigger-level evidence exists.

## Acceptance Gate

- `DbStats` reports compaction input/output tables and bytes by trigger reason.
- Bench diagnostics report compaction runs, input/output tables, input/output
  bytes, and rewritten bytes by trigger reason.
- Focused tests prove L0, level-size, and multi-table planner reasons are
  assigned correctly.
- Persistent stats tests prove public `DbStats` exposes trigger counts after a
  successful compaction.
- Any retained picker behavior change must reduce rewritten bytes or read
  candidate depth in a targeted workload without weakening MVCC, range-delete,
  blob reachability, manifest recovery, or storage formats.

## Active Task Slice

```text
task827 [x] goal:add compaction trigger diagnostics without changing picker behavior | scope:src/compaction.rs src/lsm/compact.rs src/db.rs src/stats.rs benches/v1_bench.rs tests/internal/persistent_wal.rs | verify:planner tests + persistent stats tests + bench trigger rows
task828 [ ] goal:add local-vs-broad compaction comparison workload | scope:benches/v1_bench.rs | verify:bench rows compare trigger/per-level rewritten bytes and read candidate counters
task829 [ ] goal:retain first guard-aware compaction picker change only if task828 proves avoidable rewrite | scope:src/compaction.rs src/db.rs | verify:compaction/blob/recovery tests + rewrite/candidate benchmark evidence
```

## Evidence

- Phase 187 completed L0 point-read candidate diagnostics, first L0 key-bounds
  pruning, and per-level compaction rewritten-byte diagnostics.
- The latest `write amp compaction diagnostic` benchmark row showed one L0
  compaction run with 35950 input bytes from level 0, 34931 output bytes to
  level 1, and 70881 rewritten bytes total.
- Task827 added `CompactionTrigger` stats and benchmark rows. The filtered
  bench output now reports `trigger l0-overlap` with 1 run, 4 input tables, 1
  output table, 35950 input bytes, 34931 output bytes, and 70881 rewritten
  bytes for the standard compaction write-amplification diagnostic.

## Known Risks

- Trigger stats must only count successfully installed compactions.
- Reason counters must stay aligned with the picker path, not with later blob
  GC replacement-table writes.
- A picker change that reduces one compaction's rewritten bytes may increase
  future read depth or lower-level rewrite churn.

## Next Recommendation

- Implement `task828`: add a local-vs-broad compaction comparison workload
  before retaining any new compaction-picker behavior.
