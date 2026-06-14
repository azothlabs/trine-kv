# Current Phase

## Status

Complete

## Goal

Complete Phase E from `.phrase/protocol/guard-aware-lsm-strategy.md`: move the
compaction picker from one uniform style to a non-uniform per-level policy.
Upper levels keep a tight overlap budget, middle levels keep guard-local
cleanup, and lower (deep) levels become lazy/tiered unless a justified trigger
(space amplification) requires a rewrite. Add policy stats that explain why a
compaction ran or did not run.

## Design Assessment

The picker already has three triggers: `L0Overlap` (the upper-level overlap
budget, gated by `max_l0_files`), `LevelSize` (size-pressure, justified at any
depth), and a no-pressure `MultiTableLevel` fallback that currently fires
uniformly on any non-L0 level with at least two in-range tables.

Because L1+ levels are validated non-overlapping, two in-range tables at a deep
level never overlap the same user key, so the fallback gives zero point-read
candidate benefit there. At deep levels it is pure write amplification:
rewriting a deep level only to spawn an even deeper level.

Phase E replaces the hardcoded `>= 2` fallback threshold with a depth-scaled
file budget `threshold(level) = 1 + level` (L1 -> 2, L2 -> 3, L3 -> 4, ...).
Shallow levels stay tight (L1 still merges at >= 2, preserving Phase D's L1->L2
spreading); deeper levels tolerate more non-overlapping tables before a
no-pressure merge. `LevelSize` (space amplification) still compacts any
over-target level at any depth, so lower levels are lazy *unless* a trigger
justifies the rewrite. This is in-memory only; no manifest/SSTable/format
change (that remains the Phase F decision).

## Scope

- Depth-scaled per-level no-pressure fallback budget in the picker.
- New `CompactionSkip::LowerLevelLazy` policy stat for the "did not run" side,
  recorded when a level the old uniform rule would merge is left lazy.
- `DbStats::compaction_skips` reporting, mirroring `compaction_triggers`.
- Planner unit tests + an integration/benchmark diagnostic showing reduced
  lower-level write amplification with flat point-read candidate depth.

## Out Of Scope

- Persisted guard metadata or any manifest/WAL/SSTable/blob format change.
- New persisted `BucketOptions`/`DbOptions` fields (would touch manifest).
- Public read/write API naming changes.
- Tombstone/blob/overlap-explosion triggers beyond the existing LevelSize and
  L0Overlap triggers (space amplification is the Phase E justified trigger).

## Acceptance Gate

- Write amplification improves for lower-level (deep-level) workloads.
- Point-read candidate depth stays flat after the policy change (read
  amplification does not regress beyond the configured overlap budget).
- Space use (per-level bytes) and obsolete data (blob) stay reported and
  bounded through existing stats.
- L0 overlap closure and guard-local middle-level cleanup are preserved.
- Full local Rust gate and grouped benchmark evidence pass.

## Active Task Slice

```text
task832 [x] goal:non-uniform per-level compaction picker + policy skip stats | scope:src/compaction.rs src/lsm/compact.rs src/db.rs src/stats.rs src/lib.rs tests/internal/persistent_wal.rs | verify:compaction/lsm/stats tests + persistent lower-level-lazy regression + full gate + grouped bench
```

## Evidence

- `multi_table_fallback_threshold(level) = 1 + level` replaces the uniform
  `>= 2` no-pressure fallback rule, so shallow levels stay tight (L1 still merges
  at two tables) and deep levels stay lazy until a trigger fires.
- `CompactionDecision` carries an optional `CompactionSkip`; `LsmTree::
  plan_compaction` returns `CompactionPlanResult { input, skip }`; `Db::collect_
  compaction_inputs` records `CompactionSkip::LowerLevelLazy` into
  `DbStats::compaction_skips`.
- Planner unit tests cover: shallow level still merges at two tables, deep level
  stays lazy below budget (reports the skip), deep level merges once budget is
  reached, and deep size pressure still compacts via `LevelSize`.
- `persistent_deep_level_stays_lazy_and_reports_skip` proves the end-to-end DB
  path: a deep level is left lazy (skip recorded), point table probes stay flat,
  and data stays correct.
- Phase D regression `persistent_multi_table_compaction_moves_one_guard_local_
  input` still passes (three L1 tables -> one moved to L2).
- Grouped benchmark held the guard multi-table row at 1 input table / 9177
  input+output bytes with probes flat at 2048.

## Known Risks

- The depth-scaled budget `1 + level` is an in-memory heuristic, not a persisted
  policy; tuning it further may need benchmark evidence.
- Lower-level lazy behavior relies on `LevelSize` to eventually reclaim deep
  space; tombstone/blob-specific deep triggers remain future work.

## Next Recommendation

- After Phase E, evaluate Phase F (persisted guard metadata) only if open/recover
  cost of deriving guards from table bounds becomes a measured problem.
