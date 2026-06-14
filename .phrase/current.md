# Current Phase

## Status

Complete

## Goal

Complete Phase D from `.phrase/protocol/guard-aware-lsm-strategy.md`: make
the compaction picker use guard-local inputs for multi-table level cleanup when
it can reduce rewrite work without increasing point-read candidate depth.

## Design Assessment

Phase D now keeps compaction inside the existing Trine LSM boundary. It does
not add persisted guard metadata and does not change WAL, manifest, SSTable,
blob, MVCC, or public API contracts. The retained picker change uses the
current table key bounds as in-memory guards.

The important correction exposed by benchmark verification was on the read
side: moving one L1 table to L2 created a guard hole in L1. The non-L0 table
lookup previously used the next table whose largest key was high enough but
did not confirm that the key was also above that table's smallest key. That
could add a false table probe before reaching the moved table in L2. Phase D
therefore includes the read-bound fix needed for the acceptance gate.

## Scope

- Guard-local multi-table compaction fallback.
- Lower-level overlap closure for the selected guard-local input range.
- Persistent regression coverage for moving one guard-local L1 table to L2.
- Non-L0 point lookup guard-hole fix.
- Benchmark diagnostics for broad-estimate vs actual guard-local rewrite work.

## Out Of Scope

- Persisted guard metadata.
- Manifest, WAL, SSTable, blob, or table-format changes.
- Public read/write API naming changes.
- Non-uniform per-level compaction policy.
- Bottom-level lazy or tiered compaction.

## Acceptance Gate

- Compaction input and output bytes drop for guard-local multi-table cleanup.
- Point-read candidate depth stays flat after guard-local compaction.
- L0 and lower-level overlap closure behavior is preserved.
- Table output splitting, manifest publish, and snapshot-delayed cleanup stay
  on the existing paths.
- Blob reachability, tombstone retention, and recovery tests pass.

## Active Task Slice

```text
task831 [x] goal:complete guard-aware compaction picker | scope:src/compaction.rs src/lsm/version.rs benches/v1_bench.rs tests/internal/persistent_wal.rs | verify:compaction/version/persistent/blob/range-delete/snapshot/recovery tests + bench diagnostics + full gate
```

## Evidence

- Multi-table fallback now calls `narrow_leveled_inputs`, so the picker chooses
  one overlapping same-level table and then closes only the lower-level overlap
  for that selected guard range.
- Planner tests cover guard-local multi-table selection and lower-level overlap
  closure.
- Persistent regression coverage proves three L1 tables become two L1 tables
  plus one L2 table, with one `MultiTableLevel` input table and reopen-visible
  data.
- Non-L0 point lookup now verifies `smallest_user_key <= key <=
  largest_user_key` after binary positioning, avoiding false probes into the
  next table when a level has a guard hole.
- Benchmark diagnostics reported guard multi-table compaction broad estimate
  of 4 input tables and 35950 input bytes versus actual 1 input table, 9177
  input bytes, and 9177 output bytes. Point table probes stayed flat at 2048
  before and after compaction.

## Known Risks

- Guard ranges are still derived from table key bounds in memory. Persisted
  guard metadata remains a future decision.
- This phase narrows candidate inputs but does not yet implement non-uniform
  per-level compaction budgets.

## Next Recommendation

- Start Phase E only if new evidence targets non-uniform per-level policy:
  upper-level overlap budget, middle-level guard-local cleanup, and lower-level
  lazy or tiered behavior with explicit space/read/write amplification stats.
