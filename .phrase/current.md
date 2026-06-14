# Current Phase

## Status

Complete

## Goal

Complete Phase C from `.phrase/protocol/guard-aware-lsm-strategy.md`: make
guard-aware range and prefix scan pruning safe for range-shaped operations,
including range tombstones that can hide returned records.

## Design Assessment

Phase B made point and grouped point reads use existing table key bounds to
avoid unrelated L0 candidates. Phase C needed the same guard boundary for scans,
but scan correctness is stricter: a table can matter even when it has no point
records in the queried range if it carries a range tombstone that hides those
records.

The retained change is intentionally conservative. Tables with complete key
bounds still use those bounds for point and scan candidate selection. Tables
with ambiguous bounds and range tombstones are treated as possible candidates,
so scan and point-read tombstone checks cannot skip a table only because its
guard bounds are incomplete.

## Scope

- Range scan candidate diagnostics.
- Range and prefix scan tombstone-table diagnostics.
- Conservative key-bound handling for tombstone tables with ambiguous bounds.
- Persistent regression coverage for tombstone-only table scans, reverse scans,
  prefix scans, snapshots, and reopen.

## Out Of Scope

- Persisted guard metadata.
- Manifest, WAL, SSTable, or table-format changes.
- Public read/write API naming changes.
- Compaction policy changes beyond the already closed Phase 188 picker gate.
- Bottom-level lazy/tiered policy work.

## Acceptance Gate

- Range, prefix, reverse, snapshot, and range-delete tests pass.
- Diagnostics show range scan table candidates and tombstone-table candidates.
- Prefix diagnostics show tombstone-table candidates.
- Tombstone-only or ambiguous-bound tables remain visible to point and scan
  tombstone checks.
- Transaction conflict checks remain conservative; no narrower conflict scan is
  introduced in this phase.

## Active Task Slice

```text
task830 [x] goal:complete guard-aware scan and range-delete safety | scope:src/table.rs src/lsm/scan.rs src/lsm/version.rs src/stats.rs benches/v1_bench.rs tests/internal/persistent_wal.rs | verify:range/prefix/reverse/snapshot/range-delete tests + bench diagnostics + full gate
```

## Evidence

- `ReadPathStats` now reports range scan table probes, L0/non-L0 range table
  probes, range tombstone-table probes, and prefix tombstone-table probes.
- `LsmTree::scan_sources` records range table candidates and keeps prefix
  table candidate recording.
- `scan_range_tombstones` records tombstone-table candidates for both range and
  prefix selectors.
- `Table::key_bounds_may_contain_key` and `Table::key_bounds_overlap_range`
  now keep tables with ambiguous bounds and range tombstones conservative.
- A version unit test proves ambiguous tombstone tables remain scan and point
  tombstone candidates.
- A persistent regression test proves a tombstone-only table hides range,
  reverse range, prefix, and reverse prefix scans while snapshots and reopen
  behavior remain correct.
- Bench diagnostics now include `read pruning range guarded`, `read pruning
  range tombstone guarded`, and prefix tombstone-table probe rows.

## Known Risks

- The conservative ambiguous-bound path may inspect more tombstone tables when
  table bounds are incomplete. That is intentional for correctness.
- Range scan diagnostics currently focus on table and tombstone-table
  candidates, not range data-block metadata counters.

## Next Recommendation

- Start the next phase from explicit read-frequency or range-scan workload
  evidence before broadening compaction policy beyond the current L0 picker
  gate.
