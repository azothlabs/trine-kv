# Current Phase

## Status

Complete

## Goal

Phase 2 of `.phrase/protocol/delete-gc-lifecycle.md`: add a tombstone-aware
compaction trigger so range tombstones meet and drop the data they cover instead
of lingering on the read path.

## Design Assessment

v1 targets range-tombstone debt using the cheap, format-free
`Table::may_have_range_tombstones` footer flag (no manifest change). Point-
tombstone density needs per-table entry/deletion counts that are not in
`TableProperties`; that is a deferred manifest bump if Phase 1 scan-waste shows
point-tombstone-heavy pressure. Range tombstones are the dangerous big-delete /
drop-prefix case, so they are the high-leverage start. The trigger plugs into the
Phase E guard-aware picker.

## Scope

- `CompactionTable.has_range_tombstones`; new `CompactionTrigger::TombstoneDebt`.
- Picker fires `TombstoneDebt` after `L0Overlap`/`LevelSize` and before the
  no-pressure spread, on the shallowest non-level-0, non-deepest level holding an
  in-range range-tombstone table, compacting it down with overlapping lower data.
- Termination/anti-storm: fires only with lower-level overlap (a pure move just
  relocates the tombstone), excludes the deepest level, and is lower priority
  than size pressure.

## Out Of Scope

- Point-tombstone density score (needs `TableProperties` entry/deletion counts =
  manifest bump); deferred.
- Bulk file drop (Phase 3), read-path whole-range skip (Phase 4).

## Acceptance Gate

- Range-tombstone table with lower overlap plans `TombstoneDebt`; without overlap
  or at the deepest level it does not. Met (planner + `LsmTree` tests).
- No storage-format change; compaction/range-delete/recovery suites pass. Met.
- Full local gate. Met (one pre-existing background-timing flake).

## Evidence

- Planner tests: `range_tombstone_table_with_lower_overlap_triggers_tombstone_debt`,
  `..._without_lower_overlap_is_left_alone`,
  `range_tombstone_at_deepest_level_does_not_trigger_tombstone_debt`.
- `LsmTree` test `range_tombstone_table_with_lower_overlap_plans_tombstone_debt`
  proves the `may_have_range_tombstones` flag flows through to the trigger.
- `cargo test --lib` (363) and `--all-features` (367) green; fmt/clippy/diff clean.

## Known Risks

- A range tombstone whose covered data is pinned by a long snapshot re-fires
  guard-locally as it migrates down, but terminates at the deepest level; bounded
  work, lower priority than size pressure.

## Next Recommendation

- Phase 3 (bulk drop via file drop) is the biggest user-facing win for
  drop-table/tenant/prefix. Then Phase 4 (read-path whole-range skip). Add
  point-tombstone density (manifest bump) only if Phase 1 metrics demand it.
