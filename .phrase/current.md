# Current Phase

## Status

Complete

## Goal

Phase 3 of `.phrase/protocol/delete-gc-lifecycle.md`: drop wholly-covered files
during bulk delete (DeleteFilesInRange-style) instead of reading and rewriting
them, general and layout-agnostic, MVCC-correct.

## Design Assessment

Investigating the snapshot model showed Trine point reads use the current
version + sequence filter (no per-snapshot version pin), so an instant
"remove files now" API would break older snapshots (MVCC violation). The
MVCC-correct bulk delete is already a range tombstone (`delete_range`, any
range). File drop is therefore a retention-gated compaction optimization, not a
new unsafe API, and it stays layout-agnostic (no tenant==bucket assumption).
Deeper "snapshots pin a version handle" (LMDB-grade MVCC) is a separate future
initiative, deliberately not in this phase.

## Scope

- Compaction skips reading/rewriting an input table entirely covered by one
  range tombstone when (a) it spatially covers the table span, (b) the table's
  largest sequence <= the tombstone sequence, (c) the tombstone sequence <=
  `oldest_active_snapshot`. The table drops by file via the existing
  `input_table_ids` -> obsolete -> snapshot-safe cleanup.
- Pure coverage decision function, unit-tested.

## Out Of Scope

- `truncate_bucket` / `drop_range` ergonomic wrappers (deferred sugar).
- Multi-tombstone coverage of one table (single-tombstone only; conservative).
- Snapshot version-handle pinning (separate LMDB-grade MVCC initiative).
- A dedicated covered-files-dropped counter (4-caller ripple; deferred).

## Acceptance Gate

- Coverage decision unit-tested: covered / record-too-new / snapshot-too-old /
  partial-spatial. Met.
- Bulk `delete_range(all)` drops covered data tables by file: data gone, durable
  across reopen, table bytes not grown by a rewrite. Met.
- No storage-format change; range-delete + snapshot correctness suites pass
  (the gate matches the merge's own retention). Met.
- Full local gate. Met (one pre-existing background-timing flake).

## Evidence

- Unit tests `*_droppable*` / `partially_covered_table_is_not_droppable`.
- `bulk_range_delete_drops_covered_tables_by_file`: 200 keys, delete_range(all),
  compact -> all keys gone, durable across reopen, `table_bytes` not grown.
- `cargo test --lib` (368) and `--all-features` (372) green; fmt/clippy/diff clean.

## Known Risks

- Conservative single-tombstone coverage: a table covered only by several
  tombstones together is not dropped by file (falls back to rewrite); correct,
  just not optimized.

## Next Recommendation

- Phase 4 (read-path whole-range skip: skip blocks fully covered by visible
  tombstones during scans), gated by Phase 1 scan-waste evidence. Optional sugar:
  `truncate_bucket`/`drop_range`. Optional: covered-files-dropped counter.
