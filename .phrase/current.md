# Current Phase

## Status

Complete

## Goal

Phase 4 of `.phrase/protocol/delete-gc-lifecycle.md`: skip reading source tables
fully hidden by a visible range tombstone during scans, cutting read
amplification from deleted-but-not-yet-compacted data. ROI confirmed first by
Phase 1 measurement (10x scan read amplification).

## Design Assessment

A scan cannot blindly skip a covered span: a write newer than the tombstone
inside it must still surface. The safe v1 is table-level: skip a source table
only when one visible tombstone (`seq <= read_sequence`) spatially covers the
whole table span and is strictly newer than every record in it
(`table.largest_sequence < tombstone.seq`). Then the table is provably all-hidden
for this read and is never read. Complements Phase 3 (physical drop when
retention-safe for all readers) by skipping on a per-read basis even while the
table must stay for older readers.

## Scope

- `ScanRangeTombstone::fully_hides_table` (visible + spatial cover + strictly
  newer than the table).
- `scan_sources` skips building a cursor for any fully-hidden source table;
  `scan`/`scan_async` compute tombstones first and pass them + `read_sequence`.

## Out Of Scope

- Within-table block-level skip for partially-covered tables (Phase 4b; needs
  per-block max-sequence metadata = table-format change).
- A skipped-tables counter.

## Acceptance Gate

- A scan over a fully-covered-but-present table skips it: only live keys return,
  `scan_internal_records ~= scan_user_keys`, covered keys not read as
  delete-hidden. Met (`scan_skips_fully_covered_table_on_read_path`).
- Range-delete + snapshot scan correctness preserved (gate matches range
  tombstone visibility; newer-than-tombstone writes still surface). Met.
- Full local gate. Met (one pre-existing background-timing flake).

## Evidence

- `scan_skips_fully_covered_table_on_read_path`: two tables (a*, z*), delete a*
  with a fresh tombstone, scan -> 50 z keys, `internal ~= user`, hidden ~0
  (the a* table skipped without reading).
- Single-table-partial diagnostic still 10x (unchanged): that case is Phase 4b.
- `cargo test --lib` (368) and `--all-features` (373) green; fmt/clippy/diff clean.

## Known Risks

- v1 only skips whole tables; a single table with an internal covered hole still
  pays per-key (Phase 4b). Conservative single-tombstone coverage.

## Next Recommendation

- delete-gc-lifecycle Phases 1-4 (v1) are complete. Remaining optional: Phase 4b
  (block-level skip, table-format change), `truncate_bucket`/`drop_range` sugar,
  point-tombstone density (manifest bump), covered/skipped counters. Do per
  demand. Separate big direction recorded: snapshot version-handle pinning
  (LMDB-grade MVCC).
