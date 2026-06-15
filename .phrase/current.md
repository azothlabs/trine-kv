# Current Phase

## Status

Phase 1 complete (liveness-gated obsolete cleanup). Phases 2-3 planned.

## Goal

Phase 1 of `.phrase/protocol/snapshot-version-pinning.md`: replace the coarse
"any active snapshot blocks all obsolete-file cleanup" gate with per-table
liveness, so a long-lived snapshot no longer stalls space reclamation. Foundation
for the reader-pins-the-tables-it-needs (LMDB-grade) direction.

## Design Assessment

`Arc<Table>` is held only by versions (current + iterator-pinned old versions)
and transient per-read super-versions; the manifest stores only properties.
Once a table leaves the current version no new reader can acquire it, so its
`Arc` strong count only falls. A cleanup queue that holds the obsolete
`Arc<Table>` can therefore delete a file exactly when `strong_count == 1` (queue
is sole owner) — TOCTOU-safe. Normal compaction stays correct because a snapshot
reads the current version, whose output retains its data (retention is gated by
`oldest_active_snapshot`, which includes the snapshot); only the rewritten input
files are dropped.

## Scope (done)

- `LsmVersion::with_replaced_tables` returns the removed `Arc<Table>`;
  `install_compaction` excludes trivial-move ids and returns the obsolete
  handles; `install_compacted_tables` aggregates them.
- `pending_obsolete_tables: Mutex<Vec<Arc<Table>>>` replaces the id set.
- `retire_obsolete_table_files*` enqueue handles; cleanup drains via
  `take_deletable_obsolete_tables` (`strong_count == 1`), retries the rest, and
  re-queues on delete failure. The coarse `active_count()` gate is gone.
- Cleanup runs after the compaction call returns (locals dropped) so deletion is
  prompt in the same `compact_range_sync` / `flush_sync` call.

## Out Of Scope (Phase 1)

- Blob-file cleanup stays snapshot-gated (table liveness first; blobs are
  reachable through pinned tables — Phase 2/later).
- Snapshot reading through a pinned version (Phase 2) and instant
  truncate-with-old-snapshot (Phase 3).

## Acceptance Gate

- `obsolete_tables_drop_while_point_snapshot_open`: obsolete inputs deleted even
  with a snapshot open; snapshot reads stay correct. Met.
- `obsolete_table_files_kept_until_inflight_iterator_drops`: a live iterator
  pins the files; after it drops, cleanup reclaims them. Met.
- `persistent_compaction_rewrites_tables_and_preserves_reads` updated: obsolete
  inputs reclaimed despite a pinned snapshot, which still reads its old view from
  the retained output. Met.
- Full local gate: fmt + clippy --all-targets --all-features clean; `--lib` 371,
  `--all-features` 375 pass / 1 ignored; `check --benches` clean.

## Next Recommendation

- Phase 2: `Snapshot` captures a consistent read super-version (version +
  memtable sources, lazily per bucket) and reads through it. Then Phase 3: instant
  file-drop / truncate decoupled from `oldest_active_snapshot`.
- After this initiative: remaining committed backlog — layered-filter Phase 5
  ([[layered-filter-followups]]) and delete-gc Phase 4b ([[delete-gc-lifecycle]]).
