# Current Phase

## Status

Backlog cleanup: delete-gc Phase 4b done (source-level range-tombstone GC).
snapshot-version-pinning Phase 1 done+merged, Phases 2-3 deferred. Next:
layered-filter Phase 5 (D1 cost-weighted remote curve), measure-first.

## Goal

Reclaim the read amplification a partially-covering `delete_range` leaves behind
(the measured 10x scan-waste case), at the source instead of at every read.

## Design Assessment

Root cause (traced from the diagnostic, not assumed): compaction merged the range
tombstone with the data but never dropped the covered point records, so deleted
rows survived compaction and were filtered on every scan. The originally-planned
fix (Phase 4b block-level read-skip needing per-block max-seq = a table-format
change) was the wrong tool. The right fix is source-level: drop covered point
records during the merge, retention-gated exactly like the tombstone.

## Scope (done)

- `drop_records_covered_by_droppable_tombstone` in `src/lsm/compact.rs`: in the
  per-user-key merge, drop a record when a range tombstone covers its key, is
  strictly newer (`record.seq < tombstone.seq`), and is retention-safe
  (`tombstone.seq <= oldest_active_snapshot`). Runs before
  `mark_tombstones_covering_records` so a fully-covered tombstone is then cleaned
  up in full compaction; partial compaction keeps the tombstone to hide lower
  levels. No table-format change.

## Acceptance Gate

- Tombstone-scan-waste diagnostic: ~10x (1024 internal / 102 user, 922 hidden)
  → 1.0x (102/102, 0 hidden). Met.
- `compaction_drops_range_deleted_keys_at_source` (covered rows physically gone;
  a write after the delete survives), `compaction_keeps_range_deleted_keys_for_older_snapshot`
  (an older snapshot still reads covered rows; reclaimed after it drops), and two
  unit tests for the drop predicate. Met.
- Full local gate: fmt + clippy --all-targets --all-features clean; `--lib` 375,
  `--all-features` 379 pass / 1 ignored; `check --benches` clean.

## Next Recommendation

- layered-filter Phase 5 D1 (cost-weighted curve for the `s3` remote backend):
  measure first — the classic-Monkey curve starves deep levels, which is wrong
  when a deep-level filter miss costs a network read. D2 (dynamic per-hot-guard)
  stays deferred until static evidence shows headroom.
- Deferred: snapshot-version-pinning Phases 2-3 (see that protocol).
