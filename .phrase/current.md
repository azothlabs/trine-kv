# Current Phase

## Status

Backlog cleared. Done this session: snapshot-version-pinning Phase 1 (merged;
Phases 2-3 deferred); delete-gc Phase 4b (source-level range-tombstone GC);
layered-filter Phase 5 D1 (opt-in cost-weighted curve). D2 still deferred.

## Goal

Honor the committed backlog (delete-gc 4b, layered-filter Phase 5) under
measure-first discipline: build only what has measured ROI or is a safe opt-in,
never flip a default we cannot validate.

## What Shipped

- **delete-gc 4b** (`src/lsm/compact.rs`): `drop_records_covered_by_droppable_tombstone`
  drops range-tombstone-covered point records during the merge (retention-gated).
  Diagnostic 10x -> 1.0x. Supersedes the planned block-level read-skip + table
  format change. See `.phrase/protocol/delete-gc-lifecycle.md` Phase 4b.
- **layered-filter Phase 5 D1** (`FilterDepthCurve::CostWeighted { step, ceil }`):
  ascending curve for remote/`s3` backends; opt-in only, default NOT flipped;
  manifest curve tag 3 (no version bump). See
  `.phrase/protocol/layered-filter-allocation.md` Phase 5.

## Acceptance Gate

- delete-gc 4b: diagnostic 10x -> 1.0x; covered rows physically dropped; a write
  after the delete survives; an older snapshot keeps covered rows until it drops.
- D1: `level_adjusted_filter_bits` ascending cases + manifest round-trip; default
  Auto unchanged and existing data compatible.
- Full local gate: fmt + clippy --all-targets --all-features clean; `--lib` 376,
  `--all-features` 380 pass / 1 ignored; `check --benches` clean.

## Next Recommendation

- Deferred, measure-/demand-gated: snapshot-version-pinning Phases 2-3 (need a
  workload that truncates while an older snapshot is held); layered-filter Phase 5
  D1 default-flip + tuning (need an s3 benchmark) and D2 dynamic per-hot-guard
  (need static-headroom evidence); delete-gc sub-table read-skip (need a workload
  where the transient pre-compaction read matters). Drive any of these from a real
  workload or measured regression, not speculatively.
