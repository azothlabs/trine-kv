# Current Phase

## Status

Complete

## Goal

Phase 1 of `.phrase/protocol/delete-gc-lifecycle.md`: make delete debt and
snapshot version-debt measurable with zero behavior or storage-format change.
Measure-first foundation for the tombstone/GC work.

## Design Assessment

The external playbook for snapshot/tombstone is server/DBaaS-shaped (snapshot TTL
with forced interruption, scan replicas, delete SLA, tenant/generation key
encoding). For an embedded single-process library those do not apply; what does
is observability + a tombstone-aware compaction trigger + bulk drop as file drop.
Phase 1 builds only the observability, mirroring the existing `blob_reads` metric
plumbing so counting is lock-light and off the hot allocation path.

## Scope

- `ScanWasteMetrics` threaded into the lazy scan; counted at the merge
  chokepoint where a user-key version group resolves to visible / delete-hidden /
  no-visible-version.
- `DbStats`: `scan_internal_records`, `scan_user_keys`,
  `scan_tombstone_hidden_keys` (north star = internal / user), plus
  `oldest_snapshot_seq` and `oldest_snapshot_lag` (visible minus oldest pinned).
- `active_snapshots` retained.

## Out Of Scope

- Snapshot/iterator lease, TTL, or forced interruption (RAII + observability).
- Tombstone-aware compaction trigger (Phase 2), bulk file drop (Phase 3),
  read-path whole-range skip (Phase 4).
- Any storage-format change.

## Acceptance Gate

- `DbStats` exposes scan read-amplification inputs and snapshot version-debt. Met.
- A test over tombstone/version-heavy data shows internal > user and
  tombstone-hidden > 0. Met.
- No behavior/format change. Met. Full local gate. Met (one pre-existing
  background-timing flake).

## Evidence

- `scan_waste_and_snapshot_lag_metrics_report_gc_health`: 3 versions x 4 keys + 2
  point deletes; full scan returns 2 live keys, `scan_user_keys == 2`,
  `scan_internal_records == 14 > 2`, `scan_tombstone_hidden_keys == 2`; holding a
  snapshot makes `oldest_snapshot_lag > 0` and `active_snapshots == 1`, dropping
  it returns lag to 0.
- `cargo test --lib` (359) and `--all-features` (363) green; fmt/clippy/diff clean.

## Known Risks

- `scan_internal_records` counts the per-user-key merge fan-out (full group),
  which captures version bloat regardless of early-return; it is a since-open
  cumulative counter, so consumers should diff across two `stats()` calls for a
  per-scan ratio.

## Next Recommendation

- Phase 2 (`TombstoneDebt` compaction trigger) is the natural next step, driven by
  these metrics plus a per-guard tombstone inventory. Then Phase 3 (bulk drop via
  file drop) for the biggest user-facing win, and Phase 4 (read-path skip).
