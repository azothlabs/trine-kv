# Delete / GC Lifecycle (Tombstone And Snapshot Health)

Date: 2026-06-15
Status: Draft for phased implementation

## Purpose

Keep range scans fast as deletes and snapshots accumulate, by making delete
debt and snapshot version-debt measurable, then compacting and dropping them on
purpose instead of letting them pile up on the read path.

External systems (RocksDB DeleteRange, CompactOnDeletionCollector) are design
references only. Trine keeps its own terminology, formats, tests, and contracts.

## Engineering Judgment (embedded-calibrated)

A common external playbook for this problem is written for a server / DBaaS:
snapshot TTL with forced interruption, follower/scan replicas, a delete SLA
service, and `tenant|generation|user_key` key encoding. Trine is an embedded,
single-process, async-first library, so those parts do not apply:

- An embedded library must not forcibly kill a caller's snapshot or iterator;
  that breaks the read-consistency contract. Snapshots/iterators are RAII guards
  (drop releases). Snapshot governance is observability + docs + optional
  warnings, plus the existing `keep_last_read_versions` config bound - not a
  lease/eviction system.
- There are no followers or scan replicas in a single process. A long scan uses
  a pinned read version (already available); cost is bounded by compaction
  retention.
- Generation key encoding is an application schema concern. The engine should
  provide the mechanism (drop files by range/prefix), not bake tenant/generation
  into the key format.

What does apply, and is high value:

- Observe read-path GC waste, especially internal-entries-scanned per
  user-key-returned (the north-star ratio) and snapshot version-debt.
- A tombstone-aware compaction trigger on top of the existing guard-aware picker.
- Bulk drop (drop bucket / prefix) as file drop, not a giant range tombstone.

Trine already has: range-tombstone ordered index + coverage rules + randomized
tests; output-file-boundary tombstone truncation (clean cut); compaction
retention by oldest active snapshot with safe delete dropping;
`keep_last_read_versions`; the guard-aware non-uniform picker with
trigger/skip hooks; `active_snapshots` count.

## North-Star Metric

```text
scan_read_amplification = internal_records_merged / user_keys_returned
```

High ratio means range scans are wading through obsolete versions and
tombstones rather than returning live data.

## Phase Plan

### Phase 1: Delete/Snapshot Health Observability

Goal: make delete debt and snapshot version-debt measurable with zero behavior
or storage-format change. Measure-first foundation for Phases 2-4.

Implementation scope:

- Scan-waste counters (range + prefix lazy scans), recorded at the merge
  chokepoint where a user-key version group resolves to a visible row, a
  delete-hidden row, or nothing:
  - `scan_internal_records` (versions merged across sources per user-key group);
  - `scan_user_keys` (visible rows returned);
  - `scan_tombstone_hidden_keys` (groups hidden by a point or range delete).
- Snapshot version-debt in `DbStats`:
  - `oldest_snapshot_seq` (oldest active read sequence still pinned);
  - `oldest_snapshot_lag` (visible sequence minus oldest pinned sequence);
  - existing `active_snapshots` retained.
- Mirror the existing `blob_reads` metrics plumbing (`Arc<...>` threaded into the
  lazy scan), so counting is lock-light and off the per-key hot allocation path.

Acceptance gate:

- `DbStats` exposes scan read-amplification inputs and snapshot version-debt.
- A test over tombstone/version-heavy data shows `scan_internal_records >
  scan_user_keys` and `scan_tombstone_hidden_keys > 0`.
- No read/write/compaction behavior change and no storage-format change.

### Phase 2: Tombstone-Aware Compaction Trigger

Status: Implemented (2026-06-15), range-tombstone scope.

Goal: add `CompactionTrigger::TombstoneDebt` to the guard-aware picker so range
tombstones meet and drop the data they cover instead of lingering on the read
path.

Decision: v1 uses the cheap, format-free `Table::may_have_range_tombstones`
footer flag (no manifest change). Point-tombstone density (`tombstone_entries /
total_entries`) needs per-table entry/deletion counts that are not in
`TableProperties`; persisting those is a manifest bump deferred to a later slice
if Phase 1 scan-waste shows point-tombstone-heavy pressure. Range tombstones are
the dangerous big-DeleteRange / drop-prefix case and are the high-leverage start.

Implementation:

- `CompactionTable` carries `has_range_tombstones` (from the table flag).
- The picker, after `L0Overlap`/`LevelSize` and before the no-pressure spread,
  fires `TombstoneDebt` on the shallowest non-level-0, non-deepest level holding
  an in-range range-tombstone table, compacting it down with overlapping
  lower-level data.
- Termination/anti-storm: it only fires when there is lower-level overlap (a pure
  move just relocates the tombstone), and the deepest populated level is
  excluded, so a tombstone migrates down at most to where its covered data lives,
  then stops; it is also lower priority than size pressure.

Acceptance gate:

- A range-tombstone table with lower-level overlap plans `TombstoneDebt`; one
  without overlap, or at the deepest level, does not (planner + `LsmTree` tests).
- No storage-format change. Compaction/range-delete/recovery suites pass.

### Phase 3: Bulk Drop Via File Drop

Status: Implemented (2026-06-15), compaction file-drop optimization.

Design correction (from investigating the snapshot model): Trine snapshots do
NOT pin a version handle; point reads use the current version filtered by read
sequence (`SnapshotTracker` is a per-sequence refcount). So an instant
"remove files now" API would let an older snapshot read the emptied current
version and miss its data - an MVCC violation. The MVCC-correct bulk delete is
already a range tombstone (`delete_range`, any range, layout-agnostic); file drop
is only safe as a retention-gated optimization, exactly like RocksDB
DeleteFilesInRange ("use when no snapshot needs the range").

Decision: keep it general and layout-agnostic (do not assume tenant==bucket).
The mechanism is a compaction optimization that applies to any `delete_range`:
during compaction, an input table entirely covered by one range tombstone is
dropped by file (via `input_table_ids` -> obsolete -> snapshot-safe cleanup)
instead of being read and rewritten to empty, when all three hold:

- the tombstone spatially covers the whole table key span;
- the table's largest sequence <= the tombstone sequence (all records hidden);
- the tombstone sequence <= `oldest_active_snapshot` (no retained reader needs it).

Partially covered edge tables are merged/truncated normally; the range tombstone
keeps correctness meanwhile. When everything in a drop range is covered and
retention-safe (the common embedded case: no older snapshot), the whole set of
data tables is unlinked with no rewrite.

`truncate_bucket` / `drop_range` ergonomic wrappers are deferred sugar; the
engine mechanism above is the value and needs no key-layout assumption.

Acceptance gate:

- Pure coverage decision unit-tested (covered / too-new / snapshot-too-old /
  partial-spatial). A bulk `delete_range(all)` drops covered data tables by file:
  data gone, durable across reopen, table bytes not grown by a rewrite.
- No storage-format change; range-delete + snapshot correctness suites pass
  (the retention gate matches the merge's own retention, so older snapshots that
  still need covered data keep it).

### Phase 4: Read-Path Whole-Range Skip

Goal: skip blocks/ranges fully covered by visible tombstones during scans
(tombstone skyline), and harden tombstone truncation. Gated by Phase 1
scan-waste evidence showing it is worth it.

## Non-Goals

- No snapshot/iterator lease, TTL, or forced interruption (RAII + observability).
- No follower / scan replica / backup replica (single process).
- No tenant/generation key encoding in the engine (application schema concern).
- No storage-format change in Phase 1.

## Required Verification

- Phase 1: scan-waste counters reconcile (user keys returned match the scan
  output count); snapshot version-debt reflects active pins; focused tests over
  tombstone/version-heavy data; no format change.
- Later phases: compaction/recovery/blob tests for behavior changes; protocol
  update plus migration/recovery tests for any format change.

## First Implementation Slice

```text
task860 [ ] goal:delete/snapshot health observability | scope:src/stats.rs src/iterator.rs src/db.rs | verify:scan read-amp + snapshot version-debt tests + full gate
```
