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

Goal: add `CompactionTrigger::TombstoneDebt` to the guard-aware picker, scored by
tombstone density / scan waste / oldest-tombstone age, compacting the offending
range down to where deletes can actually drop (covered data is met / bottom
level), using Phase 1 metrics plus per-guard tombstone inventory.

### Phase 3: Bulk Drop Via File Drop

Goal: drop bucket / prefix ranges by removing wholly-covered SSTable/blob files
(DeleteFilesInRange-style) with snapshot/version-handle safety, aligned to
guard/level boundaries; only partially covered edge tables write or truncate a
small tombstone. Turns drop-table/tenant/prefix into a metadata + background
cleanup operation instead of read-path tombstone pollution.

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
