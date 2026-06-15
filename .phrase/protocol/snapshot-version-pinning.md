# Snapshot Version-Handle Pinning (LMDB-grade reader pins)

Date: 2026-06-15
Status: Phase 1 done and merged. Phases 2-3 DEFERRED (low ROI for embedded;
re-open only when a real workload needs instant truncate/file-drop while an
older snapshot is concurrently held).

## Deferral decision (2026-06-15)

After implementing Phase 1 and scoping Phases 2-3 against the code, the verdict
is to stop here:

- Phase 2 adds ~zero read-correctness benefit (the current
  current-version + seq-filter model is already repeatable-read correct).
- The only unique benefit of Phases 2-3 is instant truncate/file-drop while an
  *older* snapshot is concurrently held — rare in a single-process embedded DB,
  and the common case (no older snapshot) is already instant via the
  retention-gated file-drop in [[delete-gc-lifecycle]] Phase 3.
- The cost is high and indivisible: `db.snapshot()` would go from a free
  refcount bump to an O(buckets) super-version capture pinned for the snapshot's
  whole life, plus a rewrite of the hottest read path (~30 entry points) at the
  most MVCC-sensitive point. Capture + read-reroute must land together to have
  any value, so there is no small safe slice.
- LMDB's reader-pins-root is intrinsic to its COW B+tree; bolting it onto an LSM
  is changing the concurrency model, not adding a feature.

Phase 1 already delivered the practical win (a long-lived snapshot no longer
stalls all space reclamation). Phases 2-3 below are kept as a named, deliberately
deferred future phase.

## Purpose

Move from the coarse "any active snapshot blocks all obsolete-file cleanup"
model toward a reader-pins-the-tables-it-needs model, so that:

1. a single long-lived snapshot no longer stalls all space reclamation; and
2. (later) instant file-drop / truncate is MVCC-safe even while an older
   snapshot is held, because the old snapshot reads its pinned old tables.

This is the recurring "approach LMDB" direction recorded in
[[delete-gc-lifecycle]] (its deferred larger initiative) and in memory. LMDB's
COW B+tree lets a reader pin a root and frees pages when the pin drops; the LSM
analogue is: an obsolete table file is freed exactly when no live version
handle (current version, in-flight iterator, or pinned snapshot) still
references its `Arc<Table>`.

## Model (as found in code, 2026-06-15)

- `LsmTree.current_version: RwLock<Arc<LsmVersion>>`; `LsmVersion` holds
  `Vec<Arc<Table>>`. Compaction installs a new version via
  `with_replaced_tables`; the old `Arc<LsmVersion>` (and its `Arc<Table>`) lives
  on only while some handle holds it.
- Lazy iterators already pin `Arc<LsmVersion>` for their lifetime (a per-read
  super-version: version + memtable sources). Point reads build a transient
  `LsmPointReadSnapshot` (version + memtables) per call.
- `Snapshot` (repeatable-read handle) pins ONLY a `read_sequence` (a refcount in
  `SnapshotTracker`); each read re-derives the *current* version filtered by
  that sequence. So snapshots are "current-version + seq-filter", not a pinned
  version.
- Obsolete table files: compaction queues removed ids into
  `pending_obsolete_table_ids` and `cleanup_pending_obsolete_table_files`
  deletes them **only when `snapshots.active_count() == 0`** — a global, coarse
  gate. Object-store mode uses orphan GC instead.
- `Arc<Table>` is held nowhere durable except versions (manifest stores only
  `TableProperties`). Once a table leaves the current version, no new acquirer
  can appear, so `Arc::strong_count` is monotonically non-increasing → a
  `strong_count == 1` (queue is sole owner) check is TOCTOU-safe.

## Engineering Judgment (calibration)

Full LMDB-style per-snapshot version pinning is large and, for normal
compaction, unnecessary: a snapshot at an old sequence that reads the *current*
version still sees its data, because compaction rewrites retained data into the
output table (retention is gated by `oldest_active_snapshot`, which already
includes the snapshot). The only case where reading the current version would
miss data is **file-drop without rewrite** (delete-gc Phase 3 / truncate), and
that path already gates on `tombstone.seq <= oldest_active_snapshot`.

So the high-value, low-risk core is **liveness-gated cleanup** (Phase 1):
replace the global `active_count()` gate with per-table `Arc` liveness. That
alone fixes "one long-lived snapshot stalls all cleanup" and is correct for all
existing paths. True snapshot version-handle pinning (Phase 2) is only needed to
unlock instant truncate-with-old-snapshot, and is built on top.

## Phase Plan

### Phase 1: Liveness-gated obsolete table cleanup

Goal: delete an obsolete table file as soon as no live version handle
references it, instead of waiting for zero active snapshots.

Scope:

- Pending obsolete queue holds `Arc<Table>` handles (not just ids).
- `install_compaction` / `with_replaced_tables` surface the removed
  `Arc<Table>` so the retire path can queue the real handles.
- Cleanup (sync + async native, browser) deletes a file iff
  `Arc::strong_count(&table) == 1` (queue is sole owner); pinned tables stay
  queued and are retried on the next maintenance pass. Remove the coarse
  `active_count() != 0` gate.
- No lock held across `.await`: lock queue, take the deletable handles, drop the
  lock, then delete files, then drop the handles.

Acceptance gate:

- An in-flight lazy iterator that pinned the pre-compaction version keeps the
  obsolete file on disk until it is dropped; after drop, the next maintenance
  pass deletes it.
- A held point snapshot no longer blocks deletion of normally-compacted
  obsolete tables (they are gone while the snapshot is still open), and reads
  through that snapshot remain correct (they read the current version).
- Full local gate; recovery/compaction/range-delete suites pass; no
  storage-format change.

### Phase 2: Snapshot pins a version handle

Goal: make `Snapshot` capture a consistent read super-version (version +
memtable sources) at creation (lazily per bucket on first touch) and read
through it, so reads are truly repeatable against a pinned table set and an old
snapshot keeps its tables alive via Phase 1 liveness.

Scope (planned):

- `Snapshot` holds an optional pinned per-bucket handle; first read of a bucket
  captures and caches `LsmPointReadSnapshot`-style sources; reads route through
  the pinned handle instead of `current_version()`.
- Must pin version + memtable sources together (the flush window: data moved
  from a memtable to a new table not in an older version must not be missed).
- Bucket lifecycle: a bucket dropped after the snapshot was taken keeps its
  pinned tables alive for that snapshot.

Acceptance gate (planned):

- Repeatable-read tests: a snapshot taken before writes/compaction/flush
  returns exactly its creation-time view; randomized concurrent
  write+compact+flush vs. long snapshot.
- File liveness: the snapshot pins exactly the tables it touched; unrelated
  obsolete tables are still reclaimed (Phase 1).

### Phase 3: Instant file-drop / truncate using the pin

Goal: with Phase 2 in place, `delete_range`-covered tables can be retired
immediately (queued obsolete) regardless of `oldest_active_snapshot`, because an
old snapshot reads its pinned old tables; Phase 1 liveness frees the files when
the last pin drops. Decouples the *file-drop* decision from
`tombstone.seq <= oldest_active_snapshot` (the merge-rewrite retention still
applies to data being rewritten). Optional `truncate_bucket` / `drop_range`
sugar can land here.

Acceptance gate (planned):

- Bulk `delete_range` with a concurrent older snapshot: snapshot still reads old
  data; files freed only after it drops; no MVCC violation under randomized
  interleavings.

## Non-Goals

- No snapshot lease / TTL / forced interruption (RAII + observability stays).
- No storage-format change in Phase 1.
- `Arc::strong_count` gating relies on "no new acquirer after leaving current
  version"; if a future durable table cache is added, the liveness check must
  account for it (documented invariant).

## First Implementation Slice

```text
task870 [ ] goal:liveness-gated obsolete cleanup | scope:src/lsm/version.rs src/lsm/compact.rs src/db.rs | verify:iterator-pins-file + snapshot-no-longer-blocks tests + full gate
```
