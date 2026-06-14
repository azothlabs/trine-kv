# Guard-Aware LSM Strategy

Date: 2026-06-14
Status: Phases A-F resolved (A-E implemented, F decided to keep guards derived)

## Purpose

This document records Trine's planned direction for key-space-aware reads and
non-uniform compaction.

The design combines two ideas:

- key-space guards that group table/run candidates by user-key range;
- per-level compaction policy that reduces overlap near the top while avoiding
  excessive rewrites near the bottom.

External systems and papers are references only. Trine must keep its own
terminology, storage contracts, file formats, tests, and recovery rules.

## Engineering Judgment

This is a strong direction for Trine, but it is structural engine work rather
than a narrow optimization.

It is attractive because it targets the same costs that recent benchmark work
has exposed:

- point reads and missing reads should avoid unrelated L0 or overlapping table
  candidates;
- `get_many` should group work by key-space before table/block grouping;
- metadata cache should stay useful when data blocks are much larger than RAM;
- compaction should rewrite less data when only a local key range needs
  cleanup;
- bottom levels should not be rewritten frequently only to chase small read
  improvements.

It is risky because it touches the core rules that protect MVCC visibility,
range tombstone coverage, scan ordering, manifest recovery, and compaction
retention. Therefore the first implementation must be an in-memory planning
and read-pruning layer derived from existing table metadata. It must not change
the manifest, WAL, SSTable format, public API, or recovery contract.

## Terms

`Guard` means an internal key-space range used to route point reads, batched
reads, scans, and compaction planning to a smaller table/run candidate set.

`Run` means one table or table group that participates as a read candidate
inside a level.

`Overlap depth` means how many runs in one level may contain the same user key
or overlap the same key range.

`Upper levels` means L0 and shallow levels where read cost and overlap are
most visible.

`Lower levels` means the largest levels where rewrite cost dominates.

## Current Baseline

The current LSM layout already has useful structure:

- L0 may contain overlapping tables and point reads may probe multiple L0
  tables.
- L1 and deeper levels are validated as non-overlapping by table key bounds,
  so point lookup normally selects at most one table per non-overlapping level.
- Table-level and block-level Bloom filters already prevent data-block reads
  for in-bounds missing keys.
- Metadata/data-block cache policy already gives index/filter/range-tombstone
  metadata higher priority than data/blob blocks.

This means guard-aware work should first target:

- L0 and any overlapping run sets;
- batched point reads that can be grouped by guard before table/block grouping;
- compaction input selection and output boundaries;
- diagnostics that prove fewer candidates are considered before data-block
  reads.

## Design Principles

- Measure first. Add candidate-count and rewrite-cost diagnostics before
  retaining behavior changes.
- Keep the first guard index in memory and derive it from existing table key
  bounds.
- Do not change storage format until in-memory guard pruning proves value.
- Reads must remain MVCC-correct for snapshots, point deletes, range deletes,
  and overlapping L0 tables.
- Range and prefix scans must preserve ordering and visibility.
- Compaction must keep retention rules based on the oldest active snapshot.
- Top levels may reduce overlap more aggressively.
- Bottom levels should prefer lazy or tiered behavior unless space, tombstones,
  blobs, or high overlap make rewrite work worthwhile.
- User-facing names should remain Trine names. Reference ideas must not leak
  into public API or file-format naming.

## Phase Plan

### Phase A: Guard Diagnostics

Goal: make read and compaction candidate waste visible before changing
behavior.

Implementation scope:

- Add diagnostics for point reads:
  - candidate tables by level;
  - L0 candidate count;
  - overlapping run count;
  - table probes;
  - filter skips;
  - block metadata probes;
  - data block reads;
  - cache hits/misses.
- Add diagnostics for `get_many`:
  - input keys;
  - unique keys;
  - guard groups;
  - table groups;
  - data block groups.
- Add compaction diagnostics:
  - input tables by level;
  - overlap range width;
  - input bytes;
  - output bytes;
  - rewritten bytes by level;
  - candidate keys/ranges that caused the compaction.

Acceptance gate:

- Bench output can distinguish candidate pruning from filter pruning and
  data-block avoidance.
- No read, write, compaction, or storage behavior changes.

### Phase B: In-Memory Guard Index For Reads

Goal: use an in-memory guard index to reduce point-read and batch-read
candidate sets.

Implementation scope:

- Build guard metadata from existing `LsmVersion` table bounds.
- Start with L0 and overlapping candidate sets.
- Preserve current direct table lookup for non-overlapping L1+ levels unless
  evidence shows a guard index helps.
- Route point reads:

```text
key -> guard -> candidate tables by level -> filter/index/block path
```

- Route `get_many`:

```text
keys -> guard groups -> table groups -> block groups
```

Acceptance gate:

- L0 table probes drop for point reads and missing reads when unrelated L0
  tables exist.
- `get_many` keeps existing ordering and duplicate semantics.
- Existing MVCC, range-delete, persistent, transaction, and table tests pass.
- Bench counters show fewer candidate probes without increasing data-block
  reads.

### Phase C: Guard-Aware Scan And Range Delete Safety

Goal: make guard pruning safe for operations whose input is a key range rather
than one key.

Implementation scope:

- Map range/prefix scans to guard ranges.
- Ensure range tombstone candidate selection covers every tombstone that may
  hide a returned key.
- Keep scan merge ordering identical to the current lazy iterator behavior.
- Keep transaction conflict checks conservative when guard bounds are ambiguous.

Acceptance gate:

- Range, prefix, reverse, snapshot, and range-delete tests pass.
- Diagnostics show scan candidate selection without missing any visible or
  hidden record.

### Phase D: Guard-Aware Compaction Picker

Goal: use guards to narrow compaction inputs and reduce rewrite cost.

Implementation scope:

- Select compaction candidates by guard/range instead of broad level ranges
  when safe.
- Keep overlap closure for L0 and for any lower-level tables that overlap the
  selected range.
- Preserve table output splitting at user-key boundaries.
- Keep manifest publish and snapshot-delayed cleanup unchanged.

Acceptance gate:

- Compaction input bytes and output bytes drop for local overlap cleanup
  workloads.
- Point-read candidate depth improves or stays flat.
- Blob reachability, tombstone retention, and recovery tests pass.

### Phase E: Non-Uniform Per-Level Compaction

Goal: move from one compaction style to per-level policy.

Implementation scope:

- Upper levels use tighter overlap budgets to reduce read candidates.
- Middle levels use guard-local compaction.
- Lower levels use lazy or tiered behavior unless a trigger justifies rewrite:
  - space amplification;
  - obsolete tombstones;
  - blob garbage collection;
  - extreme overlap depth;
  - read-path candidate explosion.
- Add policy stats that explain why a compaction ran or did not run.

Acceptance gate:

- Write amplification improves for lower-level workloads.
- Read amplification does not regress beyond the configured overlap budget.
- Space use and obsolete data are reported and bounded.
- Full local gate and grouped benchmark evidence pass.

### Phase F: Persisted Guard Metadata Decision

Goal: decide whether guard information should remain derived at open or become
persisted metadata.

Implementation scope:

- Compare recovery/open cost of deriving guards from table bounds against the
  runtime benefit.
- If persisted guard metadata is needed, update the protocol before changing
  manifest or SSTable formats.

Acceptance gate:

- No format change occurs without protocol update and migration/recovery tests.

#### Phase F Decision (2026-06-15): Keep Guards Derived

Status: Resolved. No format change.

Decision: guards remain derived in memory from existing table key bounds. Trine
does not add a separate persisted guard-metadata structure.

Evidence:

- The manifest table record already persists each table's `smallest_user_key`
  and `largest_user_key` (`src/manifest.rs` encode/decode). Guard bounds are
  therefore already implicitly durable. On open, `manifest.tables()` yields the
  table properties and `LsmVersion::new` only sorts them into the level layout
  (L0 read-order, L1+ by key range). There is no separate guard derivation pass
  and no extra I/O performed for guards.
- Cold open wall time is dominated by manifest/table metadata I/O and WAL
  replay, not by guard work: benchmark cold table open ~6.7 ms writable /
  ~2.0 ms read-only, WAL replay open well under 1 ms. The `LsmVersion` level
  sort is `O(tables * log)` over data already read for table open and is not a
  measured bottleneck.

Rationale: a separate persisted guard structure would duplicate the table
bounds the manifest already stores while adding format-version, migration, and
recovery-validation burden. The Phase F entry condition (deriving guards became
a measured open/recovery cost) is not met.

Future re-open condition: revisit only if the `LsmVersion` build/sort is later
measured to dominate open for very large table counts. The first response would
be caching a pre-sorted level layout inside the existing manifest, which is a
manifest optimization rather than a new guard format, and would still require a
protocol update plus migration/recovery tests before any format change.

## Non-Goals

- Do not implement another storage engine inside Trine.
- Do not rename public APIs around external paper names.
- Do not change public read/write semantics.
- Do not weaken durability or manifest publish rules.
- Do not change table, WAL, or manifest formats during the first guard-index
  read-pruning phases.
- Do not increase compaction globally just to hide read-path candidate cost.

## Required Verification

Each implementation slice must run focused verification for the touched area
and record evidence before continuing.

Minimum gates by area:

- Read pruning:
  - focused point-read, `get_many`, missing-read, range-delete, and MVCC tests;
  - targeted benchmark diagnostics for table probes, filter skips, block
    metadata probes, data block reads, cache hits/misses, and storage reads.
- Scan pruning:
  - range/prefix/reverse/snapshot/range-delete tests;
  - iterator laziness tests.
- Compaction:
  - persistent compaction-level tests;
  - blob reachability tests when blob tables participate;
  - recovery/reopen tests;
  - rewrite-byte and candidate-depth benchmarks.
- Policy changes:
  - grouped benchmark evidence for read, write, compaction, and space behavior;
  - protocol update if any stable storage or recovery behavior changes.

## First Implementation Slice

Start with diagnostics only:

```text
task823 [ ] goal:add guard candidate diagnostics without behavior change | scope:src/lsm/version.rs src/lsm/read.rs benches/v1_bench.rs | verify:cargo test -q get_many_sync + TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
task824 [ ] goal:record L0/overlap candidate depth for point/missing/get_many | scope:src/stats.rs src/db.rs benches/v1_bench.rs | verify:targeted diagnostics show candidate depth separate from filter/data-block counters
```

Only after those diagnostics identify avoidable candidate work should Trine
add an in-memory guard index to the read path.
