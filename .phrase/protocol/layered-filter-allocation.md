# Layered Filter Allocation (Monkey-Style)

Date: 2026-06-15
Status: Draft for phased implementation

## Purpose

Spend a fixed Bloom/filter memory budget non-uniformly across the LSM so that
worst-case point lookup and negative-lookup cost drop without raising total
filter memory. This is the Monkey idea adapted to Trine's guard-aware,
versioned, level-layout engine.

External systems and papers (Monkey, RocksDB) are design references only. Trine
keeps its own terminology, storage contracts, file formats, tests, and recovery
rules.

## Relationship To Guard-Aware Strategy

This is orthogonal to and composes with `guard-aware-lsm-strategy.md`:

- Routing (guards + non-overlapping L1+) lowers the candidate-run count per
  lookup.
- Layered filter allocation lowers the false-positive probability of each
  candidate run under a fixed memory budget.

Target shape of one point lookup:

```text
key -> guard -> very few candidate runs -> Monkey-tuned filters exclude most
     -> read 0..1 data blocks
```

## Cost Model

Trine already validates L1+ levels as non-overlapping (each level contributes at
most one candidate to a point lookup), so the classic Monkey approximation holds
closely:

```text
read_amp(zero-result / per-level miss) ~= sum over levels i of f_i
```

where `f_i` is the false-positive rate of level `i`'s filter. The guard-aware
generalization, when a level holds several candidate runs, is:

```text
read_amp ~= sum over levels i ( sum over candidate runs r in guard(k) of f(i, r) )
```

Filter memory for a run is roughly `bits_per_key * key_count(run)`. Deeper
levels hold exponentially more keys, so holding their `f` low is the most
expensive memory; shifting that memory to smaller, shallower runs lowers their
`f` exponentially, reducing the total sum.

## Key Design Decisions

- D1 (curve direction depends on backend access cost):
  - Local/SSD backend (current default): classic Monkey. Lower `bits_per_key`
    (higher `f`) for deep/large levels, higher `bits_per_key` (lower `f`) for
    shallow/small levels. Minimize `sum f_i`.
  - Remote/cold backend (for example the `s3` feature) where per-level access
    cost `cost_i` differs: cost-weighted Monkey, minimize `sum f_i * cost_i`,
    which can require giving deep levels more budget. This is a different curve
    and must not be conflated with the local default.
- D2 (static per-level first, dynamic per-guard later):
  - Static per-output-level `bits_per_key` is a write-time decision: flush and
    compaction choose `bits_per_key` from the output level when building the
    table filter. The filter block is self-describing, so the read path is
    unchanged and no storage format changes.
  - Dynamic per-hot-guard allocation requires rewriting filters at compaction
    time driven by per-guard access statistics. It is a feedback system (extra
    stats, churn, compaction coupling) and is deferred until static evidence
    shows remaining headroom.
- D3 (filters only help negatives): a filter saves I/O only when the key is
  absent from a run. Hits must eventually read the data block. Monkey gains are
  concentrated in negative-lookup / point-miss / existence-check workloads, and
  most hits land in deep levels (most data), which reinforces giving deep levels
  higher `f`.
- D4 (measure before tuning): no `bits_per_key` curve is retained without
  per-level false-positive evidence proving `sum f_i` dropped at equal or lower
  filter memory.

## Phase Plan

### Phase 1: Per-Level Filter Observability

Goal: make each level's observed false-positive behavior measurable before any
allocation change.

Implementation scope:

- Aggregate the existing per-table filter counters by level when building
  `DbStats` (zero hot-path change; tables already carry their level and filter
  counters).
- Expose per-level point/prefix filter hits, misses, and false positives.
- Add a benchmark diagnostic that builds a multi-level layout plus a
  negative-lookup workload and reports each level's observed false-positive
  rate.

Acceptance gate:

- `DbStats` exposes per-level filter counters distinct from the global totals.
- The diagnostic reports per-level `f_i` (false positives over filter-allowed
  absent probes).
- No read/write/compaction behavior change and no storage format change.

### Phase 2: Per-Output-Level bits/key Curve

Status: Implemented (2026-06-15).

Goal: build the point filter with a depth-scaled `bits_per_key` at write time so
deep levels cost less filter memory.

Refinement from Phase 1 evidence: Trine's two-level (table + block) filter
already drives negative-lookup data-block reads to zero, so the realizable goal
is **reducing filter memory**, not reducing I/O. The implemented curve is
therefore memory-first rather than equal-budget reallocation: it only lowers
deep levels (never raises any level above the base), so total filter memory can
only drop. The trade is a higher deep-level false-positive rate, bounded by the
floor; deep negatives may incur some extra block-filter / data-block probes
while hot shallow levels (L0/L1) stay fully accurate.

Implementation:

- `level_adjusted_point_bits_per_key(base, level)` in `src/table.rs`: L0/L1
  (pinned levels) keep the base; each deeper level drops by `BITS_PER_LEVEL_STEP`
  (2), clamped to `MIN_BITS_PER_KEY` (4) and never above the base.
- Applied to the block-level point filter (the cross-level lever, present at all
  levels) in `build_data_blocks`, and to the shallow table-level filter.
- The filter block is self-describing, so the read path and all storage formats
  are unchanged.

Acceptance gate:

- Deep levels write smaller filters at equal records, proven residency-
  independently by encoded table size (`deeper_levels_write_smaller_block_filters`).
- The curve never exceeds the base, so total filter memory cannot regress.
- Hot shallow levels (L0/L1) keep base accuracy: their FPR and negative-lookup
  data-block reads are unchanged (diagnostic: L0/L1 FPR ~1%, data-block reads 0).
- `DbStats::level_filters[*].filter_resident_bytes` reports resident filter
  memory per level.

### Phase 3: Prefix Filter Negative-Lookup Tuning

Status: Implemented (2026-06-15).

Goal: give the prefix filter the same depth-scaled budget as the point filter so
deep-level prefix filters cost less memory for prefix-heavy workloads.

Implementation:

- Generalized the Phase 2 curve into `level_adjusted_filter_bits(base, level)`
  and applied it to the prefix filter `bits_per_prefix` in `build_prefix_filter`
  (block-level, all levels) and the shallow table-level prefix filter, mirroring
  the point filter.
- Prefix filter is self-describing (`PrefixFilter::from_parts`), so no
  storage-format change. Per-level prefix false positives are already observable
  via `DbStats::level_filters[*].filters.table_prefix_*`, and prefix filter bytes
  are included in `filter_resident_bytes`.

Acceptance gate:

- Deep levels write smaller prefix filters at equal records, proven residency-
  independently by encoded table size (`deeper_levels_write_smaller_prefix_filters`).
- Curve never exceeds the base (shared curve unit test).
- Hot shallow levels keep base accuracy.

### Phase 4: Configurable Curve

Status: Implemented (2026-06-15).

Goal: let users tune (or disable) the per-level filter bits curve per bucket.

Decision (designer): the curve lives in `BucketOptions` and is persisted, not a
runtime `DbOptions` knob. Rationale: it is a filter property and the base
`bits_per_key`/`bits_per_prefix` are already per-bucket persisted there
(cohesion); the curve shapes durable SSTable filter sizing, so a non-persisted
knob would let the curve silently drift across restarts and leave a dataset of
tables written under different curves (worse than the ephemeral WAL-lane count,
which is why `WalShardPolicy` could be runtime); per-bucket granularity is free
once it is in `BucketOptions`; and the manifest already has a clean
version-gated decode (`read_bucket_options(version)`), so the format change is a
routine, tested pattern.

Implementation:

- `BucketOptions::filter_depth_curve: FilterDepthCurve` (`Auto` | `Uniform` |
  `Custom { step, floor }`), default `Auto`; `with_filter_depth_curve` builder.
  The shallow boundary stays tied to the pinned-metadata levels (not exposed).
- `level_adjusted_filter_bits(curve, base, level)` applies it to both point and
  prefix filters.
- Manifest bumped to v10; `filter_depth_curve` appended to bucket options.
  Versions < 10 decode to `Auto` (`read_bucket_options` version gate).

Acceptance gate:

- No format change without a protocol update and migration/recovery tests. Met:
  v9 payload decodes to `Auto`; v10 round-trips a `Custom` curve; every
  persistent test now round-trips a v10 manifest.
- The curve is configurable and disablable (`Uniform`).

### Phase 5: Advanced Variants

- **D1 remote branch (opt-in capability shipped 2026-06-15)**: the cost-weighted
  (ascending) curve `FilterDepthCurve::CostWeighted { step, ceil }`. Deeper levels
  *gain* `step` bits up to `ceil` while pinned shallow levels keep the base -
  the inverse of classic Monkey, for remote/cold backends (the `s3` feature)
  where a deep-level filter miss costs a network round-trip, not a cheap local
  read. Persisted via manifest curve tag 3 (no version bump; default unchanged).
  It is **opt-in only and the default is not flipped**: the benefit is the remote
  read-cost asymmetry, which cannot be validated on local SSD (locally it only
  raises memory). Auto-selecting it for `s3`, and tuning `step`/`ceil`, is gated
  on an actual s3 benchmark - left until one exists. The capability is shipped so
  a remote user can choose it now.
- **D2 dynamic branch (still deferred)**: dynamic per-hot-guard filter rewrite at
  compaction. A feedback system; deferred until static evidence shows clear
  remaining headroom. Not started.

## Non-Goals

- Do not change table/manifest/blob formats during the static phases.
- Do not implement another storage engine or adopt external paper naming in
  public API or file formats.
- Do not raise total filter memory to chase lookup cost; the budget is fixed.
- Do not start a dynamic feedback filter system before static per-level
  allocation proves and exhausts its measured benefit.

## Required Verification

- Read pruning unaffected: point, missing, prefix, range-delete, MVCC tests.
- Filter accounting: per-level filter stats tests; false positives counted only
  after a filter-allowed candidate yields no matching key.
- Allocation changes: grouped benchmark evidence for `sum f_i`, negative-lookup
  data-block reads, and filter memory before/after.
- Any format or option change: protocol update plus migration/recovery tests.

## First Implementation Slice

```text
task840 [ ] goal:per-level filter observability | scope:src/stats.rs src/db.rs src/lib.rs benches/v1_bench.rs | verify:per-level filter stats test + layered-filter FPR diagnostic + full gate
```

Only after Phase 1 shows where false positives actually concentrate should Trine
change the `bits_per_key` allocation.
