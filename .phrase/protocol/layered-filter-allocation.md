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

Goal: build table filters with a depth-scaled `bits_per_key` at write time.

Implementation scope:

- Choose `bits_per_key` from the output level during flush/compaction using an
  internal heuristic curve (local classic-Monkey direction: shallower levels
  get more bits).
- Keep the filter block self-describing; read path and formats unchanged.

Acceptance gate:

- At equal or lower total filter memory, `sum f_i` and negative-lookup
  data-block reads drop versus the uniform baseline, proven by Phase 1 stats.
- Hit-path behavior and point-read candidate depth do not regress.

### Phase 3: Prefix Filter Negative-Lookup Tuning

Goal: tune the independent prefix filter budget for prefix-heavy negative
lookups.

### Phase 4: Configurable Curve (Only If Needed)

Goal: if evidence requires user-tunable curves, add the option, which touches
`BucketOptions` and therefore needs a manifest version bump plus
migration/recovery tests before any format change.

### Phase 5: Deferred Advanced Variants

- Cost-weighted curve for remote backends (D1 remote branch).
- Dynamic per-hot-guard filter rewrite (D2 dynamic branch). Likely not done
  unless static phases leave clear headroom.

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
