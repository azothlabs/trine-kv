# Current Phase

## Status

Active

## Goal

Start Phase 187: make guard-aware LSM read and compaction decisions measurable,
then use the evidence to implement the first in-memory read-pruning slice.

## Design Assessment

The guard-aware design is a good database-engine direction for Trine, provided
we treat it as structural LSM work rather than a quick cache tweak.

It fits Trine because current evidence already shows the next useful boundary:
point reads, missing reads, `get_many`, cache residency, and compaction rewrite
cost are now measurable. Guards can reduce candidate work before Bloom/index and
data-block access, while non-uniform compaction can keep upper levels read-aware
without forcing the largest levels to rewrite too often.

The design is also risky. It touches version layout, point reads, range scans,
range tombstones, transaction conflict checks, compaction input selection,
manifest recovery assumptions, and background maintenance. Therefore the first
implementation must be diagnostics and in-memory structures derived from
existing table key bounds. No manifest, WAL, SSTable, public API, or recovery
format change is allowed in the first slices.

## Scope

- Guard candidate diagnostics for point reads, missing reads, and `get_many`.
- L0 and overlapping-run candidate depth before any behavior change.
- In-memory guard index derived from existing `LsmVersion` table bounds after
  diagnostics prove avoidable candidate work.
- Guard-aware `get_many` grouping before table/block grouping.
- Compaction diagnostics for local rewrite cost, overlap depth, and per-level
  input/output bytes.
- Design and implementation must follow
  `.phrase/protocol/guard-aware-lsm-strategy.md`.

## Out Of Scope

- Persisted guard metadata.
- Manifest, WAL, SSTable, or table-format changes.
- Public API naming changes.
- Durability, recovery, writer lease, platform I/O, or release workflow changes.
- Global "more compaction" as a read optimization.
- Bottom-level rewrite policy changes before guard diagnostics exist.

## Acceptance Gate

- Diagnostics distinguish candidate pruning from Bloom/filter pruning and
  data-block avoidance.
- Candidate tables/runs are reported by level, including L0 overlap depth.
- `get_many` diagnostics report input keys, unique keys, guard groups, table
  groups, and data-block groups.
- Compaction diagnostics report input tables, overlap range width, input bytes,
  output bytes, rewritten bytes by level, and compaction trigger reason.
- First retained behavior change is in-memory only and preserves MVCC,
  range-delete, scan ordering, transaction conflict, manifest recovery, and
  storage formats.
- Focused tests and targeted benchmark evidence pass before each commit.

## Active Task Slice

```text
task823 [x] goal:add guard candidate diagnostics without behavior change | scope:src/lsm/version.rs src/lsm/read.rs src/stats.rs src/db.rs benches/v1_bench.rs | verify:cargo test -q get_many_sync + targeted benchmark diagnostics
task824 [ ] goal:record L0/overlap candidate depth for point/missing/get_many | scope:src/lsm/version.rs benches/v1_bench.rs | verify:diagnostics show candidate depth separate from filter/data-block counters
task825 [ ] goal:derive first in-memory guard index only if diagnostics prove avoidable L0/overlap probes | scope:src/lsm/version.rs src/lsm/read.rs | verify:point/missing/get_many counters improve without extra data-block reads
task826 [ ] goal:add compaction rewrite-depth diagnostics before policy changes | scope:src/lsm/compact.rs src/db.rs benches/v1_bench.rs | verify:bench reports rewrite bytes and trigger reason by level
```

## Evidence

- Recent read-path work proves point, missing, cache, and data-block counters
  are available and useful for deciding retained optimizations.
- Persistent in-bounds missing reads already show Bloom/filter pruning can avoid
  data-block reads: 2048 filter skips, 0 data-block reads, and 0 read-owned
  storage requests in the bounded missing diagnostic.
- L0 can still contain overlapping tables; L1+ currently validates
  non-overlapping key ranges and usually selects at most one table per key per
  non-overlapping level.
- Hot/cold cache work already protects high-priority metadata from data-block
  churn; guard work should build on this rather than replacing the cache policy.
- The accepted design direction is recorded in
  `.phrase/protocol/guard-aware-lsm-strategy.md`.
- Task823 added point-read L0/non-L0 table-probe stats and a focused L0-stack
  diagnostic without changing read behavior. The local `cargo bench --bench
  v1_bench` run recorded 2048 repeated reads through an 8-table L0 stack as
  16384 L0 table probes, 2048 block metadata probes, and 2048 data-block reads.

## Known Risks

- Guard pruning must not skip a newer L0 table, a range tombstone, or a table
  needed by an old snapshot.
- Range and prefix scans need range-to-guard routing, not only point-key
  routing.
- Transaction conflict checks should remain conservative until guard coverage is
  proven for write/write and read/write conflicts.
- Non-uniform compaction must not hide obsolete data or blob references by
  simply delaying all lower-level work.

## Next Recommendation

- Implement `task824`: add explicit L0/overlap candidate-depth diagnostics for
  point, missing, and `get_many` before introducing the in-memory guard index.
