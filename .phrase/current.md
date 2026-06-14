# Current Phase

## Status

Complete

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
task824 [x] goal:record L0/overlap candidate depth for point/missing/get_many | scope:src/lsm/version.rs benches/v1_bench.rs | verify:diagnostics show candidate depth separate from filter/data-block counters
task825 [x] goal:derive first in-memory guard index only if diagnostics prove avoidable L0/overlap probes | scope:src/lsm/version.rs src/lsm/read.rs | verify:point/missing/get_many counters improve without extra data-block reads
task826 [x] goal:add compaction rewrite-depth diagnostics before policy changes | scope:src/lsm/compact.rs src/db.rs benches/v1_bench.rs | verify:bench reports rewrite bytes and trigger reason by level
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
- Task824 added version-level L0 overlap-depth and grouped point-batch shape
  diagnostics. The local `cargo bench --bench v1_bench` run recorded the same
  L0-stack sequential row as 2048 L0 lookup keys and 14336 extra L0 table
  probes. The new L0-stack batch-4 row recorded 2048 input keys, 512 unique
  keys, 512 table groups, 512 batch L0 lookup keys, and 3584 extra batch L0
  table probes.
- Task825 retained the first in-memory L0 guard pruning by applying existing
  table user-key bounds before L0 point table probes. The local `cargo bench
  --bench v1_bench` run reduced the L0-stack sequential row to 2048 L0 table
  probes and 0 extra L0 probes, and reduced the L0-stack batch-4 row to 512 L0
  table probes and 0 extra batch L0 probes. Block metadata probes and
  data-block reads stayed at 2048 for sequential and 512 for batch-4.
- Task826 added per-level compaction input/output table and byte diagnostics,
  plus rewritten-byte rows. The filtered benchmark row for `write amp
  compaction diagnostic` recorded 35950 input bytes from level 0, 34931 output
  bytes to level 1, and 70881 rewritten bytes total.

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

- Close Phase 187 diagnostics and first point-read guard pruning. The next
  phase should use the new compaction level evidence before changing
  guard-aware or non-uniform compaction policy.
