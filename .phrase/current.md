# Current Phase

## Status

Complete

## Goal

Phase 2 of `.phrase/protocol/layered-filter-allocation.md`: build the point
filter with a depth-scaled `bits_per_key` at write time so deep levels cost less
filter memory (Monkey-style, memory-first per Phase 1 evidence).

## Design Assessment

Trine has two point-filter tiers: the table-level filter (pinned shallow levels
only) and the block-level filter (per data block, all levels). The block-level
filter is the cross-level lever, so the curve applies there (and to the shallow
table-level filter). The filter block is self-describing, so per-level
`bits_per_key` is a write-time change with no read-path or storage-format
impact.

Phase 1 showed the two-level filter already drives negative-lookup data-block
reads to zero, so the curve is memory-first: it only lowers deep levels (never
above the base), so total filter memory can only drop. The trade is a higher
deep-level false-positive rate (about 15% at the floor), bounded; hot shallow
levels (L0/L1) stay fully accurate.

## Scope

- `level_adjusted_point_bits_per_key(base, level)`: L0/L1 keep base; deeper
  levels drop by 2 bits, floor 4, never above base.
- Applied to block-level point filter (`build_data_blocks`) and shallow
  table-level filter.
- `Table::resident_filter_bytes` + `DbStats::level_filters[*].filter_resident_bytes`.

## Out Of Scope

- Storage-format / `BucketOptions` change (a user-configurable curve is Phase 4).
- Prefix filter tuning (Phase 3).
- Equal-budget reallocation (raising shallow above base): not needed since I/O is
  already 0; memory-first avoids any memory regression.

## Acceptance Gate

- Deep levels write smaller filters at equal records (residency-independent via
  encoded table size). Met (`deeper_levels_write_smaller_block_filters`).
- Curve never exceeds base, so total filter memory cannot regress. Met (unit
  test, including small-base clamp).
- Hot shallow levels keep base accuracy: L0/L1 FPR and negative-lookup
  data-block reads unchanged. Met (diagnostic: ~1% FPR, 0 data-block reads).
- Full local gate. Met.

## Evidence

- `level_adjusted_point_bits_per_key`: 10,10,8,6,4,4 for L0..L5; base 3 -> 3.
- `deeper_levels_write_smaller_block_filters`: same 600 records encode strictly
  smaller at L4 than L2.
- Diagnostic L0/L1: FPR ~1%, data-block reads 0, resident filter bytes 2560/level.
- `cargo test --lib` (352) and `--all-features` (356) green; fmt/clippy/bench/diff
  clean.

## Known Risks

- Deep FPR ~15% at the 4-bit floor: on slow embedded storage a truly-absent deep
  key may incur a data-block read. The per-bucket base `bits_per_key` is the
  lever; a configurable curve is the deferred Phase 4.
- `filter_resident_bytes` reflects resident filters only; deep file-backed block
  filters are lazy, so the deep memory win shows in encoded table size, not
  resident bytes.

## Next Recommendation

- Phase 3 (prefix filter tuning) is optional; otherwise return to the single-key
  sync-write fsync bottleneck (~500 ops/s), the largest remaining throughput
  hotspot. Phase 4 (configurable curve) only if a workload needs to tune the
  deep-FPR/storage trade.
