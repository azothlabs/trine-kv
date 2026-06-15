# Current Phase

## Status

Complete

## Goal

Phase 3 of `.phrase/protocol/layered-filter-allocation.md`: give the prefix
filter the same depth-scaled bits curve as the point filter, so deep-level
prefix filters cost less memory for prefix-heavy workloads.

## Design Assessment

Symmetric with Phase 2. The Phase 2 point curve was generalized into one shared
`level_adjusted_filter_bits(base, level)` and applied to the prefix filter
`bits_per_prefix` in `build_prefix_filter` (block-level at all levels, plus the
shallow pinned table-level prefix filter). The prefix filter is self-describing,
so this is a write-time change with no read-path or storage-format impact. Deep
prefix false positives rise (bounded by the floor); hot shallow levels stay
fully accurate.

## Scope

- `build_prefix_filter` takes `level` and applies `level_adjusted_filter_bits`.
- Curve helper renamed/generalized (point + prefix share it).
- Tests: shared curve unit test; prefix-isolated encoded-size test proving deep
  prefix filters shrink.

## Out Of Scope

- Storage-format / `BucketOptions` change (user-configurable curve is Phase 4).
- Cost-weighted (remote) / dynamic per-guard variants (Phase 5).

## Acceptance Gate

- Deep levels write smaller prefix filters at equal records (residency-
  independent via encoded size). Met (`deeper_levels_write_smaller_prefix_filters`).
- Curve never exceeds base (shared unit test). Met.
- Full local gate. Met (one pre-existing background-timing flake, isolated pass).

## Evidence

- `level_adjusted_filter_bits`: 10,10,8,6,4,4 for L0..L5; base 3 -> 3.
- `deeper_levels_write_smaller_prefix_filters`: same 600 distinct-prefix records
  encode strictly smaller at L4 than L2 with point filter disabled (isolates the
  prefix curve).
- Per-level prefix FPR observable via `level_filters[*].filters.table_prefix_*`;
  prefix bytes included in `filter_resident_bytes`.
- `cargo test --lib` (356) and `--all-features` (360) green; fmt/clippy/diff clean.

## Known Risks

- Deep prefix FPR rises (~15% at the 4-bit floor); on slow embedded storage a
  truly-absent deep prefix scan may touch a data block. Base `bits_per_prefix` is
  the lever; a configurable curve is Phase 4.

## Next Recommendation

- Layered-filter Phases 1-3 done. Phase 4 (user-configurable curve; manifest
  version bump) and Phase 5 (cost-weighted remote / dynamic per-guard) remain the
  committed must-do follow-ups. Tackle when a workload needs them.
