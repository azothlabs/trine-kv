# Current Phase

## Status

Complete

## Goal

Phase 4 of `.phrase/protocol/layered-filter-allocation.md`: make the per-level
filter bits curve user-configurable (and disablable) per bucket.

## Design Assessment

Designer decision: the curve lives in `BucketOptions` and is persisted, not a
runtime `DbOptions` knob. It is a filter property (base bits are already
per-bucket persisted there), it shapes durable SSTable filter sizing (a
non-persisted knob would silently drift across restarts and mix tables written
under different curves — worse than the ephemeral WAL-lane count that justified
`WalShardPolicy` being runtime), per-bucket granularity is free in
`BucketOptions`, and the manifest already has version-gated decode so the format
change is a routine, tested pattern. The shallow boundary stays tied to the
pinned-metadata levels and is not exposed.

## Scope

- `BucketOptions::filter_depth_curve: FilterDepthCurve` (`Auto` | `Uniform` |
  `Custom { step, floor }`), default `Auto`; `with_filter_depth_curve` builder.
- `level_adjusted_filter_bits(curve, base, level)` applies it to point + prefix.
- Manifest v10: append `filter_depth_curve`; versions < 10 decode to `Auto`.

## Out Of Scope

- Exposing the shallow boundary (kept tied to pinned levels).
- Cost-weighted (remote) / dynamic per-guard variants (Phase 5).

## Acceptance Gate

- No format change without protocol update + migration/recovery tests. Met.
- Curve is configurable and disablable. Met.
- Full local gate. Met (one pre-existing background-timing flake).

## Evidence

- `manifest_decode_v9_bucket_options_default_filter_depth_curve`: a v9 payload
  decodes with `filter_depth_curve == Auto`.
- `manifest_v10_bucket_options_round_trip_filter_depth_curve`: a `Custom { step:
  3, floor: 6 }` curve round-trips through v10 encode/decode.
- `level_adjusted_filter_bits_decreases_with_depth`: Auto / Uniform / Custom math.
- Every persistent test now round-trips a v10 manifest.
- `cargo test --lib` (358) and `--all-features` (362) green; fmt/clippy/diff clean.

## Known Risks

- Manifest is now v10 (min supported still 8); older readers cannot read v10, but
  Trine reads v8/v9 and defaults the curve to `Auto`.

## Next Recommendation

- Only Phase 5 remains (committed must-do): cost-weighted curve for remote
  backends and dynamic per-hot-guard filter rewrite. Do when an S3/remote backend
  or a workload needs them.
