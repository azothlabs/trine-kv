# Current Phase

## Status

Complete

## Goal

Reduce `fast-lz4-block` decode allocation overhead by narrowing the `lz4_flex`
dependency feature set while preserving V1 codec ids, malformed-block checks,
table/blob formats, MVCC visibility, and read-path correctness.

## Scope

- `lz4_flex` Cargo feature policy for Trine's block compression implementation.
- Codec, table, and benchmark coverage for `fast-lz4-block`.
- Protocol documentation for the dependency feature boundary.
- Benchmark evidence showing whether the decode-only and table read rows move.

## Out Of Scope

- Storage format changes.
- MVCC, snapshot, transaction, range-delete, prefix-filter, manifest, WAL,
  compaction, blob-GC, platform-io, publishing, tagging, pushing, or release
  workflow changes.
- New compression formats beyond the V1 `none` and `fast-lz4-block` contract.
- LZ4 decode buffer reuse across blocks.
- Changing manifest, WAL, blob file, table file, or block encoding.
- Adding unsafe code to `trine-kv`.

## Acceptance Gate

- `cargo tree -e features -i lz4_flex` confirms `safe-decode` and `frame` are
  not enabled through Trine's default dependency set.
- Checked LZ4 decode remains enabled and codec ids remain unchanged.
- Focused codec/table tests pass.
- Full lib tests, all-feature tests, and strict clippy pass.
- Grouped benchmark evidence records decode-only and table read behavior
  without overstating noisy timing results.

## Active Task Slice

```text
task807 [x] goal:classify lz4_flex feature impact | scope:Cargo.toml Cargo.lock src/codec.rs benches/v1_bench.rs | verify:cargo tree -e features -i lz4_flex
task808 [x] goal:use checked lz4 block decode without default safe-decode zero-fill | scope:Cargo.toml Cargo.lock | verify:cargo test -q codec --lib && cargo test -q table --lib
task809 [x] goal:record lz4 decode allocation evidence | scope:docs/benchmarks .phrase/current.md .phrase/evidence.md .phrase/protocol/trine-kv-v1-spec.md | verify:git diff review
```

## Evidence

- Trine uses only `lz4_flex::block` encode/decode APIs; `frame` support is not
  used by the database format or benchmark harness.
- With default `lz4_flex` features, `safe-decode` creates an initialized output
  buffer before decoding. The dependency also exposes a checked decode path
  without `safe-decode`, which avoids that zero-fill while still rejecting
  malformed compressed input.
- `Cargo.toml` now disables default `lz4_flex` features and enables only
  `std`, `safe-encode`, and `checked-decode`.
- `cargo tree -e features -i lz4_flex` confirms Trine no longer enables
  `safe-decode` or `frame`.
- The dependency feature change removes the unused `twox-hash` lockfile entry
  that was pulled in by default frame support.
- `TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench` after the change reported
  `codec decode only fast block compression Trine data blocks` at 2235 us
  median and `codec decode only fast block compression Trine index blocks` at
  1121 us median. End-to-end cache/decode and cold-open rows remained noisy.

## Known Residuals

- This phase changes dependency feature policy, not the V1 block format.
- `fast-lz4-block` still returns a newly owned decoded buffer for compressed
  blocks; this phase reduces the buffer initialization cost instead of reusing
  one output buffer across independent cached blocks.
- The high-performance decode implementation is inside the `lz4_flex`
  dependency. Trine itself still has no new unsafe code in this phase.
- Whole-object native storage APIs still return `Arc<[u8]>` in a few places
  outside the hot table data-block path.
- Some `#[cfg(test)]` compatibility helpers still return payload `Vec`s for old
  corruption tests.

## Next Recommendation

- Move next to concurrent read/write and background maintenance unless new
  benchmark evidence points to another serialization/decode boundary.
