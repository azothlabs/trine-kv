# Current Phase

## Status

Complete

## Goal

Reduce metadata and index block payload copies at the table decode boundary
while preserving MVCC visibility, table/blob formats, prefix/range filters,
compression contracts, and read-path correctness.

## Scope

- Properties, top-level index, index partition, filter, and range tombstone
  block reads.
- Full table verification decode.
- Source-offset checked block reads used by multi-block sections.
- Focused pointer tests proving shared source-offset payload reuse.
- Benchmark evidence showing whether metadata/index shared payload reads are
  visible in current startup/cache/decode rows.

## Out Of Scope

- Storage format changes.
- MVCC, snapshot, transaction, range-delete, prefix-filter, manifest, WAL,
  compaction, blob-GC, platform-io, publishing, tagging, pushing, or release
  workflow changes.
- New compression formats beyond the V1 `none` and `fast-lz4-block` contract.
- LZ4 decode buffer reuse.
- Changing manifest, WAL, blob file, table file, or block encoding.

## Acceptance Gate

- Normal metadata/index/filter/range-tombstone decoders consume shared checked
  block payload slices instead of compatibility payload `Vec`s.
- Full table verification decodes data blocks from shared checked blocks instead
  of first copying the payload into a `Vec`.
- Focused block/table tests pass.
- Full lib tests, all-feature tests, and strict clippy pass.
- Grouped benchmark evidence records current startup/cache/decode behavior
  without overstating noisy timing results.

## Active Task Slice

```text
task804 [x] goal:classify metadata/index compatibility payload Vec paths | scope:src/table.rs src/block.rs | verify:code audit
task805 [x] goal:consume metadata/index checked blocks as shared payload slices | scope:src/table.rs src/block.rs | verify:cargo test -q table --lib
task806 [x] goal:record metadata/index shared payload evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Phase 182 removed the read-owned storage buffer copy, but normal metadata and
  index helpers still converted checked block payloads into compatibility
  `Vec`s before decoding structures from `&[u8]`.
- Properties, top-level index, index partitions, filters, range tombstones, and
  full-table verification now decode from shared checked block payload slices.
- `BlockManager::read_checked_at_source_offset_shared` supports multi-block
  section reads such as the top-level index without returning a payload `Vec`.
- The focused test `shared_source_offset_read_reuses_owned_payload_bytes` proves
  source-offset shared reads keep the decoded payload inside the full owned
  block read buffer.
- `TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench` after the change reported
  improved but still noisy rows: cold table read median 21586 us, cold table
  read-only median 4447 us, random cached block read median 1541 us, warm
  cached read median 1363 us, inline runtime block decode read median 7421 us,
  native runtime block decode read median 7586 us, and forced decode diagnostic
  wall median 7967 us.

## Known Residuals

- Current end-to-end benchmark rows still did not prove a stable wall-time win
  from the structural copy removals.
- `fast-lz4-block` still decodes into a new buffer.
- Whole-object native storage APIs still return `Arc<[u8]>` in a few places
  outside the hot table data-block path.
- Some `#[cfg(test)]` compatibility helpers still return payload `Vec`s for old
  corruption tests.

## Next Recommendation

- If continuing serialization/decode work, measure LZ4 decode allocation before
  changing it.
- Otherwise, move next to concurrent read/write and background maintenance.
