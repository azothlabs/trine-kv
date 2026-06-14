# Current Phase

## Status

Complete

## Goal

Reduce the remaining read-owned buffer copy at the storage/decode boundary
while preserving MVCC visibility, table/blob formats, prefix/range filters,
compression contracts, and read-path correctness.

## Scope

- `StorageReadBuffer` backing ownership.
- Checked block and data block shared payload ownership.
- Inline point value sharing from decoded data blocks.
- Focused pointer tests proving read-owned buffer reuse.
- Benchmark evidence showing whether the backing change is visible in current
  cache/decode rows.

## Out Of Scope

- Storage format changes.
- MVCC, snapshot, transaction, range-delete, prefix-filter, manifest, WAL,
  compaction, blob-GC, platform-io, publishing, tagging, pushing, or release
  workflow changes.
- New compression formats beyond the V1 `none` and `fast-lz4-block` contract.
- LZ4 decode buffer reuse.
- Changing manifest, WAL, blob file, table file, or block encoding.

## Acceptance Gate

- `StorageReadBuffer::from_vec` reuses the input `Vec<u8>` allocation as shared
  bytes instead of copying the payload into `Arc<[u8]>`.
- `CodecId::None` owned block decode and decoded data blocks keep using shared
  byte ranges without changing record, filter, inline value, or point lookup
  semantics.
- Focused storage/block/table tests pass.
- Full lib tests, all-feature tests, and strict clippy pass.
- Grouped benchmark evidence records current cache/decode behavior without
  overstating noisy timing results.

## Active Task Slice

```text
task801 [x] goal:classify read-owned buffer backing copy | scope:src/storage.rs src/block.rs src/table.rs src/point_value.rs | verify:code audit
task802 [x] goal:reuse Vec read buffer allocation as shared bytes | scope:Cargo.toml src/storage.rs src/block.rs src/table.rs src/point_value.rs src/blob.rs | verify:cargo test -q storage_read_buffer_from_vec_reuses_vec_allocation --lib
task803 [x] goal:record storage buffer evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Phase 181 removed the uncompressed checked-block payload duplicate copy, but
  still left `StorageReadBuffer::from_vec` wrapping the read buffer into
  `Arc<[u8]>`.
- `StorageReadBuffer` now stores `bytes::Bytes`. `Bytes::from(Vec<u8>)`
  reuses the input allocation and still gives cheap cloneable shared read-only
  bytes for decoded blocks, block cache entries, and inline point values.
- `DecodedBlock`, `DecodedDataBlock`, and shared `PointValue` backing now use
  `Bytes`, preserving the payload range model introduced in Phase 181.
- The focused test `storage_read_buffer_from_vec_reuses_vec_allocation` proves
  the storage read buffer slice pointer is the original `Vec<u8>` pointer.
- `TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench` after the change reported
  mixed/noisy cache-decode timing: random cached block read median 1741 us,
  warm cached read median 1494 us, inline runtime block decode read median
  8129 us, native runtime block decode read median 7958 us, and forced decode
  diagnostic wall median 8873 us. The forced decode diagnostic storage
  read-owned micros median was 566 us.

## Known Residuals

- Current end-to-end benchmark rows still did not prove a stable wall-time win
  from the structural copy removals.
- `fast-lz4-block` still decodes into a new buffer.
- Metadata and index block helpers still use the compatibility path that
  returns a payload `Vec`.
- Whole-object native storage APIs still return `Arc<[u8]>` in a few places
  outside the hot table data-block path.

## Next Recommendation

- If continuing serialization/decode work, measure LZ4 decode allocation and
  metadata/index compatibility payload `Vec` paths before changing them.
- Otherwise, move next to concurrent read/write and background maintenance.
