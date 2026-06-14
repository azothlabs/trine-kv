# Current Phase

## Status

Complete

## Goal

Reduce avoidable table block decode byte copies while preserving MVCC
visibility, table/blob formats, prefix/range filters, compression contracts,
and read-path correctness.

## Scope

- Owned checked block reads.
- `CodecId::None` block decode.
- Data block payload ownership and inline value sharing.
- Focused tests proving payload reuse and table read correctness.
- Benchmark evidence showing whether the structural copy removal is visible in
  current end-to-end rows.

## Out Of Scope

- Storage format changes.
- MVCC, snapshot, transaction, range-delete, prefix-filter, manifest, WAL,
  compaction, blob-GC, platform-io, publishing, tagging, pushing, or release
  workflow changes.
- New compression formats beyond the V1 `none` and `fast-lz4-block` contract.
- LZ4 decode buffer reuse.
- Replacing the storage read buffer backing type across the whole engine.

## Acceptance Gate

- `CodecId::None` owned block decode reuses the read-owned block payload bytes
  instead of duplicating the payload into a new `Vec`.
- Data block reads can consume shared decoded block payload ranges without
  changing record, filter, inline value, or point lookup semantics.
- Focused block/table tests pass.
- Full lib tests and strict clippy pass.
- Grouped benchmark evidence records current cache/decode behavior without
  overstating noisy timing results.

## Active Task Slice

```text
task798 [x] goal:classify decode byte-copy boundary | scope:src/block.rs src/table.rs | verify:code audit
task799 [x] goal:reuse none-codec owned block payload bytes | scope:src/block.rs src/table.rs | verify:cargo test -q block --lib && cargo test -q table --lib
task800 [x] goal:record decode-copy evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Phase 180 proved block-cache hit maintenance was a small but real cost, while
  forced decode and codec rows kept decode/allocation cost visible.
- `BlockManager::decode_checked_owned` now returns a shared decoded block. For
  `CodecId::None`, the decoded payload is a range inside the original
  read-owned block bytes after checksum and length validation.
- `DecodedDataBlock` now stores shared bytes plus a payload range, so data block
  record views and inline point values can reference the payload even when the
  shared bytes include a checked-block header.
- The focused test `none_codec_owned_decode_reuses_payload_bytes` proves the
  decoded `none` payload pointer is the original read buffer pointer plus the
  checked-block header length.
- `TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench` after the change reported
  mixed/noisy cache-decode timing: random cached block read median 1714 us,
  warm cached read median 1553 us, inline runtime block decode read median
  8170 us, native runtime block decode read median 7979 us, and forced decode
  diagnostic wall median 8716 us.

## Known Residuals

- Current end-to-end benchmark rows did not prove a stable wall-time win from
  this structural copy removal.
- `fast-lz4-block` still decodes into a new buffer.
- Metadata and index block helpers still use the compatibility path that
  returns a payload `Vec`.
- `StorageReadBuffer::from_vec` still wraps read buffers into `Arc<[u8]>`; a
  whole-path zero-copy read buffer would need a separate shared byte backing
  type that also works with value sources and caches.

## Next Recommendation

- If continuing serialization/decode work, measure and reduce the remaining
  storage-read buffer copy and LZ4 decode allocation boundary as a separate
  phase.
- Otherwise, move next to concurrent read/write and background maintenance.
