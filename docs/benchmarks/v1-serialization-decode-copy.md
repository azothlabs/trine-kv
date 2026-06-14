# V1 Serialization And Decode Copy Cost

This note records Phase 181. The phase narrowed the cache/decode follow-up from
general cache policy to a concrete serialization boundary: owned checked block
decode duplicated `CodecId::None` payload bytes before table data block decode
could parse the records.

## What Changed

`BlockManager::decode_checked_owned` now returns a shared decoded block.

For `CodecId::None`, the decoded payload is represented as a range inside the
read-owned checked block bytes after the block header, length fields, codec tag,
and checksum have been validated. That removes the extra payload `Vec` that the
old owned path created for uncompressed blocks.

`DecodedDataBlock` now stores shared bytes plus a payload range. Most decoded
data blocks still behave as if the payload begins at byte zero, but uncompressed
owned reads can point into a larger checked-block buffer that also contains the
block header. Record views, point lookup validation, restart points, and inline
value sources use the payload range so their offsets remain relative to the data
block payload.

Compatibility helpers still return `(CodecId, Vec<u8>)` for metadata/index
paths that have not been moved to shared decoded blocks.

## Evidence

The focused regression test `none_codec_owned_decode_reuses_payload_bytes`
checks that an owned `CodecId::None` decode returns a payload pointer equal to
the original read buffer pointer plus the checked-block header length.

Validation:

```text
cargo fmt --check
cargo test -q block --lib
cargo test -q data_block --lib
cargo test -q table --lib
cargo test -q --lib
cargo test -q --all-features
cargo clippy --all-targets --all-features -- -D warnings
TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench
```

The grouped benchmark run after the change reported:

| Row | Median elapsed us |
| --- | ---: |
| block cache random block read | 1714 |
| block cache warm read | 1553 |
| inline runtime block decode read | 8170 |
| native runtime block decode read | 7979 |
| block decode forced diagnostic wall micros | 8716 |
| codec decode only none Trine data blocks | 189 |
| codec decode only fast block compression Trine data blocks | 3681 |

The same run kept the forced decode diagnostic shape stable: zero cache hits,
2048 cache misses, 2048 data block reads, and 2049 storage read-owned requests.

## Interpretation

The retained change is a structural allocation/copy reduction for uncompressed
owned data-block reads. It preserves the disk format and table semantics, and
the pointer-level test proves the payload no longer moves through an extra
owned payload buffer on that path.

The end-to-end benchmark rows did not prove a stable wall-time improvement.
They remained noisy and in this local run were slower than the previous Phase
180 cache-maintenance record. Treat this phase as a necessary cleanup of the
decode ownership boundary, not as proof that cache/decode latency is solved.

Remaining decode costs are still visible:

- `fast-lz4-block` still allocates a decoded buffer.
- Metadata and index block helpers still use compatibility `Vec` payloads.
- `StorageReadBuffer::from_vec` still wraps read buffers into `Arc<[u8]>`;
  removing that whole-path copy would need a shared byte backing type that works
  with data blocks, value sources, block cache entries, and storage objects.
