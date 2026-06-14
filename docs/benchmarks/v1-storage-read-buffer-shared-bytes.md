# V1 Storage Read Buffer Shared Bytes

This note records Phase 182. Phase 181 removed the uncompressed checked-block
payload duplicate copy, but the read-owned buffer itself still copied the read
`Vec<u8>` into `Arc<[u8]>` before decode could share it.

## What Changed

`StorageReadBuffer` now stores `bytes::Bytes`.

`Bytes::from(Vec<u8>)` reuses the read buffer allocation and still gives Trine a
cheaply cloneable shared byte owner. The same shared owner now backs:

- checked block decode,
- decoded data block payload ranges,
- block-cache data entries,
- inline point values returned from decoded table blocks.

Whole-object storage APIs that still expose `Arc<[u8]>` keep a compatibility
conversion outside the hot data-block read path.

## Evidence

The focused regression test `storage_read_buffer_from_vec_reuses_vec_allocation`
records the input `Vec<u8>` pointer, wraps it in `StorageReadBuffer`, and checks
that the resulting read buffer slice has the same pointer.

Validation:

```text
cargo fmt --check
cargo test -q storage_read_buffer_from_vec_reuses_vec_allocation --lib
cargo test -q none_codec_owned_decode_reuses_payload_bytes --lib
cargo test -q table --lib
cargo test -q --lib
cargo test -q --all-features
cargo clippy --all-targets --all-features -- -D warnings
TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench
```

The grouped benchmark run after the change reported:

| Row | Median elapsed us |
| --- | ---: |
| block cache random block read | 1741 |
| block cache warm read | 1494 |
| inline runtime block decode read | 8129 |
| native runtime block decode read | 7958 |
| block decode forced diagnostic wall micros | 8873 |
| block decode forced diagnostic storage read owned micros | 566 |
| codec decode only none Trine data blocks | 193 |
| codec decode only fast block compression Trine data blocks | 4101 |

The previous Phase 181 grouped run reported random cached block read 1714 us,
warm cached read 1553 us, inline runtime block decode read 8170 us, native
runtime block decode read 7979 us, forced decode wall 8716 us, and forced
decode storage read-owned micros 1231 us.

## Interpretation

The retained change removes a concrete read-owned payload copy before checked
block decode. The pointer-level test proves the storage read buffer now reuses
the input read allocation, and the existing Phase 181 pointer test still proves
uncompressed checked-block decode points into that shared buffer.

The end-to-end benchmark rows remain noisy. Warm cached reads, inline decode,
native decode, and forced storage read-owned micros moved in the right direction
in this local run; random cached block read and forced decode wall time did not.
Treat this as a storage/decode ownership cleanup, not as proof that cache/decode
latency is complete.

Remaining decode costs:

- `fast-lz4-block` still allocates decoded output.
- Metadata and index block helpers still return compatibility payload `Vec`s.
- Whole-object storage helper APIs still return `Arc<[u8]>` where that is their
  existing contract.
