# V1 Metadata And Index Shared Payload Decode

This note records Phase 183. Phase 182 made checked block bytes shareable with
`bytes::Bytes`, but normal metadata/index helpers still converted checked block
payloads into compatibility `Vec<u8>` values before immediately decoding from
`&[u8]`.

## What Changed

Normal table metadata decode paths now consume shared checked block payload
slices directly:

- properties block,
- top-level index block,
- index partition blocks,
- filter block,
- range tombstone block,
- full-table verification index and data blocks.

`BlockManager::read_checked_at_source_offset_shared` was added for multi-block
sections such as the top-level index. Older compatibility helpers that return a
payload `Vec` are limited to tests or wrapper paths that still explicitly need
owned bytes.

## Evidence

The focused regression test `shared_source_offset_read_reuses_owned_payload_bytes`
records the full owned block read buffer payload pointer and checks that a
source-offset shared read returns a decoded payload slice at that pointer.

Validation:

```text
cargo fmt --check
cargo test -q shared_source_offset_read_reuses_owned_payload_bytes --lib
cargo test -q table --lib
cargo test -q persistent --lib
cargo test -q --lib
cargo test -q --all-features
cargo clippy --all-targets --all-features -- -D warnings
TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench
```

The grouped benchmark run after the change reported:

| Row | Median elapsed us |
| --- | ---: |
| cold table read | 21586 |
| cold table read-only | 4447 |
| cold table first read wall micros | 1868 |
| cold table open wall micros | 8693 |
| cold table read-only first read wall micros | 1742 |
| cold table read-only open wall micros | 2592 |
| block cache random block read | 1541 |
| block cache warm read | 1363 |
| inline runtime block decode read | 7421 |
| native runtime block decode read | 7586 |
| block decode forced diagnostic wall micros | 7967 |
| block decode forced diagnostic storage read owned micros | 109 |

The previous Phase 182 grouped run reported cold table read 22573 us, cold table
read-only 4692 us, random cached block read 1741 us, warm cached read 1494 us,
inline runtime block decode read 8129 us, native runtime block decode read
7958 us, forced decode wall 8873 us, and forced storage read-owned micros
566 us.

## Interpretation

The retained change removes avoidable payload `Vec` copies from normal
metadata/index decode. It preserves table format, compression format, metadata
validation, filters, point lookup behavior, full table verification, and lazy
value semantics.

This local benchmark run moved the relevant startup and cache/decode rows in
the right direction, but prior runs showed noise in the same area. Treat this
as a proven ownership/copy cleanup with supportive benchmark evidence, not as
proof that all decode latency is complete.

Remaining decode costs:

- `fast-lz4-block` still allocates decoded output.
- Some test-only compatibility helpers still return payload `Vec`s for old
  corruption tests.
- Whole-object storage helper APIs still return `Arc<[u8]>` where that remains
  their existing contract.
