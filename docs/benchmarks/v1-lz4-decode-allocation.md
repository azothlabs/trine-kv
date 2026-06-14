# V1 LZ4 Decode Allocation Policy

This note records Phase 184. After the earlier shared-byte phases, compressed
`fast-lz4-block` reads still had to allocate a decoded output buffer. The goal
of this phase was to reduce avoidable initialization cost without changing the
V1 block format or adding unsafe code to Trine.

## What Changed

Trine now declares `lz4_flex` with default features disabled and enables only:

- `std`,
- `safe-encode`,
- `checked-decode`.

The database uses only `lz4_flex::block` compression. It does not use the frame
API, so the default `frame` feature and its `twox-hash` dependency are no
longer part of the default build. Keeping `checked-decode` preserves
malformed-block rejection, while disabling `safe-decode` avoids the default
safe decoder's zero-filled output buffer on the table block read path.

This changes dependency feature policy only. Trine still writes stable codec ids
such as `fast-lz4-block`, still checks decoded lengths, and still rejects
invalid compressed blocks.

## Evidence

Feature audit:

```text
cargo tree -e features -i lz4_flex
```

The result shows only `std`, `safe-encode`, and `checked-decode` enabled through
Trine's default dependency set.

Validation:

```text
cargo check -q
cargo test -q codec --lib
cargo test -q table --lib
cargo test -q --lib
cargo test -q --all-features
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench
```

The grouped benchmark run after the change reported:

| Row | Median elapsed us |
| --- | ---: |
| codec decode only fast block compression Trine data blocks | 2235 |
| codec decode only fast block compression Trine index blocks | 1121 |
| codec decode only fast block compression Trine range tombstone blocks | 1026 |
| codec fast block compression Trine data blocks | 3476 |
| codec fast block compression Trine index blocks | 1947 |
| codec fast block compression Trine range tombstone blocks | 1751 |
| block cache random block read | 2036 |
| block cache warm read | 1685 |
| inline runtime block decode read | 8981 |
| native runtime block decode read | 8702 |
| block decode forced diagnostic wall micros | 9760 |
| block decode forced diagnostic storage read owned micros | 2049 |
| cold table read | 22455 |
| cold table read-only | 5215 |

For comparison, the previous recorded Phase 182 codec decode-only data-block
row was 4101 us median, and the Phase 183 cache/decode rows were lower but
known noisy. Treat this phase as a targeted LZ4 decode allocation policy change
with supportive codec-row evidence, not as proof that end-to-end table read
latency is finished.

## Interpretation

The retained change removes a dependency feature mismatch: Trine's format needs
checked LZ4 block decode, not frame compression or default safe-decode buffer
initialization. The high-performance decode implementation lives inside the
`lz4_flex` dependency; this phase does not introduce unsafe code in Trine
itself.

Remaining decode costs:

- compressed blocks still return a newly owned decoded buffer;
- cross-block decode buffer reuse would need a separate design because decoded
  data blocks can be shared by cache entries and lazy value reads;
- a few whole-object storage compatibility helpers still return `Arc<[u8]>`
  outside the hot data-block path.
