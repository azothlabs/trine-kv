# Trine KV V1 Read-Pruning Measurement

Date: 2026-06-03

Command:

```text
cargo bench --bench v1_bench
```

Harness inputs:

- rows: 1024
- ops: 2048
- build profile: Cargo bench release profile
- storage: in-memory and temporary persistent directories under the OS temp dir

## Change Measured

This phase added read-pruning diagnostic rows to the benchmark harness and
extended `DbStats::read_path` with prefix-scan table, block metadata,
data-block, and filter-skip counters.

The first diagnostic run before the cursor fix showed that matching persistent
prefix scans read only 152 data blocks but performed 8664 prefix block metadata
probes. The cursor was rechecking the same block state while returning records
from an already-loaded block.

The kept change lets table cursors continue consuming records from the current
data block without re-running block-state metadata and filter checks until the
cursor advances to a different block.

## Release Benchmark Rows

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| prefix scan | 128 | 7455 | 17169 | 160952 |
| prefix scan table partitions matching | 128 | 2959 | 43244 | 160952 |
| prefix scan table partitions nonmatching | 128 | 87 | 1463559 | 0 |
| cold table read | 32 | 165814 | 192 | 640 |
| read pruning cold point table probes | 1 | 0 | 0 | 32 |
| read pruning cold point block metadata probes | 1 | 0 | 0 | 32 |
| read pruning cold point data block reads | 1 | 0 | 0 | 32 |
| read pruning cold point filter skips | 1 | 0 | 0 | 0 |
| read pruning cold point cache misses | 1 | 0 | 0 | 32 |
| read pruning prefix matching table probes | 1 | 0 | 0 | 128 |
| read pruning prefix matching block metadata probes | 1 | 0 | 0 | 472 |
| read pruning prefix matching data block reads | 1 | 0 | 0 | 152 |
| read pruning prefix matching filter skips | 1 | 0 | 0 | 0 |
| read pruning prefix matching table filter misses | 1 | 0 | 0 | 0 |
| read pruning prefix matching block filter misses | 1 | 0 | 0 | 0 |
| read pruning prefix matching cache misses | 1 | 0 | 0 | 4 |
| read pruning prefix nonmatching table probes | 1 | 0 | 0 | 0 |
| read pruning prefix nonmatching block metadata probes | 1 | 0 | 0 | 0 |
| read pruning prefix nonmatching data block reads | 1 | 0 | 0 | 0 |
| read pruning prefix nonmatching filter skips | 1 | 0 | 0 | 0 |
| read pruning prefix nonmatching table filter misses | 1 | 0 | 0 | 0 |
| read pruning prefix nonmatching block filter misses | 1 | 0 | 0 | 0 |
| read pruning prefix nonmatching cache misses | 1 | 0 | 0 | 0 |

## Interpretation

- Matching prefix scans still touch 128 table candidates, but the block-level
  work is now much closer to the number of data blocks actually read.
- Nonmatching prefixes are rejected before any table probe in this benchmark
  shape because the constructed query range does not overlap the table bounds.
- Cold point reads still show one table probe, one metadata probe, one data
  block read, and one cache miss per reopen/read. The next cold-read target is
  likely open/read I/O or cache warmup behavior, not filter pruning.
- The `0.1` baseline recorded `prefix scan` at 9571 us, `prefix scan table
  partitions matching` at 3551 us, and `cold table read` at 183876 us. This
  release-profile run recorded 7455 us, 2959 us, and 165814 us respectively.

## Verification

- `cargo fmt --check`
- `cargo test -q table --lib`
- `cargo test -q --test async_api persistent_async_range_and_prefix_advance_flushed_tables`
- `cargo test -q persistent_prefix_filter_keeps_range_tombstones_authoritative`
- `cargo test -q --bench v1_bench`
- `cargo bench --bench v1_bench`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test -q --all-targets --all-features`
- `git diff --check`
