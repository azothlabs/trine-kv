# Trine KV V1 Batched Point Reads

Date: 2026-06-03

Command:

```text
cargo bench --bench v1_bench > /tmp/trine-v1-bench-phase141-batch4-2.txt
```

Harness inputs:

- rows: 1024
- ops: 2048
- point-read batch size: 4 keys
- build profile: Cargo bench release profile
- storage: in-memory and temporary persistent directories under the OS temp dir

## Change Measured

This phase added batched point-read APIs for the default bucket, named buckets,
and snapshot-bound `BucketReader`s. The public methods return one result per
input key in input order, preserve duplicates, and return `None` for missing or
deleted keys. Storage or format errors fail the whole batch.

The implementation captures one committed read sequence and a batch-scoped
point-read source snapshot. For delta-backed in-memory writes, the snapshot is
limited to the delta shards touched by the input keys instead of reading every
delta shard.

## Release Benchmark Rows

Second release-profile run after reducing the benchmark batch size to 4:

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| random get | 2048 | 1105 | 1852833 | 40230 |
| missing get | 2048 | 776 | 2637900 | 0 |
| sequential point batch memory | 2048 | 1069 | 1914763 | 40190 |
| batched point read memory | 2048 | 2444 | 837685 | 40190 |
| sequential point batch persistent | 2048 | 1485 | 1378892 | 40237 |
| batched point read persistent | 2048 | 1391 | 1471484 | 40237 |

A prior run with the same batch size recorded memory at 3501 us sequential vs
3304 us batched, and persistent at 1439 us sequential vs 1502 us batched. The
timing evidence is mixed.

## Interpretation

- The API semantics are stable: input order is preserved, duplicates remain,
  and missing/deleted keys return `None`.
- Persistent flushed point reads showed a small improvement in the second
  batch-size-4 run.
- In-memory random point reads are not a stable win yet. Even with delta-shard
  narrowing, grouped random keys can still include unrelated delta state for a
  given key.
- The next performance step, if this remains important, should be deeper
  table/delta grouping inside the point-read path rather than more public API
  surface.

## Verification

- `cargo fmt --check`
- `cargo test -q get_many --lib`
- `cargo test -q --bench v1_bench`
- `cargo check --all-targets --all-features`
- `cargo bench --bench v1_bench`, with output redirected and only key rows
  inspected.
- `cargo rustdoc --all-features -- -D warnings`
- `cargo test --doc --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test -q --all-targets --all-features`
- `git diff --check`
- Forbidden-term and source-name scans over touched design and benchmark
  records.
