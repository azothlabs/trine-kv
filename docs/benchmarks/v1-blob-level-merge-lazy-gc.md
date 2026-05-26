# Trine KV V1 Blob Maintenance And Lazy Value Benchmark

Date: 2026-05-26

Command:

```text
cargo bench --bench v1_bench
```

Context:

- rows: 1024
- ops: 2048
- large-value rows: 128
- large-value ops: 256
- large value size: 16 KiB
- build profile: Cargo bench release profile
- comparison scope: same local machine, same session

## Focus Rows

| name | elapsed_us | units_per_sec |
| --- | ---: | ---: |
| blob point read | 13518 | 18936 |
| blob range scan | 12929 | 2475 |
| blob range lazy keys | 173 | 184926 |
| blob GC rewrite | 126649 | 1010 |
| blob level merge | 123261 | 1038 |

## Interpretation

`blob range lazy keys` measures scanning keys without asking for blob values.
It is much cheaper than value-returning range scans because it avoids blob
record reads until the caller requests a value.

`blob GC rewrite` now selects candidates from blob footer/properties metadata,
batches all candidates that pass the discard threshold, and reads only live
referenced records by `BlobIndex`. This row disables Level Merge so it measures
GC directly. Recovery still validates full blob files.

`blob level merge` measures compaction rewriting of retained large values into
the output blob file. The default `Auto` policy triggers this when output refs
would otherwise span multiple blob files or leave stale input refs behind.

## Verification

- `cargo bench --bench v1_bench`
