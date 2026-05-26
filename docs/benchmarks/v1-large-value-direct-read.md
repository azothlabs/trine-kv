# Trine KV V1 Large-Value Direct Read Tuning

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

## Selected Hotspot

After adding dedicated large-value rows, point and range reads over blob-backed
values were the clear hotspot:

| name | before_elapsed_us | before_units_per_sec |
| --- | ---: | ---: |
| separated blob values | 106723 | 2398 |
| blob point read | 1140632 | 224 |
| blob range scan | 1128654 | 28 |
| blob GC rewrite | 148488 | 862 |

The read path was still decoding the whole blob file for one `BlobIndex` even
though the index stores the record offset. That made point reads scale with the
blob file size instead of the value being returned.

## After Tuning

| name | after_elapsed_us | after_units_per_sec |
| --- | ---: | ---: |
| separated blob values | 83917 | 3050 |
| blob point read | 13976 | 18316 |
| blob range scan | 13719 | 2332 |
| blob GC rewrite | 153518 | 833 |

`blob point read` improved from 1140632 us to 13976 us for this local run.
`blob range scan` improved from 1128654 us to 13719 us. Blob GC rewrite stayed
roughly unchanged, as expected, because this tuning targets reads and keeps GC
on the full-file scan path.

## Implementation

- `BlobIndex` reads validate the blob header, seek to the indexed record frame,
  verify the record checksum, decode that one record, and compare the decoded
  metadata with the expected index and internal key.
- Full blob-file decoding remains in recovery validation and GC scanning.

## Verification

- `cargo test blob::tests --all-features`
- `cargo bench --bench v1_bench` before and after tuning
