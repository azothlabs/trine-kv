# Trine KV V1 Get-Many Internal Batching

Date: 2026-06-03

Command:

```text
cargo bench --bench v1_bench > /tmp/trine-get-many-batch-bench.txt
```

Harness inputs:

- rows: 1024
- ops: 2048
- random point-read batch size: 4 keys
- localized point-read batch size: 16 keys
- build profile: Cargo bench release profile
- storage: temporary persistent directories under the OS temp dir

## Change Measured

This phase made `BucketReader::get_many*` use an internal batch read path
instead of looping over single-key reads.

The internal path deduplicates input keys, preserves duplicate output positions,
groups table point lookups by table and data block, and scatters results back
to input order. Small batches without duplicate keys keep the single-key path
to avoid planning overhead. Larger localized batches and duplicate-key batches
use table/block grouping.

## Release Benchmark Rows

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| sequential point batch persistent | 2048 | 1290 | 1586776 | 40237 |
| batched point read persistent | 2048 | 1500 | 1364764 | 40237 |
| localized sequential point batch persistent | 2048 | 2753 | 743724 | 40238 |
| localized batched point read persistent | 2048 | 1212 | 1689477 | 40238 |

## Interpretation

- Random batch-size-4 reads still are not the target win. They avoid deeper
  grouping when they have no duplicate keys, but they still pay batch API and
  batch snapshot setup cost.
- Localized persistent batches now exercise the intended table/block grouping
  path. In this run, the 16-key localized batch row improved from 2753 us to
  1212 us against the sequential localized row.
- A focused regression test also verifies that multiple keys in the same
  persistent data block share one point data-block read while preserving
  duplicate output entries.

## Verification

- `cargo fmt --check`
- `cargo test -q get_many --lib`
- `cargo test -q --bench v1_bench`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test -q --all-targets --all-features`
- `cargo bench --bench v1_bench`, output redirected and key rows inspected.
- `git diff --check`
