# Current Phase

## Status

Complete

## Goal

Implement true internal batching for `get_many` point reads.

## Scope

- Default-bucket and bucket-reader `get_many` sync/async internals.
- Batch-local key deduplication with duplicate output scatter.
- LSM table lookup grouping across the keys in one batch.
- Persistent table point-read grouping by data block.
- Focused regression coverage and benchmark evidence.

## Out Of Scope

- Public API shape changes.
- Storage format, manifest format, WAL format, MVCC visibility rules,
  compaction policy, blob layout, or recovery contract changes.
- Claiming random small-batch workloads as the target win when benchmark
  evidence does not support that.

## Acceptance Gate

- `get_many` preserves input order, missing-key `None` behavior, duplicate keys,
  tombstones, range tombstones, blob values, and snapshot visibility.
- Persistent keys in the same data block share one data-block read in the batch
  path.
- Small batches with no duplicates can still use the existing single-key path
  to avoid planning overhead.
- Focused tests, formatting, clippy, full tests, bench smoke, release
  benchmark, and diff checks pass.

## Active Task Slice

```text
task610 [x] goal:batch point reads internally | scope:src/bucket.rs src/db.rs src/lsm/read.rs src/lsm/version.rs src/table.rs | verify:cargo test -q get_many --lib
task611 [x] goal:prove same-block persistent grouping | scope:src/db.rs | verify:get_many_sync_groups_persistent_keys_by_data_block
task612 [x] goal:record benchmark evidence | scope:benches/v1_bench.rs docs/benchmarks README .phrase | verify:cargo bench --bench v1_bench
```

## Evidence

- The batch path deduplicates input keys, keeps duplicate positions, and scatters
  resolved values back to input order.
- The version reader groups batch keys by table while preserving LSM table read
  order.
- Table point reads group keys by data block so one metadata/data-block pass can
  serve multiple requested keys in the same block.
- Release-profile benchmark evidence shows the locality-focused persistent
  batch row improved, while the old random four-key persistent row remains mixed.

## Known Residuals

- Random small batches are still not the strong workload for this optimization.
- Further point-read work should use a measured locality, cache, or table-open
  hotspot before adding complexity.

## Next Recommendation

- Commit the internal batching change with its benchmark record.
- If point-read speed remains the next target, measure cache-warm locality and
  cross-table locality before changing the table path again.
