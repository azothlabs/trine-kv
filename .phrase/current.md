# Current Phase

## Status

Complete

## Goal

Add read-pruning measurement for point and prefix workloads, then keep the
first measured prefix-scan metadata reduction.

## Scope

- `DbStats::read_path` counters for prefix-scan table probes, block metadata
  probes, data-block reads, and filter skips.
- Benchmark diagnostic rows for cold point reads and persistent prefix scans.
- Cursor behavior inside already-loaded table data blocks.
- Benchmark evidence in `docs/benchmarks/v1-read-pruning-measurement.md`.

## Out Of Scope

- Rust code changes.
- Public API behavior changes.
- Storage format, MVCC, WAL, manifest, SSTable, blob, compaction, transaction,
  recovery, or browser persistence behavior changes.
- Storage format changes.
- Compaction, blob, WAL, manifest, transaction, recovery, browser persistence,
  or release metadata changes.
- Publishing, tagging, pushing, or release metadata changes.

## Acceptance Gate

- Benchmark diagnostics expose table probes, block metadata probes, data-block
  reads, filter skips, and cache misses for cold point and prefix workloads.
- Prefix scan cursor advancement no longer rechecks block-state metadata while
  returning records from the current loaded block.
- Focused range/prefix/table tests pass.
- `cargo bench --bench v1_bench` records the measurement rows.
- Formatting, clippy, full tests, and diff checks pass.

## Active Task Slice

```text
task575 [x] goal:add prefix read-pruning counters | scope:src/stats.rs src/table.rs src/lsm/scan.rs | verify:cargo test -q table --lib
task576 [x] goal:add diagnostic benchmark rows | scope:benches/v1_bench.rs | verify:cargo test -q --bench v1_bench
task577 [x] goal:keep first measured prefix cursor fix | scope:src/table.rs | verify:cargo bench --bench v1_bench
task578 [x] goal:finish local verification | scope:full workspace | verify:cargo clippy/test/diff checks
```

## Known Residuals

- Nonmatching prefix diagnostics report zero table probes in the benchmark
  shape because the query range is rejected before table bounds overlap.
- Cold point diagnostics show one table probe, one block metadata probe, one
  data-block read, and one cache miss per reopen/read; further cold-read work
  should look at open/read I/O or warmup behavior.
- Full clippy/test/diff verification passed after documentation was updated.

## Evidence

- The first diagnostic run before the cursor fix showed matching prefix scans
  at 8664 block metadata probes for 152 data-block reads.
- After the cursor fix, `cargo bench --bench v1_bench` reported matching prefix
  scans at 472 block metadata probes for the same 152 data-block reads.
- The release-profile benchmark row for `prefix scan table partitions
  matching` improved from the `0.1` baseline's 3551 us to 2959 us in this run.
- `cargo clippy --all-targets --all-features -- -D warnings` and
  `cargo test -q --all-targets --all-features` passed.

## Next Recommendation

- Commit the read-pruning measurement and prefix cursor fix, then choose
  whether the next target is cold table read I/O/warmup or batched point reads.
