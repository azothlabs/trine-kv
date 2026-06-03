# Current Phase

## Status

Complete

## Goal

Reduce cold point-read table-open I/O for small persistent tables without
changing lazy data-block read behavior.

## Scope

- Cold-read diagnostic rows for storage operation request counts and latency
  totals.
- Sync table-open behavior for small table files.
- Benchmark evidence in `docs/benchmarks/v1-cold-table-open-read.md`.

## Out Of Scope

- Rust code changes.
- Public API behavior changes.
- Storage format, MVCC, WAL, manifest, SSTable, blob, compaction, transaction,
  recovery, or browser persistence behavior changes.
- Data-block eager loading for large table files.
- Publishing, tagging, pushing, or release metadata changes.

## Acceptance Gate

- Cold-read diagnostics expose storage operation request counts.
- Small sync table open reduces positioned owned read requests for the measured
  cold point-read workload.
- Persistent tables still keep data blocks lazy after sync open.
- Focused table and benchmark checks pass.
- Formatting, clippy, full tests, and diff checks pass.

## Active Task Slice

```text
task579 [x] goal:add cold storage diagnostics | scope:benches/v1_bench.rs | verify:cargo test -q --bench v1_bench
task580 [x] goal:buffer small table open metadata | scope:src/table.rs | verify:cargo test -q table --lib
task581 [x] goal:record cold-read evidence | scope:docs/benchmarks/v1-cold-table-open-read.md .phrase | verify:cargo bench --bench v1_bench
task582 [x] goal:finish local verification | scope:full workspace | verify:cargo clippy/test/diff checks
```

## Known Residuals

- Small sync table open now decodes metadata from a temporary whole-file buffer
  only when the table file is at or below 256 KiB.
- The opened table still keeps its native file handle and leaves data blocks
  lazy, so large-table behavior and block-cache read behavior remain bounded.
- Full clippy/test/diff verification passed after documentation was updated.

## Evidence

- Before the table-open change, 32 reopen/get operations performed 288
  positioned owned reads.
- After the table-open change, the same diagnostic workload performed 96
  positioned owned reads.
- Release-profile `cold table read` was 174933 us in the first after-change run
  and 167430 us in the second after-change run. The `0.1` baseline was 183876
  us.
- `cargo test -q table --lib` and `cargo test -q --bench v1_bench` passed.
- `cargo clippy --all-targets --all-features -- -D warnings` and
  `cargo test -q --all-targets --all-features` passed with output redirected
  to temporary files.

## Next Recommendation

- Commit the cold table-open optimization, then choose whether the next target
  is current-manifest/open overhead or batched point reads.
