# Current Phase

## Status

Complete

## Goal

Measure and reduce range/prefix scan table and block work while preserving
MVCC visibility, range-delete behavior, prefix-filter semantics, and storage
formats.

## Scope

- Range and prefix scan diagnostics for memory, active memtable, persistent
  table, and prefix-partition workloads.
- Table candidate selection, prefix filters, block metadata probes, data block
  reads, range tombstone checks, and iterator advancement costs.
- One measured scan-path optimization if diagnostics expose a safe dominant
  cost.

## Out Of Scope

- Storage format changes.
- MVCC visibility, snapshot, range-delete, prefix-filter, table format, WAL,
  manifest, compaction, platform-io, publishing, tagging, pushing, or release
  workflow changes.
- Block-cache policy, compression codec, blob-value, write durability, and
  startup/recovery optimization.

## Acceptance Gate

- Scan diagnostics classify table candidate selection, prefix filters, block
  metadata probes, data block reads, range tombstone checks, and iterator
  advancement before optimization.
- Any retained code change preserves MVCC visibility, range-delete behavior,
  prefix-filter semantics, and storage formats.
- Focused range/prefix/table tests pass.
- Strict clippy passes.
- Single-run and grouped benchmark evidence records before/after behavior.

## Active Task Slice

```text
task789 [x] goal:add scan diagnostics | scope:benches/v1_bench.rs | verify:TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
task790 [x] goal:optimize measured scan-path bottleneck | scope:src/table.rs | verify:cargo test -q prefix --lib && cargo test -q range --lib
task791 [x] goal:record scan evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Phase 177 completed WAL dirty-lane persist optimization.
- Prefix table-partition matching scans were the clearest measured scan target:
  baseline median was 3052 us, with 472 block metadata probes and 152 data
  block reads across 128 scans.
- The retained change keeps block-level prefix filter state in the table cursor
  and records false positives when the cursor leaves a block instead of
  re-reading metadata and scanning the decoded block after load.
- After the change, prefix table-partition matching scans recorded 320 block
  metadata probes, the same 152 data block reads, and 2880 us median.

## Known Residuals

- General in-memory `prefix scan` is not on the table cursor path and showed
  run-to-run noise in this phase. The retained optimization is scoped to
  persistent table prefix scans.

## Next Recommendation

- Move next to block-cache/decode or search-policy work, whichever the latest
  grouped benchmark evidence makes most valuable.
