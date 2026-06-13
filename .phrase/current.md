# Current Phase

## Status

Complete

## Goal

Reduce startup, reopen, and recovery-path cost using measured diagnostics while
preserving recovery safety and storage formats.

## Scope

- Persistent open and reopen diagnostics for writable and read-only modes.
- Cold table read and WAL replay startup paths.
- Directory listing, process lock, manifest read, recovery checks, WAL replay,
  table metadata open, blob validation, and first-read costs.
- One measured startup/recovery optimization if diagnostics expose a safe
  dominant cost.

## Out Of Scope

- Storage format changes.
- Manifest, WAL, SSTable, blob, MVCC, transaction, or compaction semantics.
- Write group commit, scan optimization, cache policy, platform-io backend
  changes, publishing, tagging, pushing, or release workflow changes.

## Acceptance Gate

- Startup/recovery diagnostics classify the dominant cost before optimization.
- Any retained code change preserves recovery fail-closed behavior and storage
  formats.
- Focused recovery/open tests pass.
- Strict clippy passes.
- Single-run and grouped benchmark evidence records before/after behavior.

## Active Task Slice

```text
task783 [x] goal:add startup/recovery phase diagnostics | scope:benches/v1_bench.rs | verify:cargo bench --bench v1_bench
task784 [x] goal:fix measured startup/recovery benchmark boundary | scope:benches/v1_bench.rs | verify:TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
task785 [x] goal:record startup/recovery evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Startup/recovery benchmark rows were measuring setup work. Cold table rows
  included database population and flush; WAL replay rows included WAL test
  directory creation.
- After moving setup outside `measure`, the local 3-run summary recorded
  `WAL replay` at 852 us, `WAL replay read-only` at 581 us, `cold table read`
  at 17668 us, and `cold table read-only` at 3772 us.
- Cold writable diagnostics recorded 6996 us open wall, 1489 us first-read
  wall, and 9918 us close/drop wall across 32 iterations.
- A Unix-only writer-lease drop shortcut was rejected by the existing changed
  marker regression test and was not retained.

## Known Residuals

- Writable close/drop remains visibly more expensive than read-only close/drop,
  but the tested writer-lease owner guard is part of the fail-closed contract.

## Next Recommendation

- Return to the grouped baseline recommendation: decompose compaction and blob
  maintenance write amplification next.
