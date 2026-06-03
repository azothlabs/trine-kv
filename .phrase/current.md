# Current Phase

## Status

Complete

## Goal

Capture the post-`0.1` performance research design before starting another
implementation slice.

## Scope

- Map concrete performance techniques and LSM-tree research to Trine's measured
  benchmark rows.
- Define a phase order for future performance work.
- Keep Trine's own API, storage contract, recovery behavior, and benchmark
  evidence as the authority.
- Record the design in `.phrase/protocol/performance-research-design.md`.

## Out Of Scope

- Rust code changes.
- Public API behavior changes.
- Storage format, MVCC, WAL, manifest, SSTable, blob, compaction, transaction,
  recovery, or browser persistence behavior changes.
- Benchmark harness changes beyond future phase proposals.
- Publishing, tagging, pushing, or release metadata changes.

## Acceptance Gate

- The design note records current benchmark evidence.
- The design maps each borrowed idea to Trine workload rows and verification
  gates.
- The design rejects high-risk or mismatched directions for now.
- `git diff --check` passes.

## Active Task Slice

```text
task573 [x] goal:capture performance design | scope:.phrase/protocol/performance-research-design.md | verify:design maps references to benchmark rows
task574 [x] goal:check design diff hygiene | scope:.phrase current/protocol/evidence | verify:git diff --check
```

## Known Residuals

- The design does not choose an implementation target yet. The next phase must
  start with read-pruning measurement before changing storage behavior.
- External papers and systems are references only; Trine must keep its own
  names, contracts, and verification gates.
- `git diff --check` passed after recording the design.

## Evidence

- The `0.1` benchmark baseline shows ordinary point reads are already decent,
  while cold table reads, prefix scans, blob maintenance, compaction, WAL
  replay, and shared-prefix workloads remain sharper performance targets.
- Performance references point to batched execution, small work units, MVCC,
  and prefix/radix indexing. LSM papers and RocksDB notes point to filter
  allocation, batched point reads, key-value separation maintenance, and
  compaction tradeoffs.

## Next Recommendation

- Run the Phase A read-pruning measurement from
  `.phrase/protocol/performance-research-design.md`, then select one narrow
  implementation target from fresh benchmark evidence.
