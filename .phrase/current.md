# Current Phase

## Status

Complete

## Goal

Investigate the `localized batched point read persistent` benchmark follow-up
and keep only changes supported by local benchmark evidence.

## Scope

- `get_many` batch organization on the LSM point-read path.
- `LsmVersion` table selection for borrowed key views.
- Localized point-read benchmark diagnostics for batch sizes 4, 8, 16, and 32.
- Benchmark evidence for the current localized sequential and batched rows.

## Out Of Scope

- Storage format changes.
- WAL, manifest, compaction, or MVCC behavior changes.
- Broad point-read tuning outside the localized `get_many` path.
- Publishing, tagging, pushing, or release workflow changes.

## Acceptance Gate

- Diagnostics show whether localized batching still shares SSTable block work.
- The chosen code change removes measured fixed cost without changing public
  API behavior.
- Existing `get_many` tests pass.
- Strict clippy passes.
- Release-profile benchmark evidence records the current sequential and batched
  localized rows.

## Active Task Slice

```text
task777 [x] goal:add localized point-read diagnostics | scope:benches/v1_bench.rs | verify:cargo bench --bench v1_bench
task778 [x] goal:avoid owned key copies in point-read batch state | scope:src/lsm/read.rs src/lsm/version.rs | verify:cargo test -q get_many --lib
task779 [x] goal:record localized point-read evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Localized batch size 16 still shares table block work: diagnostic metadata
  probes and data block reads dropped from 2048 sequential operations to 134
  batched operations.
- After borrowing keys in `PointReadBatch`, the local release-profile run
  recorded `localized sequential point batch persistent` at 1529 us and
  `localized batched point read persistent` at 1399 us.
- Batch size 4 remains slower because it repeatedly enters the small-batch
  single-key path and still pays repeated batch-call overhead.

## Known Residuals

- Local run-to-run noise is still visible at this microsecond scale, so the
  correct claim is that the severe localized batch regression is not reproduced
  after the key-copy fix, not that every run will beat sequential reads.

## Next Recommendation

- Continue performance work from fresh benchmark evidence rather than assuming
  localized batching is the next bottleneck.
