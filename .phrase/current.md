# Current Phase

## Status

In Progress

## Goal

Investigate and fix the current `WAL replay` benchmark slowdown without
changing storage formats or weakening the single-writer guarantee.

## Scope

- `benches/v1_bench.rs` WAL replay diagnostics.
- Native-file writer lease acquisition on the default backend.
- Platform-io thread-pool writer lease acquisition.
- Native platform-io writer lease acquisition.
- Benchmark evidence for before/after behavior.

## Out Of Scope

- Storage format changes.
- WAL frame format changes.
- Manifest semantics.
- Broad point-read or batch-read tuning.
- Publishing, tagging, pushing, or release workflow changes.

## Acceptance Gate

- `WAL replay` is decomposed into writable and read-only reopen diagnostics.
- The dominant current cost is classified before optimizing.
- Writer lease changes keep existing fail-closed tests passing.
- Platform-io feature variants keep writer lease tests passing.
- Strict clippy passes.
- Release-profile benchmark evidence shows whether the selected change improved
  the measured slowdown.

## Active Task Slice

```text
task774 [x] goal:add WAL replay reopen diagnostics | scope:benches/v1_bench.rs | verify:cargo bench --bench v1_bench
task775 [x] goal:remove unnecessary writer-lease owner sync | scope:src/storage.rs src/io/platform_threadpool.rs src/io/platform_backend.rs | verify:writer_lease tests
task776 [x] goal:record benchmark evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Writable WAL replay reopen was dominated by writer-lease acquisition, not WAL
  read/decode/replay.
- Before the change, 32 writable reopen diagnostics spent 88076 us in writer
  lease acquisition and 106515 us wall-clock total.
- After the change, 32 writable reopen diagnostics spent 1888 us in writer
  lease acquisition and 18706 us wall-clock total.
- The `WAL replay` row moved from 35430 us to 31693 us in the local
  release-profile run.

## Known Residuals

- `localized batched point read persistent` remains a separate observation from
  the `0.4.0` release benchmark check.
- A full multi-run median after this change would reduce benchmark noise before
  making a broader performance claim.

## Next Recommendation

- Run one more release-profile benchmark if we want a three-run median for the
  final write-up, then commit the writer-lease performance fix.
