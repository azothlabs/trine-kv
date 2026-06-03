# Current Phase

## Status

Complete

## Goal

Break down read-only cold reopen cost into open-time and first-read work so the
next optimization target is chosen from evidence.

## Scope

- Benchmark diagnostics for cold writable and read-only reopen.
- Split open-time storage/read-path counters from the first point-read delta.
- Phase evidence and benchmark documentation for the next target decision.

## Out Of Scope

- Storage format, MVCC, WAL frame format, table format, manifest format,
  compaction, transaction, blob layout, browser persistence, or release
  metadata changes.
- Weakening writer-lease protection for writable opens.
- Changing read-only public API semantics.
- Batched point-read internals.

## Backend Boundary Receipt

- Trine operation names: persistent open, read-only persistent open, cold point
  read, manifest load, WAL discovery/replay, table open, lazy data-block read,
  directory listing, writer lease acquisition.
- Owned interface: benchmark harness diagnostics plus existing `DbOptions` and
  `Db::open(_sync)` paths.
- Chosen backend: existing native filesystem storage backend only.
- Known backend limits: diagnostics must not change read/write/recovery
  behavior; read-only opens still cannot mutate files.
- Leak-check scope: benchmark/documentation names describe Trine operations,
  not OS-specific backend implementation details.
- Verification gate: benchmark smoke, release benchmark key rows, focused
  read-only tests, clippy, full tests, diff checks, forbidden-term/source-name
  scans.

## Acceptance Gate

- Benchmark rows distinguish open-time counters from first-read delta counters.
- Existing cold table read and read-only benchmark rows remain comparable.
- No public API or storage behavior changes are required for the diagnostic
  phase.
- Evidence recommends one next target: manifest/table metadata, WAL replay, or
  lazy point-read data-block work.
- Focused checks, formatting, clippy, full tests, diff checks, and scans pass.

## Active Task Slice

```text
task599 [x] goal:add open-vs-first-read cold diagnostics | scope:benches/v1_bench.rs | verify:cargo test -q --bench v1_bench
task600 [x] goal:measure release benchmark key rows | scope:benches/v1_bench.rs docs/benchmarks | verify:cargo bench --bench v1_bench
task601 [x] goal:record phase evidence and next target | scope:.phrase docs README.md | verify:git diff --check
task602 [x] goal:finish local gate | scope:full workspace | verify:clippy/test/diff scans
```

## Known Residuals

- Phase 143 showed read-only cold reopen avoids writer-lease acquisition and is
  much faster than writable cold reopen in the measured run.
- Remaining read-only cold cost includes shared open work and one lazy
  point-read data-block load.

## Evidence

- Phase 143 release-profile benchmark recorded `cold table read` at 215780 us
  and `cold table read-only` at 93188 us.
- Phase 143 diagnostic rows recorded writer-lease requests at 32 for writable
  cold reopen and 0 for read-only cold reopen.
- Phase 143 did not split open-time costs from the first point-read delta.
- Phase 144 split diagnostics recorded read-only open phase whole-object reads
  at 128 requests across 32 opens, with 32 current-manifest requests.
- Read-only first read phase recorded 64 positioned owned reads, 32 point data
  block reads, and zero whole-object reads.
- Code inspection maps the non-manifest open-phase whole-object reads to WAL
  shard reads.

## Next Recommendation

- Commit the split diagnostics. If cold-read work continues, start a clean-WAL
  read-only open phase that avoids WAL shard reads only when manifest/WAL state
  proves there are no replayable records.
