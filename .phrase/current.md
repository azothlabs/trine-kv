# Current Phase

## Status

Complete

## Goal

Reduce measured cold persistent reopen fixed cost by verifying and measuring
the existing native read-only open path, while keeping writable open safety
unchanged.

## Scope

- Persistent native read-only open path for databases that only need reads.
- Cold table read benchmark diagnostics for writer-lease and open/list costs.
- Public options/API audit for the existing explicit read-only selectors.
- Recovery and cleanup behavior audit for read-only opens.

## Out Of Scope

- Storage format, MVCC, WAL frame format, table format, manifest format,
  compaction, transactions, blob layout, browser persistence, or release
  metadata changes.
- Weakening writer-lease protection for writable opens.
- Background worker behavior changes for writable databases.
- Batched point-read internals.

## Backend Boundary Receipt

- Trine operation names: persistent open, cold point read, manifest load, WAL
  discovery, table open, safe temporary-file repair, unreferenced object check,
  writer lease acquisition.
- Owned interface: `DbOptions`/`Db::open(_sync)` plus internal native storage
  open helpers.
- Chosen backend: existing native filesystem storage backend only.
- Known backend limits: read-only opens must not acquire the writer lease or
  mutate files; writable opens must retain writer-lease and repair/cleanup
  behavior.
- Leak-check scope: no public option names should expose OS-specific backend
  mechanics; benchmark/docs should describe Trine behavior.
- Verification gate: focused recovery/persistent/open tests, benchmark smoke,
  release benchmark key rows, rustdoc/doctest if public API changes, clippy,
  full tests, diff checks, forbidden-term/source-name scans.

## Acceptance Gate

- Benchmark evidence identifies cold reopen request and latency shape.
- Native read-only open skips writer-lease acquisition only when explicitly
  selected.
- Writable open behavior and recovery safety remain unchanged.
- Read-only open cannot perform writes, background maintenance, recovery
  repair, or cleanup mutations.
- Cold table read benchmark evidence compares before/after key rows.
- Focused checks, formatting, clippy, full tests, diff checks, and scans pass.

## Active Task Slice

```text
task595 [x] goal:audit persistent open safety boundaries | scope:src/db.rs src/options.rs | verify:code inspection + cargo test -q read_only --lib
task596 [x] goal:verify existing native read-only path | scope:src/db.rs | verify:cargo test -q read_only --lib
task597 [x] goal:measure cold reopen/read-only evidence | scope:benches/v1_bench.rs docs/benchmarks | verify:cargo bench --bench v1_bench
task598 [x] goal:record phase evidence and finish gate | scope:.phrase docs full workspace | verify:clippy/test/diff scans
```

## Known Residuals

- Phase 140 measured writer-lease acquisition as the largest cold writable
  reopen fixed cost.
- Phase 142 found no small batched point-read internal change worth keeping.

## Evidence

- Phase 140 before-change discovery measured writer-lease acquisition at
  72826 us in one cold-read run and 108958 us in a later run.
- Phase 140 intentionally kept writer-lease acquisition for writable opens as
  a safety boundary.
- Current cold table read benchmark still includes writer-lease acquisition
  requests for every reopen/get operation.
- Native read-only open already skipped writer-lease acquisition and WAL writer
  creation, but the benchmark harness did not expose it as a cold-read row.
- Phase 143 added a native read-only regression test and benchmark rows.
- Release-profile benchmark evidence recorded `cold table read` at 215780 us
  and `cold table read-only` at 93188 us.
- Diagnostic rows recorded writer-lease requests at 32 for writable cold reopen
  and 0 for read-only cold reopen.

## Next Recommendation

- Commit this benchmark/test evidence. If cold-read work continues, the next
  target is shared read-only open work: manifest/table metadata reuse or WAL
  replay/read-object reduction.
