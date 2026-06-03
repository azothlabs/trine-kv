# Current Phase

## Status

Complete

## Goal

Reduce read-only cold open work when WAL shards are provably empty after a clean
flush.

## Scope

- Native persistent read-only open.
- WAL replay discovery after manifest load.
- Skip WAL content reads only when every discovered WAL shard object has length
  zero.
- Focused tests and benchmark evidence for the clean-WAL path.

## Out Of Scope

- Storage format, MVCC, WAL frame format, table format, manifest format,
  compaction, transaction, blob layout, browser persistence, or release
  metadata changes.
- Weakening writer-lease protection for writable opens.
- Changing read-only public API semantics.
- Batched point-read internals or table read-path changes.
- Skipping WAL checks when any WAL shard has bytes.

## Backend Boundary Receipt

- Trine operation names: read-only persistent open, manifest load, WAL shard
  discovery, WAL shard length proof, WAL replay, table open.
- Owned interface: existing `DbOptions::read_only` and `Db::open(_sync)` paths;
  no new public API.
- Chosen backend: existing native filesystem storage backend.
- Known backend limits: directory listing returns paths, not sizes, so the proof
  uses existing read-object length calls.
- Leak-check scope: documentation and benchmark labels use Trine operation
  names only.
- Verification gate: focused read-only/WAL recovery tests, benchmark smoke,
  full local quiet gate, diff checks, forbidden-term/source-name scans.

## Acceptance Gate

- Read-only persistent open skips WAL content reads only when all discovered WAL
  shard files are empty.
- Read-only open still replays non-empty WAL shards and preserves committed
  records.
- Writable open behavior is unchanged.
- Release benchmark evidence records whether the optimization lowers read-only
  cold open whole-object reads or latency.
- Focused checks, formatting, clippy, full tests, diff checks, and scans pass.

## Active Task Slice

```text
task603 [x] goal:add empty-WAL proof helper | scope:src/wal.rs | verify:cargo test -q read_only --lib
task604 [x] goal:use helper in read-only persistent open | scope:src/db.rs | verify:cargo test -q read_only --lib
task605 [x] goal:measure clean-WAL read-only cold open | scope:benches/v1_bench.rs docs/benchmarks | verify:cargo bench --bench v1_bench
task606 [x] goal:record evidence and finish gate | scope:.phrase docs full workspace | verify:quiet full gate
```

## Known Residuals

- Directory entries now carry file length for the native filesystem path, so
  clean-WAL read-only open avoids WAL content reads without extra open/len
  requests in the measured sync native path.
- Async native open still discovers WAL paths through its separate async
  directory-list path and can reuse this strategy in a later slice.

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
- Phase 144 committed the open-vs-first-read diagnostic split as `79079a9`.
- Phase 145 release-profile benchmark recorded `cold table read-only` at 91081
  us and read-only open phase whole-object reads at zero.

## Next Recommendation

- Commit the clean-WAL read-only open change. If cold-read work continues, move
  next to manifest/table open metadata or first-read table data-block work.
