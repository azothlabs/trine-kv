# Current Phase

## Status

Complete

## Goal

Extend clean-WAL read-only open reuse to the async native open path.

## Scope

- Async native persistent read-only open.
- WAL replay discovery after manifest load.
- Skip WAL content reads only when every discovered WAL shard object has length
  zero.
- Reuse one directory listing for async native temporary-file repair, recovery
  checks, WAL discovery, and WAL shard length proof.
- Focused tests and benchmark evidence for the async clean-WAL path.

## Out Of Scope

- Storage format, MVCC, WAL frame format, table format, manifest format,
  compaction, transaction, blob layout, browser persistence, or release
  metadata changes.
- Weakening writer-lease protection for writable opens.
- Changing read-only public API semantics.
- Batched point-read internals or table read-path changes.
- Skipping WAL checks when any WAL shard has bytes.
- Browser and WASI persistence behavior.

## Backend Boundary Receipt

- Trine operation names: async read-only persistent open, manifest load, WAL
  shard discovery, WAL shard length proof, WAL replay, table open, recovery
  checks.
- Owned interface: existing `DbOptions::read_only` and `Db::open(_sync)` paths;
  no new public API.
- Chosen backend: existing native filesystem storage backend.
- Known backend limits: native directory listing now returns file lengths; other
  async storage backends may still return path-only entries.
- Leak-check scope: documentation and benchmark labels use Trine operation
  names only.
- Verification gate: focused async read-only/WAL recovery tests, benchmark
  smoke, full local quiet gate, diff checks, forbidden-term/source-name scans.

## Acceptance Gate

- Async read-only persistent open skips WAL content reads only when all
  discovered WAL shard files are empty.
- Async read-only open still replays non-empty WAL shards and preserves
  committed records.
- Writable open behavior is unchanged.
- Sync read-only open behavior remains unchanged from the previous phase.
- Focused checks, formatting, clippy, full tests, diff checks, and scans pass.

## Active Task Slice

```text
task607 [x] goal:reuse directory files in async native open | scope:src/db.rs | verify:cargo test -q persistent_read_only_open_async --test async_api
task608 [x] goal:add async empty-WAL recovery coverage | scope:tests/async_api.rs src/wal.rs | verify:cargo test -q persistent_read_only_open_async --test async_api
task609 [x] goal:record evidence and finish gate | scope:.phrase docs full workspace | verify:quiet full gate
```

## Known Residuals

- The previous phase proved the sync native path can avoid clean-WAL content
  reads without extra open/len requests by reusing directory entry lengths.
- Async native open now reuses one directory file list for repair, unreferenced
  file checks, WAL path discovery, and clean-WAL length proof.
- The older generic async WAL discovery helper remains available for other
  backends and tests.

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
- Async native read-only focused tests now cover both clean-WAL skip and
  non-empty WAL replay.

## Next Recommendation

- Commit async-native parity for clean-WAL read-only open. Further cold-read
  work should wait for a new measured hotspot.
