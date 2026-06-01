# Current Phase

## Status

Complete

## Goal

Close the async-first contract gap before release claims by making the primary
persistent paths enter Trine through async storage operations, then narrowing or
removing compatibility wrappers that still call blocking APIs.

## Scope

- Phase 130: async contract closure.
- Native persistent `Db::open_async` and recovery through async storage-trait
  operations.
- Native persistent async point reads, scans, lazy value reads, and maintenance
  wrappers.
- Public async compatibility audit for WASI host persistence and deeper
  maintenance/WAL engine ownership.
- Blocking native APIs remain available only as adapter surface.

## Out Of Scope

- Changing table, WAL, manifest, MVCC, transaction, compaction, or blob storage
  formats.
- Reworking browser persistence behavior beyond keeping target checks green.
- Publishing, tagging, or package release work.
- Performance tuning unless async conversion exposes a blocking correctness
  issue.

## Acceptance Gate

- Native persistent `Db::open_async` does not delegate to blocking `Db::open`.
- Native persistent async open uses async storage-trait operations for directory
  creation, writer lease, safe temporary repair, manifest open/create, table
  loading, recovery checks, and WAL recovery reads.
- Persistent async open rejects runtimes that cannot provide wait support
  instead of running blocking storage work inline.
- Remaining async methods that can wait either use primary async paths or have
  recorded blockers with tests proving the current behavior.
- Native, WASI, and browser compile/clippy gates stay green.

## Active Task Slice

```text
task546 [x] goal:route native persistent open_async through async storage path | scope:src/db.rs src/recovery.rs tests/async_api.rs | verify:cargo test --test async_api; cargo clippy --all-targets --all-features -- -D warnings; WASM target checks
task547 [x] goal:convert native point/range async read wait boundaries | scope:src/db.rs src/bucket.rs src/transaction.rs src/iterator.rs src/lsm src/table.rs src/point_value.rs tests/async_api.rs | verify:cargo test --test async_api; cargo test --all-targets --all-features; WASM target checks
task548 [x] goal:route native async maintenance wrappers through runtime blocking task boundary | scope:src/db.rs tests/async_api.rs | verify:cargo test --test async_api; cargo clippy --all-targets --all-features -- -D warnings
task549 [x] goal:route WASI host persistent open_async away from blocking Db::open | scope:src/db.rs README.md docs/usage.md docs/durability.md CHANGELOG.md | verify:cargo test --lib; cargo test --all-targets --all-features; WASM target checks
task550 [x] goal:classify primary async maintenance/WAL ownership boundary | scope:src/db.rs src/db/commit.rs src/wal.rs src/manifest.rs .phrase | verify:code audit plus full native/WASM gates
```

## Known Residuals

- Native persistent async open still uses synchronous path metadata checks and
  synchronous cleanup/background-worker startup after recovery has loaded.
- Native `persist_async`, `flush_async`, compaction, maintenance, and
  `close_async` now leave the caller thread through runtime blocking tasks, but
  they still reuse synchronous engine internals rather than a primary async
  maintenance/WAL implementation. This is recorded as follow-up hardening, not
  a Phase 130 release blocker.
- Native `persist_async` still reaches the synchronous WAL front door inside
  the runtime task boundary.
- The browser runtime fixture remains absent; browser coverage is still target
  compilation and shared native tests.

## Evidence

- Native persistent `Db::open_async` now creates directories, acquires writer
  lease, repairs safe temporary files, opens/creates manifest state, loads
  tables, runs recovery checks, and reads WAL recovery streams through async
  storage-trait calls before building the same database state as blocking open.
- `Db::open_persistent_async` and `Db::open_read_only_async` now route through
  `Db::open_async`.
- Persistent async open rejects inline runtime options with a typed unsupported
  error before entering native storage waits.
- Focused async tests cover WAL replay through persistent async reopen and
  inline runtime rejection.
- Native persistent async point reads, snapshot reads, bucket reads,
  transaction reads, range scans, prefix scans, and lazy blob value reads now
  await async table/blob storage helpers instead of delegating to blocking
  public reads.
- WASI host persistent `Db::open_async` now enters the same async
  storage-trait persistent open path on WASI targets instead of delegating to
  blocking `Db::open`; non-WASI targets still return `UnsupportedBackend`.
- Native async maintenance wrappers now run their synchronous engine work on
  runtime blocking tasks when a persistent native backend is present.
- Native async write futures already submit accepted writes to the runtime
  blocking task pool when the runtime supports it; this preserves the
  unpolled-future no-side-effect rule and the polled-future owns-completion
  rule covered by existing tests.
- Focused async tests cover persistent lazy blob reads and native maintenance
  wrapper task submission.
- Verified with native clippy/tests and browser/WASI target checks, including
  full `cargo test --all-targets --all-features`.

## Next Recommendation

- Treat a primary async maintenance/WAL engine and an in-browser persistence
  fixture as follow-up hardening, while keeping the current release claim
  scoped to public async entry points, async storage reads/open, browser async
  persistence, and native runtime task boundaries.
