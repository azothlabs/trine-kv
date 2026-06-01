# Current Phase

## Status

In Progress

## Goal

Close the async-first contract gap before release claims by making the primary
persistent paths enter Trine through async storage operations, then narrowing or
removing compatibility wrappers that still call blocking APIs.

## Scope

- Phase 130: async contract closure.
- Native persistent `Db::open_async` and recovery through async storage-trait
  operations.
- Public async compatibility audit for reads, scans, maintenance, close, and
  WASI host persistence.
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
task547 [ ] goal:classify remaining async wrappers and convert the next wait boundary | scope:src/db.rs tests | verify:focused async tests plus native/WASM gates
```

## Known Residuals

- `Db::open_async` for WASI host persistence still delegates to blocking
  `Db::open`.
- Native persistent async open still uses synchronous path metadata checks and
  synchronous cleanup/background-worker startup after recovery has loaded.
- `get_async`, snapshot reads, range/prefix scans, native `persist_async`,
  native `flush_async`, native compaction, native maintenance, and
  `close_async` still call blocking public methods on native targets.
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
- Verified with native clippy/tests and browser/WASI target checks.

## Next Recommendation

- Convert the next highest-impact native async wrapper: point/range reads if
  the read path can wait on table/blob I/O, or maintenance/flush if foreground
  write-pressure behavior needs the tighter async guarantee first.
