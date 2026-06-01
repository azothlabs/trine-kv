# Current Phase

## Status

Complete

## Goal

Close the remaining async backend boundary honestly by distinguishing portable
blocking-adapter async work from true platform async file I/O.

## Scope

- Add explicit storage capability reporting for blocking-adapter-backed storage
  tasks and platform async file I/O.
- Keep native-file backend on the bounded blocking adapter unless a real
  platform driver exists.
- Expose native-file blocking-adapter usage through database stats.
- Update the async-first storage protocol to make this distinction stable.
- Add focused tests for capability reporting and stats.

## Out Of Scope

- Adding io_uring, kqueue, Windows overlapped I/O, Tokio, or a new runtime
  dependency.
- Changing WAL, SSTable, manifest, blob, MVCC, transaction, compaction, or
  recovery formats.
- Changing public database operation names or durability semantics.

## Acceptance Gate

- Native-file backend reports `BlockingAdapter` when it uses the runtime
  blocking adapter.
- Native-file backend does not report `PlatformAsyncIo` without a real
  platform async file driver.
- `DbStats` exposes storage adapter usage and task counts.
- Existing async/storage tests pass with focused coverage for the boundary.
- Formatting, clippy, full tests, `git diff --check`, and forbidden-term scan
  pass.

## Active Task Slice

```text
task446 [x] goal:commit completed write-path tail phase | scope:git | verify:commit 6fa6461
task447 [x] goal:add honest storage async capabilities | scope:src/storage.rs | verify:storage tests
task448 [x] goal:surface storage adapter stats | scope:src/stats.rs src/db.rs | verify:db stats test
task449 [x] goal:update async storage protocol and evidence | scope:.phrase | verify:manual + scans
task450 [x] goal:run final verification | scope:repo | verify:full gate
```

## Known Blockers

- True platform async file I/O is not available in the current dependency and
  runtime boundary. Implementing it should be a later backend-specific phase
  with a platform driver and fallback rules.

## Evidence

- Phase 103 completed the local async/write-path tails and left only the native
  file I/O boundary.
- The accepted async-first storage protocol allows native files to use a
  bounded blocking pool, but requires capabilities and diagnostics to be honest.
- Current Cargo dependencies do not include an OS async file I/O driver.
- Native-file capabilities now distinguish `BlockingAdapter` from
  `PlatformAsyncIo`.
- `DbStats` now exposes whether the storage backend uses the blocking adapter
  or platform async file I/O, plus adapter and inline task counts.
- Verification passed: `cargo fmt --check`, `cargo test storage --lib`,
  `cargo test --test async_api`,
  `cargo clippy --all-targets --all-features -- -D warnings`, and
  `cargo test --all-targets --all-features`.

## Next Recommendation

- Start platform-native async file I/O only after choosing a concrete driver and
  supported-platform matrix.
