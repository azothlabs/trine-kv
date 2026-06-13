# Current Phase

## Status

Complete

## Goal

Make `RuntimeMode::PlatformIo` route native storage through Trine's platform I/O
driver on every supported target while keeping `PlatformAsyncIo` honest.

## Scope

- Split runtime capability for platform driver routing from true platform async
  I/O capability.
- Start `PlatformIoDriver` whenever the `platform-io` feature is enabled and
  `RuntimeMode::PlatformIo` is selected.
- Report platform-driver routing separately in `DbStats`.
- Keep non-Linux targets fallback-classified unless a current Trine storage
  operation uses a real OS async primitive.
- Update ADR/docs/tests to reflect the routing/fallback distinction.

## Backend Boundary Receipt

- Trine operation names: runtime capability selection, native storage driver
  routing, storage capability reporting, platform task accounting, `DbStats`
  reporting.
- Owned interface: `RuntimeCapabilities`, `NativeFileBackend`,
  `NativeFileStorageStats`, `DbStats`, and `StorageCapability`.
- Chosen backend: existing Trine `PlatformIoDriver` behind the `platform-io`
  feature.
- Known backend limits: Linux has true async classes in the current matrix;
  macOS/BSD/Windows remain fallback-classified for current Trine operations;
  directory listing remains a blocking fallback.
- Leak-check scope: user-facing docs and stats must not imply non-Linux
  fallback work is true platform async I/O.
- Verification gate: focused runtime/storage tests with and without
  `platform-io`, formatting, clippy, and diff checks.

## Out Of Scope

- Rewriting the native async commit path.
- Splitting flush or compaction into async storage phases.
- Changing WAL, SSTable, manifest, MVCC, transaction, or durability semantics.
- Adding a new platform backend or claiming new true async support on non-Linux.
- Publishing, tagging, pushing, or creating a GitHub release.

## Acceptance Gate

- `RuntimeOptions::platform_io().capabilities()` exposes platform-driver routing
  separately from true platform async I/O.
- `NativeFileBackend::with_runtime(RuntimeOptions::platform_io())` creates the
  platform driver whenever the feature is enabled.
- `DbStats::storage_uses_platform_io_driver` reports platform driver routing.
- Non-Linux `platform-io` tests expect platform fallback counters instead of
  Trine bounded sync-adapter tasks.
- ADR and usage docs describe the distinction between platform routing,
  fallback work, and `PlatformAsyncIo`.
- `cargo fmt --check`, focused tests with `--features platform-io`, focused
  tests without the feature, and `git diff --check` pass.

## Active Task Slice

```text
task650 [x] goal:record platform routing boundary | scope:.phrase/current .phrase/adr docs/usage | verify:docs scan
task651 [x] goal:split platform routing from true async capability | scope:src/runtime.rs src/storage.rs src/stats.rs src/db.rs | verify:focused tests
task652 [x] goal:verify platform routing phase | scope:runtime/storage/docs | verify:fmt + focused tests + diff check
```

## Evidence

- Previous runtime capability used `PlatformAsyncIo` as the creation gate for
  `PlatformIoDriver`, so non-Linux `RuntimeMode::PlatformIo` silently fell back
  to the bounded sync adapter.
- ADR 0002 already classifies platform operations as true async, backend
  fallback, or blocking fallback, but its old consequence blocked all-fallback
  targets from starting the platform driver.
- `cargo test -q --features platform-io` passed on the local non-Linux target
  with the non-Linux fallback test expecting platform-driver fallback counters.
- `cargo test -q`, `cargo clippy -q --all-features`,
  `cargo rustdoc --all-features -- -D warnings`, `cargo fmt --check`, and
  `git diff --check` passed.

## Known Residuals

- Native async write, flush, and compaction paths still need separate phases.

## Next Recommendation

- After this phase passes, start a native async write-path phase that makes
  Linux `platform-io` writes await storage completions instead of running the
  whole commit through the bounded sync adapter.
