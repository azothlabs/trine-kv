# Current Phase

## Status

Complete

## Goal

Complete the native async flush path so Linux `platform-io` public `flush()`
awaits Trine storage completions instead of running the whole flush through the
bounded sync adapter.

## Scope

- Keep `flush_sync` and native fallback async flush behavior unchanged.
- Route public native async `flush()` through an async flush path only when the
  native storage backend advertises `PlatformAsyncIo`.
- Write flushed table files through Trine async storage traits.
- Publish manifest updates through prepared manifest publish without holding a
  std mutex or publish barrier across await.
- Sync the database directory and rewrite WAL through async storage/durability
  operations where the selected backend supports them.
- Verify Linux `platform-io` counters in Docker/Orbstack.

## Backend Boundary Receipt

- Trine operation names: public async flush, freeze public flush target, flush
  table write, manifest publish, directory sync after rename, WAL replay-floor
  rewrite, table install, flush stats.
- Owned interface: `Db::flush`, `ManifestStore` prepared publish,
  `DurabilitySubstrate`, `WalFrontDoor`, `NativeFileBackend`,
  `StorageCapability::PlatformAsyncIo`, and `DbStats` storage task counters.
- Chosen backend: existing Trine `PlatformIoDriver` for table object writes,
  native manifest publish, directory sync, and WAL rewrite when the backend
  advertises `PlatformAsyncIo`.
- Known backend limits: native fallback targets keep the bounded adapter;
  `run_maintenance_with_budget`, compaction, cleanup, and background
  maintenance remain later phases unless directly needed for public `flush()`.
- Leak-check scope: the public flush future must not hold std locks or the
  publish barrier across await; native manifest publish uses a small runtime
  blocking task to share the existing manifest/publish locks with sync
  mutators while waiting for platform storage completion. Manifest state
  advances only after durable publish succeeds; table files written before a
  failed publish are cleaned up.
- Verification gate: focused async flush tests with and without `platform-io`,
  Linux Docker platform test, formatting, clippy, rustdoc, full tests, and diff
  checks.

## Out Of Scope

- Rewriting compaction or background maintenance.
- Reworking manifest format, WAL format, SSTable format, MVCC visibility, or
  transaction conflict rules.
- Claiming new true async support on fallback targets.
- Publishing, tagging, pushing, or creating a GitHub release.

## Acceptance Gate

- Public native async `flush()` uses the async flush path only when
  `NativeFileBackend` supports `PlatformAsyncIo`.
- Linux `platform-io` public `flush()` does not spawn the whole flush through
  the bounded sync adapter.
- WAL replay remains correct after async flush.
- Native fallback async flush retains existing bounded-adapter behavior.
- `cargo fmt --check`, focused tests with and without `platform-io`, Linux
  Docker platform test, `cargo clippy -q --all-features`,
  `cargo rustdoc --all-features -- -D warnings`, `cargo test -q`, and
  `git diff --check` pass.

## Active Task Slice

```text
task670 [x] goal:select flush as next async-storage phase | scope:evidence roadmap current | verify:phase brief
task671 [x] goal:prepare manifest publish for native async flush | scope:src/manifest.rs src/db.rs | verify:focused flush tests
task672 [x] goal:route platform async public flush through async storage | scope:src/db.rs src/substrate.rs src/wal.rs | verify:Linux platform counters
task673 [x] goal:verify and record native async flush evidence | scope:tests docs phrase | verify:full gate
```

## Evidence

- Phase 151 completed native async writes and left flush, compaction, manifest
  publish, and directory work as residual async-storage phases.
- Existing native public `flush()` calls `run_native_blocking_task(|db|
  db.flush_sync())` for every persistent native backend.
- Browser flush already uses a prepared manifest publish shape that avoids
  holding std locks across await.
- `ManifestStore` prepared publish is now available to native async flush, so
  manifest bytes can be published through the async storage backend before the
  prepared state is installed.
- Linux Docker/Orbstack with `--security-opt seccomp=unconfined` passed
  `platform_io_async_flush_awaits_storage_without_whole_flush_adapter`.
- The Linux platform flush test allows one sync-adapter submitted-task delta
  because observing `DbStats` can perform a native table-size lookup; the flush
  storage write, manifest publish, directory sync, and WAL rewrite counters all
  advance through storage operation counters.
- `cargo test -q`, `cargo test -q --features platform-io`,
  `cargo clippy -q --all-features`,
  `cargo rustdoc --all-features -- -D warnings`, `cargo fmt --check`, and
  focused async flush/WAL tests passed.

## Known Residuals

- Compaction, background maintenance, cleanup, close, and native cooperative
  maintenance still use existing fallback paths after this phase.

## Next Recommendation

- After public native async flush passes, use fresh counters to choose between
  async compaction and async maintenance/cleanup.
