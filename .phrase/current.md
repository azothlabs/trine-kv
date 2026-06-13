# Current Phase

## Status

Complete

## Goal

Reshape the internal platform I/O driver boundary so each platform backend owns
operation submission and reports the capability class for each Trine storage
operation. KV engine code should use Trine operations and diagnostics, not
Linux, Windows, macOS, BSD/other Unix, or fallback mechanics directly.

## Scope

- Keep existing Linux platform-io write and flush behavior intact.
- Expand platform I/O task classes to match Phase 153:
  `TruePlatformAsync`, `PlatformNativeAsyncButPartial`,
  `PlatformManagedFallback`, `BlockingFallback`, and `Unsupported`.
- Add operation-level class reporting for length lookup, random read,
  whole-object read, temporary write plus rename publish, append open, append,
  persist/fsync, WAL rewrite, delete, directory create, directory sync,
  directory listing, and writer lease.
- Preserve existing aggregate `DbStats` fields for compatibility while adding
  the per-operation platform I/O table.
- Keep Windows, macOS, and BSD backend implementation for later phases; this
  phase only creates the trait/statistics shape and honest current
  classifications.

## Backend Boundary Receipt

- Trine operation names: length lookup, owned random read, whole-object read,
  temporary write plus rename publish, append open, append, persist/fsync, WAL
  rewrite, delete, directory create, directory sync, directory listing, and
  writer lease.
- Owned interface: `PlatformIoBackendMatrix`, `PlatformIoOperation`,
  `PlatformIoTaskClass`, `PlatformIoDriver`, `NativeFileStorageStats`,
  `DbStats`, and native storage metrics.
- Chosen backend: existing platform-specific platform-io matrix modules.
  Linux remains true platform async where current evidence supports it.
  Windows reports partial native async until complete Trine operations are
  implemented. macOS/BSD/other Unix and generic fallback report managed fallback
  unless an operation has explicit blocking fallback or unsupported status.
- Known backend limits: no new Windows/macOS/BSD async backend is implemented
  in this phase. `PlatformAsyncIo` remains a coarse compatibility capability;
  operation-level stats are the new implementation boundary for planning.
- Leak-check scope: metrics must be recorded outside backend internals without
  adding OS-specific branches to KV engine code, and no std lock may be held
  across await as part of the new accounting.
- Verification gate: formatting, default and `platform-io` checks, focused
  platform matrix/storage tests, clippy, rustdoc, full tests if feasible, and
  diff checks.

## Out Of Scope

- Implementing Windows, macOS, or BSD true async file backends.
- Rewriting compaction, maintenance, cleanup, close, or cooperative
  maintenance.
- Changing manifest, WAL, SSTable, MVCC, transaction, or recovery formats.
- Making platform I/O the default runtime.
- Publishing, tagging, pushing, or creating a GitHub release.

## Acceptance Gate

- `RuntimeOptions::platform_io()` still enters the platform driver path on
  targets where the driver can be constructed.
- Platform class names in code match the Phase 153 contract.
- Linux-specific, Windows partial, Unix managed fallback, and unsupported
  fallback classifications are explicit in backend matrix modules.
- Backend stats report operation-level class counters while preserving existing
  aggregate counters.
- Existing Linux platform write and flush behavior remains verified.
- Documentation explains the new per-operation diagnostics.

## Active Task Slice

```text
task679 [x] goal:start Phase 154 brief | scope:current roadmap | verify:phase brief
task680 [x] goal:align platform task classes with Phase 153 | scope:src/io.rs src/io/platform_backend | verify:platform matrix test
task681 [x] goal:add operation-level platform I/O stats | scope:src/stats.rs src/storage.rs src/db.rs | verify:focused storage stats tests
task682 [x] goal:verify Linux write/flush behavior remains intact | scope:tests/async_api.rs storage tests | verify:focused platform-io tests
task683 [x] goal:record Phase 154 evidence and next backend phase | scope:evidence roadmap current | verify:docs and diff checks
```

## Evidence

- Phase 153 completed the cross-platform platform-io contract and operation
  table.
- The code already had `PlatformIoBackendMatrix`, so Phase 154 can be completed
  as a focused driver/statistics cleanup rather than an engine rewrite.
- Current changes add the five documented class names, add `WalRewrite` as its
  own platform operation, and expose per-operation class counters through
  `DbStats`.
- `cargo check -q`, `cargo check -q --features platform-io`,
  `cargo test -q platform_backend_matrix_matches_target_family --features
  platform-io`, `cargo test -q
  platform_io_native_file_management_ops_use_platform_driver --features
  platform-io`, focused async write/flush tests, `cargo clippy -q
  --all-features`, `cargo rustdoc --all-features -- -D warnings`,
  `cargo test -q`, `cargo test -q --features platform-io`, and Linux Docker
  `cargo test -q --features platform-io --test async_api` passed.

## Known Residuals

- Full Windows backend implementation remains Phase 155.
- macOS backend implementation or audit remains Phase 156.
- BSD/other Unix backend implementation or audit remains Phase 157.
- Engine compaction, maintenance, cleanup, close, and cooperative maintenance
  still need revalidation after platform-io backend work progresses.

## Next Recommendation

- Start Phase 155: Windows Platform Backend, using the operation-level class
  table as the implementation contract.
