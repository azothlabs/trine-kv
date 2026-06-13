# Current Phase

## Status

Complete

## Goal

Advance the Windows platform-io backend from a vague partial bucket to an
audited Windows-native async backend plan and implementation slice. Windows
must use native overlapped/IOCP file primitives where a complete Trine
operation can honestly support them, while mixed operations stay classified as
partial until every required step is covered.

## Scope

- Audit the currently selected `compio` Windows backend behavior used by
  Trine's platform-io layer.
- Keep KV engine code on Trine operations; Windows details stay below
  `platform-io`.
- Split Windows classifications by actual evidence:
  overlapped/IOCP read/write steps versus blocking open, metadata, sync,
  rename, delete, create-directory, and listing steps.
- Add Windows-target compile and matrix tests that lock the current Windows
  backend classification.
- Update ADR/protocol/evidence if the audit changes any Phase 153 assumptions.

## Backend Boundary Receipt

- Trine operation names: length lookup, owned random read, whole-object read,
  temporary write plus rename publish, append open, append, persist/fsync, WAL
  rewrite, delete, directory create, directory sync, directory listing, and
  writer lease.
- Owned interface: `src/io/platform_backend/windows_backend.rs`,
  `PlatformIoBackendMatrix`, `PlatformIoTaskClass`, `PlatformIoOperation`,
  `PlatformIoDriver`, and platform I/O stats.
- Chosen backend: existing `compio` Windows path using `FILE_FLAG_OVERLAPPED`
  file handles and IOCP-backed `ReadFile` / `WriteFile` for positioned
  read/write primitives.
- Known backend limits: `compio-fs 0.7.0` opens files with
  `FILE_FLAG_OVERLAPPED`, but file open, metadata, rename, delete,
  create-directory, and some sync operations still use blocking or helper
  paths. A Trine operation that includes those steps remains partial.
- Leak-check scope: no Windows-specific branching in KV engine code; no std
  lock is introduced across await; stats must keep reporting operation class
  honestly.
- Verification gate: Windows target `cargo check`, platform matrix tests,
  formatting, clippy/rustdoc where relevant, and diff checks. Real Windows
  runtime tests are recorded as required evidence before claiming stronger
  true async coverage beyond compile/audit.

## Out Of Scope

- Implementing macOS or BSD backends.
- Rewriting compaction, maintenance, cleanup, close, or cooperative
  maintenance.
- Changing manifest, WAL, SSTable, MVCC, transaction, or recovery formats.
- Claiming a complete Windows Trine operation is true platform async while any
  required step remains blocking or fallback-classified.
- Publishing, tagging, pushing, or creating a GitHub release.

## Acceptance Gate

- Windows backend classification is backed by source audit and target compile.
- Windows read/write primitives are tied to overlapped/IOCP evidence.
- Mixed Windows Trine operations remain `PlatformNativeAsyncButPartial` unless
  every required step is proven true platform async.
- Directory listing remains `BlockingFallback`.
- Evidence records what still requires a real Windows runtime test.
- Phase completion is committed before starting the next phase.

## Active Task Slice

```text
task684 [x] goal:start Windows backend phase | scope:current roadmap | verify:phase brief
task685 [x] goal:audit selected compio Windows file path | scope:cargo registry source evidence | verify:audit notes
task686 [x] goal:lock Windows backend classifications | scope:src/io/platform_backend/windows_backend.rs src/io.rs | verify:windows target check
task687 [x] goal:update docs/evidence for Windows backend limits | scope:ADR protocol evidence | verify:docs diff
task688 [x] goal:verify and commit Phase 155 | scope:tests docs git | verify:target check plus local gate
```

## Evidence

- Phase 154 added operation-level platform I/O stats and made Windows current
  classifications visible as `PlatformNativeAsyncButPartial`.
- Local audit found `compio-fs 0.7.0` Windows `OpenOptions::open` sets
  `FILE_FLAG_OVERLAPPED` before opening the file, then wraps it for compio.
- Local audit found `compio-driver 0.7.1` Windows `ReadAt` and `WriteAt` call
  `ReadFile` / `WriteFile` with `OVERLAPPED`, and IOCP completion ports are
  used by the driver.
- Local audit also found Windows metadata, open, rename, remove, and
  create-directory helpers use blocking/helper paths in the selected backend.
- `cargo check -q --target x86_64-pc-windows-gnu --features platform-io` and
  `cargo check -q --target x86_64-pc-windows-gnu --features platform-io
  --tests` passed.

## Known Residuals

- Real Windows runtime tests have not run in this environment yet.
- A complete Windows Trine operation cannot be upgraded from partial to true
  platform async until every required step is audited and implemented.
- macOS and BSD/other Unix remain later phases.

## Next Recommendation

- Commit Phase 155, then start Phase 156: macOS Platform Backend, unless the
  user chooses to keep iterating on Windows blocking open/metadata/sync steps
  first.
