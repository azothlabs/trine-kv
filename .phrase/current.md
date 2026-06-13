# Current Phase

## Status

Complete

## Goal

Finish platform-io as a cross-platform async abstraction for the current backend
slice: Windows rows are operation-level instead of broad partial claims, and
macOS uses an Apple-side async file data path instead of generic managed
fallback for read/write work.

## Scope

- Windows:
  - keep IOCP-backed read/write operations as partial when the complete Trine
    operation also includes open, metadata, sync, rename, delete, directory, or
    lease setup work;
  - classify operations without a Windows async substep as managed or blocking
    fallback.
- macOS:
  - use Apple `DispatchIO` through `dispatch2` inside platform-io for file
    open/read/write data paths;
  - keep metadata, rename, delete, directory creation/listing, and remaining
    durability gaps visible as partial or fallback at the Trine operation row.
- Runtime/statistics:
  - treat true and partial native async rows as platform async capability;
  - keep per-operation class counters as the detailed truth.
- Keep KV engine code unaware of Windows or macOS mechanics.

## Backend Boundary Receipt

- Trine operation names are the acceptance rows, not OS function names.
- Owned internal surface: platform backend matrix modules, macOS DispatchIO
  backend module, shared platform task submission, runtime capability flag,
  per-operation diagnostics, and target-family tests.
- Chosen macOS backend: Apple `DispatchIO` via `dispatch2`, with unsafe calls
  contained in the macOS platform backend module.
- Known backend limits after this phase: Windows and macOS still have partial
  rows where complete operations include non-native steps; directory listing
  remains blocking fallback; generic other Unix remains managed fallback.
- Leak-check scope: no Windows-specific or macOS-specific branching in KV
  engine code.
- Verification gate: target-family matrix tests, platform-io storage tests,
  Windows target compile checks, and local full gates.

## Out Of Scope

- Making every Windows/macOS complete operation `TruePlatformAsync`.
- Other Unix upgrades beyond preserving current BSD/Solaris and generic fallback
  classification.
- Engine revalidation, storage format changes, publishing, tagging, pushing, or
  PR creation.

## Acceptance Gate

- Windows rows distinguish partial IOCP-backed operations from managed or
  blocking fallback rows.
- macOS read/write data-path rows use an Apple native async backend and are
  classified at the complete Trine operation boundary.
- Runtime capabilities and stats no longer claim macOS/Windows have no platform
  async when native partial async rows exist.
- Protocol/evidence records remaining limits per complete Trine operation.
- Phase completion is recorded and committed.

## Active Task Slice

```text
task720 [x] goal:audit Windows backend substeps | scope:compio windows driver storage path | verify:source evidence
task721 [x] goal:update Windows operation matrix | scope:src/io/platform_backend/windows_backend.rs src/io.rs | verify:matrix test
task722 [x] goal:implement macOS Apple data path | scope:Cargo.toml src/io/platform_backend/apple_dispatch.rs src/io/platform_backend.rs | verify:platform-io storage tests
task723 [x] goal:update runtime/stat diagnostics | scope:src/runtime.rs src/storage.rs src/stats.rs | verify:runtime capability and platform_io tests
task724 [x] goal:record evidence and commit | scope:current evidence protocol roadmap git | verify:full gate plus commit
```

## Evidence

- Windows positioned file read/write use IOCP, while open, metadata, sync,
  rename, delete, directory creation/listing, and related lease steps remain
  synchronous or helper-managed in the selected backend.
- macOS `mio-aio` is not usable for this target because it does not support
  macOS; selected `compio` also leaves macOS regular files on the polling
  fallback path.
- macOS `dispatch2` exposes Apple `DispatchIO`; the new backend uses it for
  open/read/write data paths and keeps the unsafe boundary inside platform-io.
- Platform-io capability is now true on targets with true or partial native
  async rows, while operation counters still show true/partial/fallback
  separately.

## Known Residuals

- Windows rows that include IOCP read/write plus synchronous setup remain
  partial until non-async substeps are removed or split into separate accepted
  operations.
- macOS metadata, rename, delete, directory create/listing, and parts of
  durability remain partial or fallback.
- Directory listing remains explicit blocking fallback on all current native
  backends.

## Next Recommendation

- Run the cross-platform operation acceptance phase to verify Linux, Windows,
  macOS, BSD/Solaris, and generic fallback rows against the same Trine operation
  harness before returning to engine revalidation.
