# Current Phase

## Status

Complete

## Goal

Give macOS its own platform-io backend classification instead of inheriting the
generic non-Linux Unix fallback label. The macOS backend should remain honest:
ordinary file work is platform-managed fallback until a platform-supported
async regular-file path is audited and implemented.

## Scope

- Audit the selected `compio` macOS regular-file path.
- Add a distinct macOS platform backend kind and matrix module.
- Keep macOS operation classes fallback-classified where the selected backend
  uses blocking decisions or direct syscalls.
- Verify macOS diagnostics on the local host.
- Update ADR/protocol/evidence so macOS is first-class but not overstated.

## Backend Boundary Receipt

- Trine operation names: length lookup, owned random read, whole-object read,
  temporary write plus rename publish, append open, append, persist/fsync, WAL
  rewrite, delete, directory create, directory sync, directory listing, and
  writer lease.
- Owned interface: `PlatformIoBackendKind::MacOsNative`,
  `src/io/platform_backend/macos_backend.rs`, platform operation stats, ADR
  0002, and async storage protocol.
- Chosen backend: selected `compio` Unix polling path on macOS.
- Known backend limits: `compio-driver 0.7.1` enables AIO only for FreeBSD and
  Solaris-family targets. On macOS, regular-file open/stat/read/write/sync and
  rename/delete/create-directory operations are blocking decisions or direct
  syscalls in the selected path.
- Leak-check scope: no macOS-specific branching in KV engine code; only the
  platform backend matrix and diagnostics distinguish macOS.
- Verification gate: macOS host platform matrix/storage tests, formatting,
  clippy/rustdoc if public docs are touched, full local tests if feasible, and
  diff checks.

## Out Of Scope

- Implementing new Apple-specific async file primitives.
- Implementing Windows or BSD backend upgrades.
- Rewriting compaction, maintenance, cleanup, close, or cooperative
  maintenance.
- Changing manifest, WAL, SSTable, MVCC, transaction, or recovery formats.
- Publishing, tagging, pushing, or creating a GitHub release.

## Acceptance Gate

- macOS has an explicit backend kind and matrix module.
- macOS ordinary file operations report `PlatformManagedFallback`.
- macOS directory listing reports `BlockingFallback`.
- Local macOS platform-io tests prove diagnostics use platform-driver fallback
  counters and operation-level fallback counters.
- Evidence records that true macOS async regular-file support remains a future
  implementation/audit.
- Phase completion is committed before starting the next phase.

## Active Task Slice

```text
task689 [x] goal:start macOS backend phase | scope:current roadmap | verify:phase brief
task690 [x] goal:audit selected macOS compio path | scope:cargo registry source | verify:audit notes
task691 [x] goal:add explicit macOS backend matrix | scope:src/io src/io/platform_backend | verify:platform matrix test
task692 [x] goal:update docs/evidence for macOS backend limits | scope:ADR protocol evidence | verify:docs diff
task693 [x] goal:verify and commit Phase 156 | scope:tests docs git | verify:local macOS gate
```

## Evidence

- Local audit found `compio-driver 0.7.1` sets `aio` only for FreeBSD and
  Solaris-family targets in `build.rs`.
- Local audit found macOS regular-file `ReadAt`, `WriteAt`, and `Sync` on the
  selected polling path use `Decision::Blocking` before direct `pread`,
  `pwrite`, or `fsync` syscalls.
- Local audit found open/stat/rename/delete/create-directory operations are
  also blocking decisions or direct syscalls in the selected path.
- `src/io/platform_backend/macos_backend.rs` now gives macOS an explicit
  `MacOsNative` backend kind while keeping operation classes honest.
- `cargo fmt --check`, `cargo check -q`, `cargo check -q --features
  platform-io`, focused macOS platform tests, `cargo clippy -q
  --all-features`, `cargo rustdoc --all-features -- -D warnings`,
  `cargo test -q`, `cargo test -q --features platform-io`, and
  `git diff --check` passed.

## Known Residuals

- No Apple-specific true async regular-file implementation exists in this
  phase.
- BSD/other Unix still need their own phase.
- Engine compaction, maintenance, cleanup, close, and cooperative maintenance
  still need later revalidation.

## Next Recommendation

- Commit Phase 156, then start Phase 157: BSD/other Unix backend
  classification.
