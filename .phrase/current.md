# Current Phase

## Status

Complete

## Goal

Correct the platform I/O architecture so Trine's `io` boundary is the design
subject and the selected native backend is only an implementation detail.

## Backend Boundary Receipt

- Trine operations owned by `io`: length lookup, owned random read, optional
  whole-object read, temp-write-and-rename publish, append-object open, append,
  persist, object delete, directory create, directory sync, directory listing,
  and writer lease acquisition.
- Owned interface: `IoCompletion`, `IoDriverInfo`, `IoDriverKind`,
  `InlineIoDriver`, `BlockingAdapterIoDriver`, and `PlatformIoDriver`.
- Selected backend: the feature-gated native platform backend currently used by
  `RuntimeMode::PlatformIo`.
- Backend limits: directory listing has no true native async enumeration
  primitive in the selected backend and must remain a separately counted
  platform-driver blocking fallback.
- Leak-check scope: user-facing docs, current phase, roadmap, and protocol must
  name Trine `io`, platform backend, capabilities, and fallbacks rather than a
  dependency crate. Dependency-selection evidence and Cargo metadata may name
  the dependency.
- Verification gate: focused platform I/O tests, full feature gate, diff check,
  forbidden-term scan, and backend-name leakage scan.

## Scope

- Keep Phase 108 behavior: platform runtime routes native-file supported work
  through `PlatformIoDriver`, including listing fallback and writer lease drop
  cleanup.
- Move backend-specific platform implementation code out of the `io` boundary
  surface into a backend implementation module.
- Keep storage code submitting Trine operation requests through `crate::io`
  only.
- Update docs and phase records so the architecture is `io`-boundary-led.
- Commit only after the correction and verification pass.

## Out Of Scope

- Replacing the selected backend crate.
- Adding target-specific Linux, macOS/BSD, or Windows backend implementations
  in this phase.
- Making platform I/O the default runtime mode.
- Changing public API, storage format, WAL, MVCC, table, manifest, compaction,
  transaction, or recovery semantics.

## Acceptance Gate

- `src/io.rs` expresses Trine-owned completion, driver info, driver submission,
  and operation routing without backend crate references.
- Backend-specific native platform implementation lives below the `io` boundary
  in a feature-gated implementation module.
- `src/storage.rs` depends on `crate::io` operation methods, not a backend
  crate or backend-specific implementation module.
- Docs/protocol/current/roadmap pass backend-name leakage scan outside
  dependency-selection evidence.
- Formatting, clippy, full tests, `git diff --check`, forbidden-term scan, and
  backend-name leakage scan pass.
- Commit message states this is an `io` boundary correction and names the
  verification.

## Active Task Slice

```text
task474 [x] goal:record backend boundary receipt | scope:.phrase/current.md | verify:manual
task475 [x] goal:separate platform backend implementation from io boundary | scope:src/io.rs src/io/platform_backend.rs | verify:cargo check --features platform-io
task476 [x] goal:audit storage dependency and docs naming | scope:src/storage.rs docs .phrase | verify:leakage scan
task477 [x] goal:run final verification | scope:repo | verify:full gate
task478 [x] goal:commit correction | scope:git | verify:git status
```

## Known Blockers

- Directory enumeration is still a platform-driver blocking fallback, not true
  platform async I/O, until the selected backend exposes a real directory
  enumeration operation.

## Evidence

- Process correction rules are now durable in `.phrase/decision.md` and the
  async storage protocol.
- Verification passed: `cargo check`, `cargo check --features platform-io`,
  `cargo test platform_io_native_file_management_ops_use_platform_driver --lib
  --features platform-io`, `cargo clippy --all-targets --all-features -- -D
  warnings`, `cargo test --all-targets --all-features`, `cargo fmt --check`,
  `git diff --check`, forbidden-term scan, project-name scan, and backend-name
  leakage scan.

## Next Recommendation

- Start new platform backend work only after writing the next backend boundary
  receipt first.
