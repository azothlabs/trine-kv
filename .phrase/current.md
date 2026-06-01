# Current Phase

## Status

Complete

## Goal

Close Phases 110 through 116 by turning the platform backend work into an
evidence-backed capability matrix, target switch layer, honest per-operation
stats, directory-listing closure, and final async storage verification gate.

## Backend Boundary Receipt

- Trine operations owned by `io`: length lookup, owned random read, optional
  whole-object read, temp-write-and-rename publish, append-object open, append,
  persist, object delete, directory create, directory sync, directory listing,
  and writer lease acquisition.
- Owned interface: `IoCompletion`, `IoDriverInfo`, `IoDriverKind`,
  `InlineIoDriver`, `BlockingAdapterIoDriver`, `PlatformIoDriver`, platform
  operation class, and platform backend capability matrix.
- Selected backend: feature-gated native platform backend below `src/io.rs`.
- Backend switch targets: Linux native async path, Windows native-capable
  backend path with fallback-classified Trine composite operations, Unix
  polling fallback path, and explicit unsupported fallback path.
- Backend limits: directory listing has no true native async enumeration
  primitive in the selected backend and remains a separately counted
  platform-driver blocking fallback. macOS/BSD regular-file operations are not
  claimed as true native async without a stronger backend.
- Leak-check scope: docs/current/roadmap/protocol/storage/db/stats/io boundary
  must name Trine `io`, platform backend, capabilities, operation classes, and
  fallbacks rather than a dependency crate. Cargo metadata, backend
  implementation code, and dependency-selection evidence may name the
  dependency.
- Verification gate: platform backend matrix tests, focused platform storage
  tests, full feature gate, diff check, forbidden-term scan, project-name scan,
  and backend-name leakage scan.

## Scope

- Phase 110: record a platform backend capability matrix for Linux, Windows,
  macOS/BSD/Unix fallback, and unsupported fallback.
- Phase 111: add an `io` backend switch layer below `PlatformIoDriver`.
- Phase 112: enable Linux native async backend through the platform backend
  feature and classify Linux operations as true async except directory listing.
- Phase 113: record that Windows read/write primitives are IOCP-capable, while
  current Trine composite storage operations remain backend fallback because
  they include fallback-classified open, metadata, sync, rename, or directory
  work.
- Phase 114: record macOS/BSD as a backend decision: polling fallback only for
  regular files in this phase, not true native async.
- Phase 115: close directory enumeration by making it an explicit
  platform-driver blocking fallback with tests and stats.
- Phase 116: run final async storage gate and record evidence.

## Out Of Scope

- Replacing the selected backend dependency.
- Adding hand-written OS bindings for Linux, Windows, macOS, or BSD.
- Claiming macOS/BSD ordinary file reads and writes are true native async.
- Making platform I/O the default runtime mode.
- Changing public API behavior, storage format, WAL, MVCC, table, manifest,
  compaction, transaction, or recovery semantics.

## Acceptance Gate

- Linux builds enable the native async backend feature and classify supported
  file operations as true platform async work.
- Windows builds classify current Trine composite storage operations as backend
  fallback unless a future implementation proves every step in that operation
  can use a native async primitive.
- macOS/BSD/other Unix builds classify platform-driver file work as backend
  fallback unless a stronger backend is added.
- Directory listing is explicitly counted as platform-driver blocking fallback,
  separate from true platform async work and separate from Trine's bounded
  blocking adapter.
- `DbStats` reports true platform async tasks, platform backend fallback tasks,
  platform blocking fallback tasks, and blocking-adapter tasks separately.
- Docs/protocol/current/roadmap pass backend-name leakage scan outside allowed
  files.
- Formatting, clippy, full tests, `git diff --check`, forbidden-term scan,
  project-name scan, and backend-name leakage scan pass.

## Active Task Slice

```text
task479 [x] goal:record 110-116 backend boundary receipt | scope:.phrase/current.md | verify:manual
task480 [x] goal:add platform backend operation matrix | scope:src/io.rs src/io/platform_backend.rs | verify:io tests
task481 [x] goal:add target backend switch modules | scope:src/io/platform_backend/*.rs | verify:cargo check --features platform-io
task482 [x] goal:enable Linux native async backend feature | scope:Cargo.toml Cargo.lock | verify:cargo check --features platform-io
task483 [x] goal:surface true async vs backend fallback stats | scope:src/stats.rs src/db.rs src/storage.rs | verify:platform storage tests
task484 [x] goal:record directory enumeration closure and mac/BSD decision | scope:docs .phrase | verify:leakage scan
task485 [x] goal:run final async storage gate | scope:repo | verify:full gate
task486 [x] goal:commit 110-116 closure | scope:git | verify:git status
```

## Known Blockers

- Directory enumeration remains true-async-blocked until a backend exposes a
  native async directory enumeration operation.
- macOS/BSD ordinary file operations remain backend fallback in this phase.

## Evidence

- Local dependency audit: the selected backend line supports Linux native async
  when its native async feature is enabled and Windows IOCP for lower-level
  read/write primitives, but current Windows Trine composite storage operations
  still include fallback-classified steps. Non-Linux Unix uses polling
  fallback. The filesystem crate does not expose directory enumeration.
- Implementation evidence: the platform backend matrix now records operation
  classes per target family, storage stats distinguish true platform async
  tasks from backend fallback and blocking fallback tasks, and Linux builds
  enable the selected backend's native async feature through `platform-io`.
- Verification evidence: `cargo check`, `cargo check --features platform-io`,
  focused platform matrix/storage tests, `cargo clippy --all-targets
  --all-features -- -D warnings`, `cargo test --all-targets --all-features`,
  `cargo fmt --check`, `git diff --check`, forbidden-term scan, project-name
  scan, and backend-name leakage scan pass.

## Next Recommendation

- Keep the matrix as the source of truth for future platform I/O work. Only
  start hand-written OS backend work after a new boundary receipt and
  target-specific proof that the operation can honestly be true async.
