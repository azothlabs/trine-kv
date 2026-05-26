# Roadmap

## Goal

Keep long-term direction visible at phase granularity while letting short-term
implementation follow evidence.

## Planning Rule

Roadmap entries describe phase direction, entry conditions, acceptance gates,
and major out-of-scope boundaries. Detailed implementation tasks belong only in
`.phrase/current.md`.

## Phases

### Phase 1: Freeze V1 Database Spec

**Status**: Accepted

**Goal**: Define Trine KV v1 as a complete embedded LSM MVCC database before
implementation.

**Entry Condition**: `.phrase/decision.md` exists.

**Acceptance Gate**:

- ADR records the LSM MVCC engine decision.
- Protocol spec covers API, MVCC, WAL, SSTable, manifest, compaction, recovery,
  transactions, in-memory mode, tests, and benchmarks.
- User accepts the spec as the implementation source of truth.

### Phase 2: Scaffold Rust Crate

**Status**: Complete

**Goal**: Create the Rust crate and module skeleton that matches the accepted
spec.

**Entry Condition**: Phase 1 accepted.

**Acceptance Gate**:

- `Cargo.toml` follows local Rust guidance.
- Module skeleton matches the spec boundaries.
- `cargo fmt --check`, `cargo clippy`, and empty scaffold tests pass.

### Phase 3: Build V1 Engine By Spec

**Status**: Complete

**Goal**: Implement the complete v1 engine in slices without changing the
accepted contracts silently.

**Entry Condition**: Phase 2 complete.

**Acceptance Gate**:

- The v1 acceptance gate in `.phrase/protocol/trine-kv-v1-spec.md` passes.

### Phase 4: Write Usage Documentation

**Status**: Complete

**Goal**: Give users a runnable path from opening a database to using the core
v1 API safely.

**Entry Condition**: Phase 3 complete.

**Acceptance Gate**:

- README explains what Trine KV is, how to run verification, and where to start.
- Usage docs cover in-memory and persistent open, keyspaces, reads/writes,
  batches, snapshots, transactions, range/prefix scans, durability, maintenance,
  stats, and recovery boundaries.
- At least one example program compiles and runs with `cargo run --example`.

### Phase 5: Polish Public API

**Status**: Complete

**Goal**: Reduce first-use friction in the v1 public API without changing the
storage contract.

**Entry Condition**: Phase 4 complete.

**Acceptance Gate**:

- Common open and write-option paths need less caller-side struct boilerplate.
- Existing v1 tests and examples keep passing.
- Usage docs stay aligned with the polished API.

### Phase 6: Production Hardening

**Status**: Complete

**Goal**: Audit and harden operational behavior after API polish lands.

**Entry Condition**: Phase 5 complete.

**Acceptance Gate**:

- Operational failure-mode audit records concrete risks and verification.
- Hardening changes are backed by focused tests before the phase closes.

### Phase 7: Release Packaging

**Status**: Complete

**Goal**: Prepare the v1 crate for a clean first package using Semantic
Versioning.

**Entry Condition**: Phase 6 complete.

**Acceptance Gate**:

- Cargo package metadata is ready for a `0.1.0` SemVer release candidate.
- Package contents exclude local workflow files and include user-facing docs,
  examples, tests, benches, changelog, and license files.
- Release checklist documents versioning and verification.
- `cargo package --list`, `cargo package`, `cargo fmt --check`,
  `cargo clippy`, `cargo test`, `cargo run --example quickstart`, and
  `git diff --check` pass.

### Phase 8: Integration Examples

**Status**: Complete

**Goal**: Add runnable examples that show Trine KV embedded behind realistic
application boundaries.

**Entry Condition**: Phase 7 complete.

**Acceptance Gate**:

- Integration examples are runnable with `cargo run --example`.
- README or usage docs point users to the examples.
- Examples use public APIs without changing the v1 storage contract.

### Phase 9: CI And Publishing Workflow

**Status**: Complete

**Goal**: Automate release verification and provide a guarded manual crates.io
publishing workflow.

**Entry Condition**: Phase 8 complete.

**Acceptance Gate**:

- CI workflow runs formatting, clippy, tests, examples, package content guard,
  and package verification.
- Publishing workflow is manual, checks the requested SemVer version, runs the
  full verification gate, defaults to dry-run behavior, and publishes only when
  explicitly requested.
- Release docs explain the CI and publishing workflow.

### Phase 10: Targeted Pre-Publish Hardening

**Status**: Complete

**Goal**: Reduce one concrete publish-blocking durability risk before the first
crate publish.

**Entry Condition**: Phase 9 complete and user requests targeted hardening
before publishing.

**Acceptance Gate**:

- The selected risk is classified and fixed without changing public API or v1
  storage formats.
- Focused regression coverage exists for the hardening mechanism.
- The full local release gate still passes.

### Phase 11: Windows Directory Sync Hardening

**Status**: Complete

**Goal**: Extend parent-directory sync after atomic file publish to Windows
before crate publish.

**Entry Condition**: Phase 10 complete and user asks how non-Unix targets are
handled.

**Acceptance Gate**:

- Windows uses a directory handle path for parent-directory sync after rename.
- Unix behavior remains unchanged.
- Other targets are documented as best-effort.
- The full local release gate still passes.

### Phase 12: Benchmark-Backed Performance Tuning

**Status**: Complete

**Goal**: Improve one measured v1 benchmark hotspot before CI push without
changing public API or storage formats.

**Entry Condition**: Phase 11 complete and user requests benchmark/performance
tuning.

**Acceptance Gate**:

- Current benchmark baseline is recorded.
- A hotspot is selected from benchmark evidence before implementation.
- The tuning change has before/after benchmark evidence and keeps the full
  release gate passing.

### Phase 13: Rust 1.85 CI Compatibility Fix

**Status**: Complete

**Goal**: Restore CI compatibility with the declared Rust 1.85 MSRV without
raising the crate's minimum supported compiler.

**Entry Condition**: Remote CI reports Rust 1.85 rejecting crate code that
newer local toolchains accepted.

**Acceptance Gate**:

- Code no longer uses unstable-in-1.85 `Vec` methods inside `const fn`.
- Runtime public API behavior and storage formats remain unchanged.
- Local verification passes for formatting, clippy, tests, examples, package
  checks, and dry-run publishing.

### Phase 14: Lazy Range Iterator

**Status**: Complete

**Goal**: Replace eager range/prefix result building with a lazy seek cursor
that merges memtable and SSTable records under MVCC visibility.

**Entry Condition**: User review identifies eager range iteration as an
incorrect engine shape for v1.

**Acceptance Gate**:

- Range and prefix scans create a cursor instead of prebuilding all visible
  `KeyValue` rows.
- The cursor merges memtable and SSTable user-key groups lazily and applies
  MVCC point/range-delete rules per returned row.
- Existing scan, snapshot, range-delete, table, and persistent tests pass.
- A focused test proves table blocks are not touched until `Iterator::next`.

### Phase 15: Point Read Hot Path

**Status**: Complete

**Goal**: Remove avoidable contention and allocation from point reads without
changing public API or v1 storage formats.

**Entry Condition**: User benchmark review identifies repeated snapshot pinning,
global block-cache locking, vector-based point lookup, and full memtable scans
as point-read bottlenecks.

**Acceptance Gate**:

- Snapshot-backed point reads reuse the caller's existing snapshot pin.
- Point reads seek memtable/table records for the requested user key and choose
  the newest visible record without building and sorting a full record vector.
- Block-cache hit tracking no longer depends on one global exclusive lock.
- Existing MVCC, range-delete, persistent, transaction, and benchmark gates pass.

### Phase 16: LSM Write Path And WAL Lifecycle

**Status**: Complete

**Goal**: Make the write path match the v1 LSM shape by adding immutable
memtables, size-triggered active memtable freeze, pressure-triggered flush, and
bounded WAL replay after flush.

**Entry Condition**: User audit identifies active-only memtables, manual-only
flush, and unbounded WAL replay as the next P1 production risks.

**Acceptance Gate**:

- Active memtables freeze into immutable memtables when the configured write
  buffer threshold is reached.
- Reads, transactions, range scans, and prefix scans include immutable
  memtables before SSTables.
- Flush consumes immutable memtables and manual flush first freezes current
  active memtables.
- Immutable memtable pressure is handled before accepting the next write, so
  storage errors do not leave a new write half-reported.
- Manifest WAL replay floor advances only after flushed SSTables are published,
  and the WAL is atomically rewritten so startup does not decode indefinitely
  old flushed batches.
- Existing MVCC, range-delete, persistent, transaction, recovery, and release
  gates pass.
