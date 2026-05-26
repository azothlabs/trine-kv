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
