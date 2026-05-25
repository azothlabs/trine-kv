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

**Status**: In progress

**Goal**: Implement the complete v1 engine in slices without changing the
accepted contracts silently.

**Entry Condition**: Phase 2 complete.

**Acceptance Gate**:

- The v1 acceptance gate in `.phrase/protocol/trine-kv-v1-spec.md` passes.
