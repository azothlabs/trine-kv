# Current Phase

## Status

Complete

## Goal

Implement real WASI persistent open support by binding `DbOptions` to a
host-preopened WASI filesystem path, while keeping browser persistence
explicitly unsupported and preserving existing native-file behavior.

## Scope

- Phase 119: WASI persistent backend implementation.

## Out Of Scope

- Browser persistent storage.
- Claiming WASI strict sync durability before host behavior is proven.
- Background worker threads or resumable maintenance budgets for WASI.
- Changing WAL, MVCC, table, manifest, transaction, recovery, or compaction
  correctness rules.

## Acceptance Gate

- `DbOptions::wasi_persistent(path)` carries a path and defaults to inline
  runtime execution with no background workers.
- On `target_os = "wasi"`, WASI persistent open uses the existing persistent
  engine against the host-preopened filesystem path.
- On non-WASI targets, WASI persistent open returns `UnsupportedBackend`.
- WASI strict durability requests return `UnsupportedDurability`.
- Browser persistence remains an explicit `UnsupportedBackend`.
- Native checks, WASI target check, tests, formatting, clippy, diff check,
  forbidden-term scan, project-name scan, and backend-name leakage scan pass.

## Active Task Slice

```text
task497 [x] goal:commit Phase 118 closure | scope:git | verify:commit 127c5b4
task498 [x] goal:add path-carrying WASI persistent option | scope:src/options.rs src/lib.rs | verify:constructor tests
task499 [x] goal:route WASI target to persistent engine | scope:src/db.rs src/db/commit.rs src/runtime.rs | verify:native + wasm32-wasip2 check
task500 [x] goal:preserve unsupported non-WASI/browser boundaries | scope:src/db.rs docs protocol | verify:unit tests
task501 [x] goal:run final gate and record evidence | scope:repo .phrase | verify:full gate
```

## Known Blockers

- WASI strict sync durability remains unsupported until host guarantees are
  proven.
- WASI background workers remain out of scope; default WASI persistent options
  use inline runtime execution.
- Browser persistence still needs an async-only adapter, reliable writer lease,
  atomic publish story, and cooperative budgeted maintenance.

## Evidence

- Verification: `cargo check`, `cargo check --target wasm32-wasip2 --lib`,
  `cargo check --target wasm32-wasip2 --tests`, `cargo test --lib`, `cargo
  clippy --all-targets --all-features -- -D warnings`, `cargo test
  --all-targets --all-features`, `cargo fmt --check`, `git diff --check`,
  forbidden-term scan, project-name scan, and backend-name leakage scan pass.

## Next Recommendation

- Commit Phase 119, then choose browser persistence or resumable maintenance
  budgets as a separate phase.
