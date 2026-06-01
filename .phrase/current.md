# Current Phase

## Status

Complete

## Goal

Close the remaining async tail after true-async capability hardening: make
WASI/browser persistent storage an explicit host-backend boundary, expose
storage/runtime observability needed to debug async behavior, and surface
cooperative maintenance yields without changing storage format or core
database semantics.

## Scope

- Phase 118: async host boundary and observability closure.

## Out Of Scope

- Implementing real WASI or browser persistence.
- Adding hand-written OS bindings, replacing the platform backend, or claiming
  fallback work as true OS async.
- Changing WAL, MVCC, table, manifest, transaction, recovery, or compaction
  correctness rules.

## Acceptance Gate

- WASI and browser persistent modes are explicit public options and fail with
  `UnsupportedBackend` until their host capability adapters exist.
- `DbStats` exposes blocking-adapter queue capacity, queued/submitted/completed
  /rejected task counts, total adapter runtime, and per-storage-operation
  request/latency counters.
- Cooperative maintenance yield and budget-exhaustion counters are recorded
  when foreground work yields to background maintenance or bounded waiting
  expires.
- Existing runtime/storage/backend capability behavior remains unchanged.
- Formatting, clippy, full tests, `platform-io` check, `git diff --check`,
  forbidden-term scan, project-name scan, and backend-name leakage scan pass.

## Active Task Slice

```text
task492 [x] goal:add explicit WASI/browser persistent backend boundary | scope:src/options.rs src/db.rs docs protocol | verify:unsupported-backend test
task493 [x] goal:add runtime blocking-adapter queue observability | scope:src/runtime.rs src/storage.rs src/stats.rs | verify:runtime/storage tests
task494 [x] goal:add storage operation request/latency stats | scope:src/storage.rs src/stats.rs docs | verify:storage/db stats tests
task495 [x] goal:add cooperative maintenance yield counters | scope:src/db.rs src/stats.rs | verify:compaction wait test
task496 [x] goal:run final gate and record evidence | scope:repo .phrase | verify:full gate
```

## Known Blockers

- Real WASI persistence still needs host capability discovery, writable lease
  semantics, durability mapping, and recovery proof.
- Real browser persistence still needs an async-only adapter, reliable writer
  lease, atomic publish story, and cooperative budgeted maintenance.
- Cooperative maintenance is now observable, but resumable compaction work
  budgets are still a future implementation phase.

## Evidence

- `DbOptions::wasi_persistent()` and `DbOptions::browser_persistent()` now
  select explicit host persistent modes and return `UnsupportedBackend`.
- `DbStats` now includes runtime queue stats and per-storage-operation
  request/latency metrics.
- `DbStats` now includes cooperative maintenance yield and budget-exhaustion
  counters.
- Verification: `cargo check`, `cargo check --features platform-io`, `cargo
  test --lib`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `cargo fmt --check`, `git diff
  --check`, forbidden-term scan, project-name scan, and backend-name leakage
  scan pass.

## Next Recommendation

- Commit this closure, then choose a focused next phase for real host
  persistence or resumable maintenance budgets.
