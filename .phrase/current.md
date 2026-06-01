# Current Phase

## Status

Complete

## Goal

Add resumable maintenance work budgets so hosts can advance flush and compaction
in bounded atomic units, and classify the remaining browser persistent backend
work without pretending an unsupported storage path is complete.

## Scope

- Phase 120: resumable compaction and maintenance work budgets.

## Out Of Scope

- Browser persistent storage implementation.
- Splitting one compaction output publish across multiple manifests.
- Changing WAL, MVCC, table, manifest, transaction, recovery, or compaction
  correctness rules.

## Acceptance Gate

- Public maintenance budget and outcome types describe bounded flush and
  compaction work.
- `run_maintenance_with_budget` advances at most the requested number of flush
  inputs and compaction units, then reports whether the budget was exhausted.
- `compact_range_with_budget` advances one bounded compaction pass for a key
  range without blocking behind a busy compaction reservation.
- Budget exhaustion increments `maintenance_budget_exhaustions`.
- Existing `flush()` and `compact_range()` barrier behavior is preserved.
- Focused tests prove budget exhaustion and resume-by-replanning behavior.
- Native checks, wasm target checks, tests, formatting, clippy, diff check,
  forbidden-term scan, project-name scan, and backend-name leakage scan pass.

## Active Task Slice

```text
task502 [x] goal:commit Phase 119 WASI backend | scope:git | verify:commit fa243b7
task503 [x] goal:add public maintenance budget API | scope:src/db.rs src/lib.rs | verify:cargo check
task504 [x] goal:budget flush and compaction units | scope:src/db.rs | verify:focused persistent_wal tests
task505 [x] goal:document cooperative maintenance budgets | scope:docs protocol .phrase | verify:docs diff
task506 [x] goal:classify browser persistent blocker | scope:storage/db audit | verify:wasm32-unknown-unknown check + blocking call audit
```

## Known Blockers

- Browser persistence still requires a true async browser object store, reliable
  writer lease, atomic manifest publish, and an async persistent open path.
- Current persistent database open, WAL, manifest, table, blob, recovery, and
  cleanup paths still call blocking storage adapters around `NativeFileBackend`;
  that cannot be used as a real browser persistent backend on the browser main
  thread.
- `wasm32-unknown-unknown` library compilation passes, so the browser blocker
  is backend integration, not basic target compilation.

## Evidence

- Verification: `cargo check`, `cargo check --target wasm32-wasip2 --lib`,
  `cargo check --target wasm32-wasip2 --tests`, `cargo check --target
  wasm32-unknown-unknown --lib`, `cargo test --test persistent_wal`, `cargo
  clippy --all-targets --all-features -- -D warnings`, `cargo test
  --all-targets --all-features`, `cargo fmt --check`, `git diff --check`,
  forbidden-term scan, project-name scan, and backend-name leakage scan pass.
- Audit: persistent paths still reference `NativeFileBackend` and
  `BlockingStorage*` APIs across `db`, `wal`, `manifest`, `table`, `blob`, and
  `recovery`.

## Next Recommendation

- Commit Phase 120, then start a browser persistence phase whose first task is
  replacing the remaining blocking persistent engine calls with async storage
  operations before wiring IndexedDB/OPFS.
