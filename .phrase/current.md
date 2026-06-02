# Current Phase

## Status

Complete

## Goal

Make the public crate boundary strict before the first release candidate by
removing durable-format and engine-internal helper modules from the crate root.

## Scope

- Integration tests that currently inspect WAL, manifest, table, and blob files
  through public hidden modules.
- Benchmarks and scaffold tests that currently import internal codec helpers.
- Crate root module visibility for durable-format and engine-internal modules.
- Public user-facing re-exports that should remain stable.

## Out Of Scope

- Storage format changes.
- MVCC, WAL, manifest, table, blob, compaction, transaction, or recovery
  behavior changes.
- Public user-facing API renames.
- Publishing, tagging, or crate version metadata changes.

## Acceptance Gate

- `blob`, `codec`, `internal_key`, `manifest`, `table`, and `wal` are not public
  crate-root modules.
- Durable-file inspection tests still run from crate-internal tests.
- Integration tests and benchmarks compile through public user APIs or local
  test/bench helpers only.
- `cargo check --all-targets --all-features`, `cargo clippy --all-targets
  --all-features -- -D warnings`, `cargo test --all-targets --all-features`,
  rustdoc gates, and `git diff --check` pass.

## Active Task Slice

```text
task561 [x] goal:migrate durable-format integration tests into crate-internal boundary | scope:tests/persistent_wal.rs src/lib.rs | verify:cargo test persistent_ --lib
task562 [x] goal:remove remaining external imports of internal format helpers | scope:tests benches examples | verify:cargo check --all-targets --all-features
task563 [x] goal:make format/helper modules crate-private | scope:src/lib.rs | verify:rustdoc and full native gate
```

## Known Residuals

- Release polish still needs package/example/readme-facing verification after
  this API boundary cleanup.

## Evidence

- `tests/persistent_wal.rs` moved under `tests/internal/` and is included from
  `src/lib.rs` under `cfg(test)`.
- `blob`, `codec`, `internal_key`, `manifest`, `table`, and `wal` are now
  crate-private modules in `src/lib.rs`.
- External tests and benchmarks no longer import internal format helpers from
  `trine_kv`.
- `cargo check --all-targets --all-features`, `cargo test persistent_ --lib`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`,
  `cargo rustdoc --all-features -- -D missing-docs`,
  `cargo rustdoc --all-features -- -D warnings`,
  `cargo test --doc --all-features`, `cargo fmt --check`, and
  `git diff --check` pass.

## Next Recommendation

- Return to release-candidate package/example verification and release polish.
