# Current Phase

## Status

Complete

## Goal

Make database opening read like a normal database API before the first release
candidate: passing a path opens a persistent database, and in-memory mode is an
explicit option.

## Scope

- Public `Db::open` and `Db::open_sync` inputs.
- `DbOptions` constructors used by docs and examples.
- README, usage guide, durability notes, changelog, examples, tests, and
  benchmark call sites affected by open API naming.
- No storage-format, WAL, manifest, compaction, MVCC, recovery, or transaction
  semantic changes.

## Out Of Scope

- Removing host-specific `DbOptions::wasi_persistent` or
  `DbOptions::browser_persistent`.
- Changing durability guarantees beyond the existing safety-first native
  persistent default.
- Publishing, tagging, or changing crate version metadata.

## Acceptance Gate

- `Db::open(path).await?` and `Db::open_sync(path)?` open persistent databases.
- `Db::open(DbOptions::memory()).await?` and
  `Db::open_sync(DbOptions::memory())?` are the explicit in-memory path.
- Configured persistent opens use `DbOptions::new(path)`.
- Mode-specific public open helpers are no longer part of the primary API.
- README, usage docs, examples, tests, and benchmarks compile against the
  path-first open API.

## Active Task Slice

```text
task554 [x] goal:make open path-first persistent by default | scope:src/db.rs src/options.rs src/lib.rs | verify:cargo check --all-targets --all-features
task555 [x] goal:update docs/examples/tests/benches for path-first open | scope:README.md docs examples tests benches CHANGELOG.md .phrase | verify:cargo fmt --check; cargo clippy --all-targets --all-features -- -D warnings; cargo test --all-targets --all-features; example gate
```

## Known Residuals

- `DbOptions::default()` remains an in-memory options value because a persistent
  default requires a caller-provided path. Public docs use path input or
  `DbOptions::new(path)` for ordinary persistent databases.
- Host persistence remains selected through explicit WASI/browser options.

## Evidence

- `cargo check --all-targets --all-features` passes after `Db::open` and
  `Db::open_sync` accept path inputs.
- `cargo fmt --check` and
  `cargo clippy --all-targets --all-features -- -D warnings` pass.
- `cargo test --test scaffold` passes with a path-open persistent smoke test.
- `cargo test --all-targets --all-features` passes.
- `cargo run --example quickstart`, `sync_quickstart`, `user_store`, and
  `event_index` pass.
- `cargo check --target wasm32-unknown-unknown --lib` and
  `cargo check --target wasm32-wasip1 --lib` pass.
- `cargo package --list --allow-dirty` and `git diff --check` pass.
- Public docs/examples no longer use `open_persistent`, `open_memory`, or
  `Db::memory` as the first-use open path.

## Next Recommendation

- Commit this API DX follow-up if the release-candidate scope looks right.
