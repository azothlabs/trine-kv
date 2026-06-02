# Current Phase

## Status

Complete

## Goal

Polish the release-candidate surface so the crate package, examples, README,
docs, and public API presentation are coherent after the API boundary cleanup.

## Scope

- Cargo package metadata and packaged file list.
- README, docs, changelog, examples, and public first-use snippets.
- Release-facing examples and doctests.
- Scans for stale public API names, hidden internal helper references, and
  project-forbidden wording.

## Out Of Scope

- Publishing, tagging, or changing crate version metadata unless verification
  proves the metadata is invalid.
- Storage format, MVCC, WAL, manifest, table, blob, compaction, transaction, or
  recovery semantic changes.
- New public API names or behavior changes.
- Browser persistence fixture automation unless package/example verification
  exposes it as a release blocker.

## Acceptance Gate

- `cargo package --list --allow-dirty` shows expected release-facing files and
  no obvious local-only clutter.
- `cargo package --allow-dirty`, release-facing examples, doctests, and full
  native verification pass.
- README/docs/examples align with path-first persistent open and async-first
  public naming.
- Scans find no stale public helper-module references or project-forbidden
  wording in release-facing files.
- `cargo fmt --check` and `git diff --check` pass.

## Active Task Slice

```text
task564 [x] goal:audit package metadata and packaged file surface | scope:Cargo.toml README.md package list | verify:cargo package --list --allow-dirty
task565 [x] goal:verify release-facing examples and docs | scope:examples README docs src docs | verify:cargo run examples + cargo test --doc --all-features
task566 [x] goal:remove stale release-facing wording/API references | scope:README docs examples CHANGELOG src docs .phrase | verify:rg scans + cargo package --allow-dirty
```

## Known Residuals

- Primary async maintenance/WAL internals and browser persistence fixture
  automation remain follow-up hardening; this phase did not find them to be
  release blockers.
- Publishing, tagging, and version bump decisions remain separate release
  actions.

## Evidence

- `cargo package --list --allow-dirty` packaged 80 files and did not include
  `.github/`, `.phrase/`, `.rust-skills/`, `.claude/`, or other local workflow
  directories.
- `cargo package --allow-dirty` passed after rerunning outside the sandboxed
  proxy restriction; `cargo package --allow-dirty --offline` also passed.
- `cargo run --example quickstart`, `sync_quickstart`, `user_store`, and
  `event_index` pass.
- `cargo test --doc --all-features`, `cargo clippy --all-targets
  --all-features -- -D warnings`, and `cargo test --all-targets
  --all-features` pass.
- `cargo check --target wasm32-unknown-unknown --lib`, `cargo check --target
  wasm32-wasip1 --lib`, and `cargo clippy --target wasm32-unknown-unknown
  --lib -- -D warnings` pass.
- Release-facing stale API/internal helper scans and project-forbidden wording
  scans found no matches.
- `cargo fmt --check` and `git diff --check` pass.
- The extra README explanatory section was removed after user review; remaining
  release-facing README content passed the same wording/API scans.
- CI follow-up changed the storage test accounting helper to borrow
  `NativeFileStorageStats`; the all-target clippy gate and
  `cargo test storage::tests --lib --all-features` pass.

## Next Recommendation

- Prepare the final release-candidate claim and decide separately whether to
  tag or publish.
