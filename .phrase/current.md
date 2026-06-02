# Current Phase

## Status

Complete

## Goal

Make the public API read async-first before the first release candidate by
giving primary names to async database, bucket, scan, lazy value, snapshot, and
transaction operations, while keeping synchronous callers on explicit `*_sync`
adapters.

## Scope

- Phase 131: async-first public API rename.
- Public `Db`, `Bucket`, `BucketReader`, `Transaction`, `Snapshot`, `Iter`,
  `LazyIter`, `LazyKeyValue`, and `LazyValue` method names.
- Public `DbStats` sync-adapter field names.
- README, usage docs, durability notes, changelog, examples, tests, and
  benchmarks that demonstrate or depend on public API names.
- No storage-format, MVCC, WAL, manifest, compaction, blob, recovery, or
  transaction semantic changes.

## Out Of Scope

- Replacing synchronous maintenance/WAL internals with a primary async engine.
- Adding an in-browser runtime persistence fixture.
- Publishing, tagging, or changing crate version metadata.
- Changing pure in-memory builder APIs such as `WriteBatch::put` and staged
  `Transaction::put`.

## Acceptance Gate

- Async operations own the primary public names, for example `Db::open`,
  `db.put`, `db.get`, `db.flush`, `bucket.get`, `txn.commit`, `iter.next`, and
  `value.read`.
- Synchronous database/storage operations are available through explicit
  `*_sync` adapters.
- Pure builder/state helpers keep natural names when they do not trigger
  storage work.
- Public stats fields use `sync_adapter` naming for sync-adapter observability.
- README, usage docs, examples, tests, and benchmarks compile against the new
  names.
- Full native verification and release-facing example gates pass.

## Active Task Slice

```text
task551 [x] goal:rename public API so async owns primary names | scope:src/db.rs src/bucket.rs src/transaction.rs src/snapshot.rs src/iterator.rs src/db/commit.rs | verify:cargo check --all-targets --all-features
task552 [x] goal:update docs/examples/tests/benches to async-first names | scope:README.md docs examples tests benches CHANGELOG.md .phrase | verify:cargo fmt --check; cargo clippy --all-targets --all-features -- -D warnings; cargo test --all-targets --all-features; example gate
task553 [x] goal:record phase evidence and commit | scope:.phrase/evidence.md git commit | verify:git diff --check; forbidden-term scan; clean staged diff
```

## Known Residuals

- Native async maintenance and WAL operations still use runtime task boundaries
  over synchronous internals; this phase only fixes the public API contract.
- Sync adapters intentionally remain for native and non-browser embeddings.
- Browser runtime persistence still lacks an in-browser fixture.

## Evidence

- `cargo check --all-targets --all-features` passes after the public API rename.
- `cargo fmt --check` and `cargo clippy --all-targets --all-features -- -D warnings` pass.
- `cargo test --all-targets --all-features` passes.
- `cargo run --example quickstart`, `async_quickstart`, `user_store`, and
  `event_index` pass.
- `cargo check --target wasm32-unknown-unknown --lib` and
  `cargo check --target wasm32-wasip1 --lib` pass.
- `cargo package --list --allow-dirty`, `git diff --check`, and the
  forbidden-term scan pass.

## Next Recommendation

- Commit the async-first public API rename, then return to release-candidate
  verification and publish readiness.
