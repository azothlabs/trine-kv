# Current Phase

## Status

Complete

## Goal

Move async table cursor metadata reads for data-block decisions and index
partition misses onto awaited owned-read completions.

## Scope

- Add an async cache-miss loader for index partitions.
- Add async table data-block metadata lookup and index partition loading.
- Route async table cursor block-state checks, async data-block load metadata,
  and async prefix false-positive validation through the async metadata path.
- Preserve synchronous metadata reads for blocking iterators and standalone
  helper paths.
- Preserve existing public async API shape, blocking API, publish barrier,
  commit tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Adding true async OS file I/O.
- Converting table open header/footer reads.
- Moving synchronous iterators onto async advancement.
- Adding public runtime tuning options.
- Changing public storage formats or recovery protocol.
- Narrowing the publish barrier or adding WAL partitioning.

## Acceptance Gate

- Roadmap records this as the async table metadata-read phase.
- Async table cursor block-state checks use async data-block metadata lookup.
- Async index partition cache misses await owned storage read completion when a
  cached native table file exists.
- Synchronous table metadata reads remain available and unchanged for blocking
  iterators.
- Focused async/table/cache tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining async blockers and the recommended next phase.

## Active Task Slice

```text
task377 [x] goal:start async table metadata-read phase | scope:current roadmap | verify:manual
task378 [x] goal:add async index partition cache miss loader | scope:src/cache.rs | verify:cache/table tests
task379 [x] goal:add async table metadata lookup and index partition reads | scope:src/table.rs | verify:table/async tests
task380 [x] goal:verify focused and full gates | scope:src tests benches | verify:full tests
task381 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap src | verify:git status
```

## Known Blockers

- Native-file async reads still use the bounded blocking adapter rather than
  platform-native async file I/O.
- Table open header/footer reads still use synchronous borrowed reads.
- Runtime tuning options are still internal.
- True multi-writer execution still needs writer-local deltas and WAL
  partitioning.
- Public recovery report reads remain standalone no-runtime helpers by design.

## Evidence

- Phase 90 moved async table cursor data-block body reads onto awaited owned
  storage read completions.
- The remaining async cursor read gap is metadata needed to choose and validate
  data blocks, especially index partition misses.
- `BlockCache` now has an async index-partition miss loader.
- Async table cursor block-state checks, async data-block load metadata lookup,
  and async prefix false-positive validation now use async data-block metadata
  lookup.
- Async index partition reads use `StorageReadObject::read_exact_at_owned`
  through cached native table files when available.
- Verification passed: `cargo test --test async_api`, `cargo test cache --lib`,
  `cargo test table --lib`, `cargo test storage --lib`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, `cargo test --all-targets --all-features`,
  `git diff --check`, and forbidden-term scan outside the repository
  instruction file.

## Next Recommendation

- The read cursor path now has awaited owned-read completions for data-block
  metadata and body loads. The remaining read-side gap is table open
  header/footer metadata, but the larger async plan can now reasonably move to
  writer-local deltas before WAL partitioning.
