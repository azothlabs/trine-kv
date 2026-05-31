# Current Phase

## Status

Complete

## Goal

Route async table cursor data-block loads through awaited owned-read
completions while preserving the synchronous iterator path.

## Scope

- Add an async cache-miss loader for data blocks so async cursor advancement can
  await a data-block load without holding block-cache locks across the await.
- Add async table data-block loading that uses `NativeFileObject` owned read
  completion when a cached table file is available.
- Keep synchronous `Table::load_data_block` and synchronous iterator behavior
  unchanged.
- Keep no-file standalone table helper behavior on the existing synchronous
  fallback path.
- Reuse the persistent async range/prefix test from Phase 89 to exercise the
  flushed-table async cursor path.
- Preserve existing public async API shape, blocking API, publish barrier,
  commit tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Adding true async OS file I/O.
- Converting table header/footer metadata reads to async owned reads.
- Moving synchronous iterators onto async advancement.
- Adding public runtime tuning options.
- Changing public storage formats or recovery protocol.
- Narrowing the publish barrier or adding WAL partitioning.

## Acceptance Gate

- Roadmap records this as the async table block-load phase.
- Async table cursor block loading awaits a `StorageReadObject::read_exact_at_owned`
  completion when a cached native table file exists.
- The async block-cache miss path does not hold cache locks across awaited
  loading.
- Synchronous table block loading remains available and unchanged for blocking
  iterators.
- Focused async/table/cache tests, formatting, clippy, full tests,
  `git diff --check`, and forbidden-term scan pass.
- Evidence records remaining async blockers and the recommended next phase.

## Active Task Slice

```text
task372 [x] goal:start async table block-load phase | scope:current roadmap | verify:manual
task373 [x] goal:add async data-block cache miss loader | scope:src/cache.rs | verify:cache/table tests
task374 [x] goal:route async table cursor block load through owned read completion | scope:src/table.rs | verify:async tests
task375 [x] goal:verify focused and full gates | scope:src tests benches | verify:full tests
task376 [x] goal:record evidence and commit | scope:.phrase/evidence.md current roadmap src | verify:git status
```

## Known Blockers

- Native-file async reads still use the bounded blocking adapter rather than
  platform-native async file I/O.
- Table header/footer metadata reads still use the borrowed read path.
- Runtime tuning options are still internal.
- True multi-writer execution still needs writer-local deltas and WAL
  partitioning.
- Public recovery report reads remain standalone no-runtime helpers by design.

## Evidence

- Phase 89 established an internal async cursor advancement path down to
  `TablePointCursor::ensure_current_block_async`.
- The async table cursor now has a real await point for owned table block read
  completion.
- `BlockCache` now has an async data-block miss loader that checks hits before
  awaiting and reloads outside cache locks before insertion.
- `Table::load_data_block_async` uses the async cache loader and
  `StorageReadObject::read_exact_at_owned` through cached native table files.
- Synchronous `Table::load_data_block` remains unchanged for blocking
  iterators.
- Verification passed: `cargo test --test async_api`, `cargo test cache --lib`,
  `cargo test table --lib`, `cargo test storage --lib`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, `cargo test --all-targets --all-features`,
  `git diff --check`, and forbidden-term scan outside the repository
  instruction file.

## Next Recommendation

- Start the table metadata async-read phase or keep it separate if the next
  priority is multi-writer work. The remaining read-path gap is table
  header/footer and index metadata reads, which still use borrowed synchronous
  reads.
