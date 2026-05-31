# Current Phase

## Status

Complete

## Goal

Add benchmark-backed evidence for table block-decode read cost under Trine
runtime modes before changing cursor advancement or decode scheduling.

## Scope

- Add benchmark rows for persistent table point reads with native-thread runtime
  and inline runtime.
- Disable the block cache in the new benchmark rows and use small table blocks
  so each measured point read exercises table data-block loading and decode.
- Assert the benchmark path records table data-block reads and disabled-cache
  misses, making the row useful as read-path evidence rather than only a timing
  loop.
- Keep existing block decode scheduling unchanged: synchronous callers still
  read and decode synchronously through the owned read seam.
- Record benchmark output and use it to choose the next async cursor/read
  phase.
- Preserve existing public async API, blocking API, publish barrier, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and storage behavior.

## Out Of Scope

- Adding true async block decode.
- Converting range or prefix cursor advancement to awaitable advancement.
- Coupling synchronous decode callers to the runtime blocking queue.
- Adding public runtime tuning options.
- Adding true async file I/O.
- Changing public storage formats or recovery protocol.
- Narrowing the publish barrier or adding WAL partitioning.

## Acceptance Gate

- Roadmap records this as the measured block-decode runtime read phase.
- `benches/v1_bench.rs` emits native-thread and inline runtime rows for
  cache-disabled persistent table point reads.
- The benchmark rows assert they exercised table data-block reads and
  disabled-cache misses.
- Verification includes the benchmark command that prints the new rows.
- Focused tests, formatting, clippy, full tests, `git diff --check`, and
  forbidden-term scan pass.
- Evidence records benchmark observations, interpretation, remaining async
  blockers, and the recommended next phase.

## Active Task Slice

```text
task363 [x] goal:start measured block-decode runtime read phase | scope:current roadmap | verify:manual
task364 [x] goal:add runtime-mode benchmark rows for block decode reads | scope:benches/v1_bench.rs | verify:bench output
task365 [x] goal:verify benchmark assertions and existing read/block behavior | scope:benches src | verify:targeted/full tests
task366 [x] goal:record measured evidence and commit | scope:.phrase/evidence.md current roadmap benches | verify:git status
```

## Known Blockers

- Block decode reads owned completions but still runs synchronously on the
  calling thread; it does not yet await through the runtime owned-read boundary.
- Table header/footer metadata reads still use the borrowed `read_exact_at`
  path.
- Range and prefix cursor advancement still expose async compatibility wrappers
  over synchronous iterator advancement.
- Database read decode paths still use blocking table/blob readers.
- True async file I/O is not implemented.
- Runtime tuning options are still internal.
- True multi-writer execution still needs writer-local deltas and WAL
  partitioning.
- Public recovery report reads remain standalone no-runtime helpers by design.

## Evidence

- Phase 87 introduced `BlockReadSource::read_exact_at_owned`, returning an
  `Arc`-backed `StorageReadBuffer`, and routed both block decode entry points
  through owned completions.
- Phase 87 kept synchronous decode callers decoupled from the runtime blocking
  queue, consistent with the earlier native-file blocking adapter rule.
- Fresh measurement is needed before changing async cursor advancement because
  today the owned read seam exists but decode is still synchronous.
- `cargo bench --bench v1_bench` printed:
  native runtime block decode read: 2048 ops in 6153us; inline runtime block
  decode read: 2048 ops in 6225us.
- The benchmark disables the block cache, uses small table blocks, and asserts
  data-block reads plus disabled-cache misses, so the rows measure real table
  block load/decode work.
- Inline runtime requires `background_worker_count = 0` for persistent writable
  opens because it does not support background worker threads; the benchmark
  now configures that explicitly.
- Verification passed: `cargo bench --bench v1_bench`,
  `cargo test block --lib`, `cargo test storage --lib`,
  `cargo test table --lib`, `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan outside the repository instruction file.

## Next Recommendation

- Start the async cursor advancement phase. The measurement shows runtime mode
  does not materially change today's synchronous block-decode cost, so the next
  slice should add an explicit awaitable advancement shape for async range and
  prefix callers while preserving the current synchronous iterator path.
