# Current Phase

## Status

In Progress (group commit landed; WAL shard-count default decision pending user)

## Goal

Solve the single-key durable-write throughput bottleneck (~500 ops/s, fsync
floor) with group commit: the WAL lane worker batch-drains queued commits and
serves the whole batch with one fsync, amortizing fsync cost across concurrent
(or async-pipelined) writers without weakening the per-confirmed-write
durability contract.

## Design Assessment

The single-threaded synchronous number (~500 ops/s) is the fsync latency floor
(on macOS the std `sync_data`/`sync_all` use `F_FULLFSYNC`, ~2ms); it is correct
durability cost, not an architecture bug, and cannot be beaten for a lone
serial writer. The real lever is throughput under concurrency/pipelining.

The WAL lane worker already had the infrastructure (per-shard worker thread,
command queue, per-commit completion replies, persist coalescing). What was
missing: the worker processed one command per `recv()`. Now it blocks for one
command, drains all immediately-queued commands (`try_recv`), appends every
frame buffered (no per-frame fsync), issues one fsync at the strongest requested
durability, and completes every waiter. A buffered append marks the lane dirty
so a later persist still fsyncs.

Finding: with the default 4 WAL shards, sequence round-robin spreads concurrent
commits across lanes, and on single-device storage (fsyncs serialize at the
device) batching cannot engage. So `wal_shard_count` is now configurable; 1 lane
lets group commit coalesce.

## Scope

- `process_wal_lane_batch` / batch-drain in `run_wal_lane_worker` (`src/wal.rs`).
- Configurable `DbOptions::wal_shard_count` (runtime-only; WAL files discovered
  by name, no manifest/format change). Default kept at 4 pending decision.
- Concurrent group-commit benchmark (1 vs 4 shards x concurrency).

## Out Of Scope

- Weakening durability (each waiter still returns only after its covering fsync).
- Changing WAL frame format, replay, torn-tail, or recovery behavior.
- A commit-delay window for bursty single writers (possible follow-up).

## Acceptance Gate

- Concurrent durable writers amortize fsyncs and exceed the single-writer floor.
  Met: 1-shard x8 = 4.0 commits/persist, ~1420 ops/s vs ~400 single (3.5x).
- Durability contract preserved; WAL/recovery/persistent tests pass. Met.
- Full local gate. Met (one pre-existing background-timing flake, passes in
  isolation, unrelated to the WAL commit path).

## Evidence

- `group commit sync-data` benchmark (TRINE_BENCH_RUNS=3):
  - 1-shard x1: 256 persists / 256 commits, ~400 ops/s (fsync floor).
  - 1-shard x8: 511 persists / 2048 commits (4.0 commits/fsync), ~1420 ops/s.
  - 4-shard x8: ~1982 persists / 2048 commits (~1.03, no batching), ~527 ops/s.
- macOS `F_FULLFSYNC` confirmed as the per-commit cost (`std` sync_data/sync_all).

## Known Risks / Open Decision

- WAL shard-count default: evidence says 1 is far better for single-device
  (embedded) under concurrency; >1 only helps where fsyncs parallelize. Default
  left at 4 for now; flipping to 1 is a durable boundary to confirm with the
  user. Changing it is recovery-safe (WAL discovery handles any prior count).

## Next Recommendation

- Decide the default `wal_shard_count` (recommend 1 for the embedded target).
- Optional: bounded commit-delay window to help bursty single writers.
- Then return to remembered layered-filter Phase 3/4/5.
