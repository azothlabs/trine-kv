# V1 Write Path Persist Diagnostics

Date: 2026-06-13

## Scope

This note records Phase 177, which measured persistent write behavior and
optimized one safe WAL persist hot path without changing write durability,
storage formats, or commit visibility.

## Measurement

The benchmark now includes persistent write rows for:

- single-key puts with `Buffered`, `Flush`, `SyncData`, and `SyncAll`;
- one large persistent `WriteBatch` with the same durability levels;
- explicit `persist_sync(DurabilityMode::SyncData)` after buffered commits;
- public `flush_sync` storage-operation counters.

Before the fix, the explicit persist diagnostic used 256 buffered commits and
called `persist_sync(SyncData)` after each commit. Because the WAL front door
sent persist to every opened shard, that produced 1018 persist storage requests
and about 1.21s median persist wall time.

After the fix, each WAL lane tracks the durability level already reached by
its current bytes. Persist requests skip lanes whose WAL bytes are already at
least as durable as the requested mode. The same diagnostic produced 256
persist storage requests and about 0.49s median persist wall time.

## Interpretation

The safe optimization was skipping redundant work for clean WAL shards. It does
not make strict per-commit `SyncData` or `SyncAll` writes cheap, because those
writes still need the storage device to confirm the lane that accepted the
commit.

For applications that need high throughput with strict durability, the intended
shape remains batching writes and using explicit persist checkpoints rather
than syncing every individual key.

## Verification

- `cargo fmt --check`
- `cargo test -q wal_front_door_persists_only_dirty_shards --lib`
- `cargo test -q --lib`
- `cargo clippy --bench v1_bench -- -D warnings`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench`
- `git diff --check`
