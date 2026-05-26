# Current Phase

## Status

Complete

## Goal

Make the write path match the v1 LSM shape by adding immutable memtables,
size-triggered active memtable freeze, pressure-triggered flush, and bounded WAL
replay after flush.

## Entry Condition

- Phase 15 point-read hot-path work is committed.
- User audit identifies active-only memtables, manual-only flush, and unbounded
  WAL replay as the next P1 production risks.

## Scope

- Add a per-keyspace immutable memtable queue.
- Freeze active memtables when `write_buffer_bytes` is reached.
- Include immutable memtables in point reads, transaction validation, range
  scans, prefix scans, and range tombstone checks.
- Make `Db::flush()` consume immutable memtables; manual flush first freezes
  current active memtables.
- Use `max_immutable_memtables` as write-side pressure: flush queued immutable
  memtables before accepting the next write when the queue is full.
- Advance the manifest WAL replay floor only after SSTables are published, then
  atomically rewrite the WAL to keep only newer batches.

## Out Of Scope

- File-backed SSTable block loading and table-cache design.
- Background worker scheduling.
- Splitting flush or compaction output by `target_table_bytes`.
- Replacing the exact filter set with probabilistic Bloom bits.
- Implementing Eytzinger or galloping layouts beyond existing policy names.
- Changing public API or on-disk record formats.

## Acceptance Gate

- Active memtables freeze into immutable memtables when the configured write
  buffer threshold is reached.
- Reads, transactions, range scans, and prefix scans include immutable
  memtables before SSTables.
- Flush consumes immutable memtables and manual flush first freezes current
  active memtables.
- Immutable memtable pressure is handled before accepting the next write, so
  storage errors do not leave a new write half-reported.
- Manifest WAL replay floor advances only after flushed SSTables are published,
  and the WAL is atomically rewritten so startup does not decode indefinitely
  old flushed batches.
- Local verification passes for formatting, clippy, full tests, examples,
  Windows target check, and `git diff --check`.

## Active Task Slice

```text
task052 [x] goal:immutable memtable queue participates in all MVCC reads and flush | scope:src/db.rs,src/db/commit.rs,tests,.phrase | verify:targeted persistent and transaction tests
task053 [x] goal:WAL replay floor is paired with safe WAL rewrite after flushed tables publish | scope:src/wal.rs,src/db.rs,src/recovery.rs,tests,.phrase | verify:WAL size/checkpoint/reopen tests
```

## Known Blockers

- Auto-flush after a commit cannot report I/O failure without making the just
  acknowledged write ambiguous. This phase handles immutable pressure before
  accepting the next write and keeps explicit `flush()` as the synchronous
  maintenance surface.
- Background workers remain a later phase; no hidden worker thread should be
  introduced in this slice.
- GitHub Actions cannot be executed locally; remote CI must run after push.

## Evidence To Record

- `persistent_write_buffer_freezes_active_memtable_and_reads_immutable` proves
  size-triggered freeze and point/range/prefix reads from immutable memtables.
- `persistent_immutable_range_tombstone_hides_point_records` proves immutable
  range tombstones participate in visibility.
- `persistent_transaction_conflict_checks_immutable_memtables` proves
  transaction validation checks immutable memtable records.
- `persistent_flush_writes_table_and_reopen_can_skip_wal` proves manual flush
  freezes active memtables and removes flushed batches from the WAL.
- `persistent_immutable_pressure_flushes_before_next_write_and_keeps_new_wal_batch`
  proves write-side pressure flushes queued immutable memtables before the next
  write and keeps newer unflushed WAL batches for replay.
- `wal_decode_after_floor_skips_old_operation_payloads` proves replay-floor
  recovery does not rebuild operation lists for already flushed batches.
- Full local gate passed: `cargo fmt --check`, `cargo clippy --all-targets
  --all-features -- -D warnings`, `cargo test --all-targets --all-features`,
  all examples, `cargo check --target x86_64-pc-windows-gnu`, and
  `git diff --check`.

## Next Recommendation

- If this phase passes, move to file-backed SSTable block loading and table
  cache so block cache memory can replace startup-time full table loading.
