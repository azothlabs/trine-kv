# Current Phase

## Status

Complete

## Goal

Finish the remaining async/write-path tails that can be completed without
adding a platform-specific native async file-I/O backend.

## Scope

- Move WAL shard lanes behind bounded front-door worker queues.
- Keep one commit record on one WAL shard and preserve existing recovery merge.
- Split transaction commit into serialized read validation/sequence assignment,
  WAL accept outside the publish barrier, and later memory publication.
- Move memtable publication and post-commit freeze out of the global publish
  barrier and under a narrower memtable publish lock.
- Keep public flush freeze ordered with memtable publication.
- Route table-open header/footer metadata reads through owned read buffers.
- Expose the WAL queue capacity through stats.

## Out Of Scope

- Adding public WAL queue/shard tuning options.
- Changing WAL frame format, table/blob/manifest formats, MVCC semantics,
  compaction behavior, or public API names.
- Adding platform-native async file I/O such as io_uring, kqueue, or Windows
  overlapped I/O.

## Acceptance Gate

- WAL front-door append/persist/rewrite commands run through bounded lane
  workers.
- Transaction writes reserve their sequence under the publish barrier but append
  WAL outside that barrier.
- Memtable publication and post-commit freeze no longer hold the global publish
  barrier.
- Public flush captures and freezes active memtables under the memtable publish
  lock so newer commits cannot slip into an older flush boundary.
- Table open metadata reads use owned read buffers, not borrowed caller buffers.
- Focused WAL, commit, flush, table-open, persistent WAL, async tests,
  formatting, clippy, full tests, `git diff --check`, and forbidden-term scan
  pass.
- Evidence records the remaining platform-native async I/O boundary honestly.

## Active Task Slice

```text
task440 [x] goal:start async/write-path tail phase | scope:current roadmap | verify:manual
task441 [x] goal:add bounded WAL lane workers | scope:src/wal.rs src/stats.rs src/db.rs | verify:wal tests
task442 [x] goal:accept transaction WAL outside publish barrier | scope:src/db/commit.rs | verify:commit tests
task443 [x] goal:narrow memtable publication lock | scope:src/db.rs src/db/commit.rs | verify:flush and persistent tests
task444 [x] goal:use owned table-open metadata reads | scope:src/table.rs | verify:table metadata test
task445 [x] goal:record evidence and run final verification | scope:.phrase src tests | verify:full gate
```

## Known Blockers

- True platform-native async file I/O is still not implemented. The native-file
  backend continues to use the bounded runtime blocking adapter for async-shaped
  reads and writes.

## Evidence

- Phase 102 left WAL shard lanes synchronous, transaction WAL accept inside the
  validation barrier, memtable publication/freeze under the global publish
  barrier, and table-open metadata on borrowed reads.
- WAL lanes now own worker threads with bounded queues; callers submit commands
  and wait for typed results.
- Transaction commit now validates reads and reserves sequence under the
  publish barrier, then accepts WAL outside that barrier before memory
  publication.
- Memtable publication and post-commit freeze now run under
  `memtable_publish_lock`; public flush takes that lock before capturing and
  freezing its visible boundary.
- Table open header/footer reads now use `read_exact_at_owned`.
- Focused tests passed: `cargo test wal_front_door --lib`,
  `cargo test commit --lib`, `cargo test flush --lib`,
  `cargo test table_open_metadata_reads_use_owned_source --lib`, selected
  persistent WAL recovery/table tests, and `cargo test --test async_api`.
- Final gate passed: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `git diff --check`, and the
  forbidden-term scan outside instruction files.

## Next Recommendation

- Treat platform-native async file I/O as a separate backend phase with an ADR
  or protocol update, because it requires platform-specific execution support
  beyond the current portable runtime boundary.
