# Current Phase

## Status

Complete

## Goal

Complete the native async write path so Linux `platform-io` writes await Trine
storage completions instead of running the whole commit through the bounded sync
adapter.

## Scope

- Keep WAL lane ownership as the filesystem append serialization boundary.
- Add an awaitable WAL lane completion so async writes can wait without
  spawning the whole commit into the bounded adapter.
- Route native persistent async writes through the async publish path when the
  native storage backend advertises `PlatformAsyncIo`.
- Keep native fallback writes on the existing bounded sync-adapter path.
- Verify sync/async WAL semantics, transaction writes, and stats boundaries.

## Backend Boundary Receipt

- Trine operation names: async batch write, async transaction commit, WAL append,
  WAL persist, commit sequence assignment, memtable publish, write stats.
- Owned interface: `Db::write`, transaction async commit, `DurabilitySubstrate`,
  `WalFrontDoor`, `NativeFileBackend`, `StorageCapability::PlatformAsyncIo`,
  and `DbStats` storage task counters.
- Chosen backend: existing Trine `PlatformIoDriver` for native-file
  append/persist operations when the backend advertises `PlatformAsyncIo`.
- Known backend limits: only targets whose current native-file backend reports
  true platform async I/O use this path; fallback targets keep the bounded
  adapter. Flush, compaction, manifest publish, and directory work remain later
  phases.
- Leak-check scope: no direct file append outside the WAL lane; sync and async
  writes must still share one WAL serialization boundary.
- Verification gate: focused async write/WAL tests with and without
  `platform-io`, formatting, clippy, rustdoc, and diff checks.

## Out Of Scope

- Splitting flush or compaction into async storage phases.
- Rewriting manifest publish, SSTable, blob, flush, or compaction paths.
- Changing WAL format, MVCC visibility, transaction conflict rules, or
  durability semantics.
- Adding a new platform backend or claiming new true async support on fallback
  targets.
- Publishing, tagging, pushing, or creating a GitHub release.

## Acceptance Gate

- Native persistent async writes choose the async write path only when
  `NativeFileBackend` supports `PlatformAsyncIo`.
- The async write path awaits WAL lane completion and does not spawn the whole
  commit through the bounded sync adapter on true platform async targets.
- Sync writes and native fallback async writes retain existing behavior.
- WAL records remain replayable after async writes and async transactions.
- `cargo fmt --check`, focused async/WAL tests with and without
  `platform-io`, `cargo clippy -q --all-features`,
  `cargo rustdoc --all-features -- -D warnings`, and `git diff --check` pass.

## Active Task Slice

```text
task660 [x] goal:commit completed platform routing phase | scope:git history | verify:commit fcd5cc3 exists
task661 [x] goal:add awaitable WAL lane completion | scope:src/wal.rs src/substrate.rs | verify:WAL tests
task662 [x] goal:route native platform async writes through async publish | scope:src/db/commit.rs | verify:async write stats tests
task663 [x] goal:verify and record native async write evidence | scope:tests docs phrase | verify:focused gates
```

## Evidence

- Phase 150 was committed as `fcd5cc3`.
- Existing native async writes use `WriteFuture`; when the runtime has a
  blocking adapter it spawns the entire accepted commit into the bounded
  adapter.
- WAL appends are already serialized through `WalFrontDoor` lanes. Bypassing
  those lanes from an async path would create append-offset races with sync
  writes.
- `WalFrontDoor` now has an awaitable lane completion; sync callers block on the
  same completion and async callers await it.
- Native persistent async writes use the async publish path only when
  `NativeFileBackend` advertises `PlatformAsyncIo`; fallback targets keep the
  existing `WriteFuture` bounded-adapter path.
- `cargo test -q --features platform-io --test async_api
  platform_io_async_write_awaits_wal_without_whole_commit_adapter` compiled to
  0 tests on the non-Linux host because the assertion is Linux-only, then passed
  inside a Linux Docker container on Orbstack with Docker seccomp unconfined.
- `cargo test -q`, `cargo test -q --features platform-io --test async_api`,
  `cargo test -q --features platform-io --lib wal::tests`,
  `cargo clippy -q --all-features`,
  `cargo rustdoc --all-features -- -D warnings`, `cargo fmt --check`, and
  `git diff --check` passed.

## Known Residuals

- Flush and compaction remain on their current native async fallback paths.
- Non-Linux platform-driver fallback remains fallback-classified for true async
  I/O.

## Next Recommendation

- After this phase passes, choose the next async-storage phase from fresh
  evidence: flush, compaction, or manifest/directory publish.
