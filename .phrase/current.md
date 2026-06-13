# Current Phase

## Status

Complete

## Goal

Revalidate the KV engine after platform-io became the full platform async
abstraction: async engine APIs should call Trine operation-level storage and
durability async boundaries, while platform-io decides whether each target uses
native async, partial native async, or thread-pool managed async below that
boundary.

## Scope

- Native persistent engine APIs:
  - write / transaction commit
  - WAL persist
  - public flush
  - compact range and budgeted compaction
  - cooperative maintenance
  - obsolete table/blob cleanup
  - close coordination
- Existing browser and object-store async paths remain separate accepted async
  backends.
- Keep OS-specific async mechanics inside platform-io/storage/substrate, not
  inside engine API decisions.

## Backend Boundary Receipt

- Engine code now selects async storage by Trine operation and storage mode,
  not by OS async primitive or `PlatformAsyncIo` capability.
- `platform-io` default/threadpool feature provides the async completion floor;
  `platform-io-native` only changes the lower driver choice.
- Engine async dependencies are:
  - WAL append / persist / rewrite through `DurabilitySubstrate`;
  - table/blob object writes through `StorageObjectWriteBackend`;
  - manifest publish through async prepared manifest publish;
  - directory sync through `StorageDirectorySyncBackend`;
  - object delete through `StorageObjectDeleteBackend`;
  - blob reads during GC through `StorageReadBackend`.
- Writer lease release during close remains a synchronous drop/lock release,
  not a storage operation. Close now performs pending cleanup through async
  storage before releasing the lease.

## Out Of Scope

- New OS backend discovery or making every native backend row
  `TruePlatformAsync`.
- Storage format changes.
- Publishing, tagging, pushing, or PR creation.

## Acceptance Gate

- Public native async write no longer uses the old whole-write runtime blocking
  adapter wrapper.
- Public native async persist no longer wraps `persist_sync`; it awaits WAL lane
  persistence.
- Public native async flush, compaction, maintenance, cleanup, and close no
  longer use `run_native_blocking_task`.
- Native compaction and blob GC write/read/delete/publish through async storage
  operations.
- Default `platform-io` and `platform-io-native` both compile and test through
  the same engine async path.
- Evidence records operation dependencies and verification.

## Active Task Slice

```text
task745 [x] goal:remove whole-write blocking wrapper | scope:src/db/commit.rs src/substrate.rs src/wal.rs | verify:cargo check/test
task746 [x] goal:route WAL persist/rewrite through async substrate | scope:src/db.rs src/substrate.rs src/wal.rs | verify:platform-io tests
task747 [x] goal:route native flush/compaction/maintenance through async storage | scope:src/db.rs | verify:async API tests
task748 [x] goal:route cleanup/blob-GC/close through async storage where storage work exists | scope:src/db.rs | verify:cleanup tests/full gate
task749 [x] goal:update evidence and roadmap | scope:.phrase | verify:rg stale wrappers
```

## Evidence

- `Db::write` / async transaction commit now starts a background native write
  future that runs `commit_write_request_async`. Poll-then-drop writes still
  complete, but the background task no longer calls the old whole-write
  synchronous wrapper.
- `Db::persist` now calls `persist_native_async`, which awaits
  `DurabilitySubstrate::persist_wal_async` and `WalFrontDoor::persist_async`.
- `Db::flush`, `Db::compact_range`, `Db::compact_range_with_budget`, and
  `Db::run_maintenance_with_budget` now use native async flush/compaction
  helpers for persistent native storage regardless of whether the lower driver
  is native or threadpool.
- Native compaction, blob GC, obsolete-table cleanup, and obsolete-blob cleanup
  now use async storage writes, reads, directory sync, deletes, and manifest
  publish.
- `run_native_blocking_task` and the engine-level
  `uses_native_platform_async_storage_path` selector were removed.

## Known Residuals

- Synchronous APIs intentionally remain synchronous.
- Background worker maintenance still uses the synchronous maintenance path; it
  already runs on owned background workers and is not part of public async API
  executor blocking.
- `DbInner::drop` keeps best-effort synchronous cleanup because Rust destructors
  cannot await.

## Next Recommendation

- With platform-io and engine revalidation complete, the next phase should be a
  full validation/packaging pass or any user-selected release/push workflow,
  rather than more async abstraction work.
