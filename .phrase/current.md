# Current Phase

## Status

Complete

## Goal

Finish browser writable persistent support without accepting any mutation path
that can silently skip WAL, manifest, or maintenance guarantees.

## Scope

- Phase 127: browser writable storage foundation.
- Phase 128: browser writable database open, WAL-backed async mutation, and
  cancellation-safe async maintenance entry points.

## Out Of Scope

- Changing storage formats, manifest format, WAL format, MVCC behavior,
  transaction behavior, or compaction selection.
- Claiming native strict sync durability for browser storage.
- Adding a second browser-only engine path outside the storage contracts.

## Backend Boundary Receipt

- Trine operation names: append WAL object, rewrite WAL object after replay
  floor, acquire writer lease, publish manifest, write table/blob/recovery
  objects, repair safe temporary files, delete obsolete objects, and open a
  writable persistent database asynchronously.
- Owned interface: storage capabilities, async storage traits, WAL async
  helpers, manifest async helpers, recovery async helpers, and `Db::open_async`.
- Chosen backend: browser persistent storage uses the existing OPFS-backed
  storage implementation on `wasm32-unknown-unknown`.
- Known backend limits: browser storage accepts `Buffered` and `Flush`; it
  rejects `SyncData` and `SyncAll` because native strict sync guarantees are
  unavailable through this backend.
- Leak-check scope: public API, docs, protocol, and phase text keep Trine-owned
  backend terminology; OPFS remains an implementation detail of the browser
  backend.
- Verification gate: native checks, WASI checks, browser checks, focused WAL
  tests, full lib tests, formatting, clippy, diff check, forbidden-term scan,
  project-name scan, and backend-name leakage scan.

## Acceptance Gate

- Browser storage implements append, WAL rewrite, object writes, and writer
  lease through storage traits.
- Browser writer lease uses Web Locks and fails clearly when Web Locks are
  missing or already held.
- Browser storage reports only honest durability capabilities.
- Writable browser `Db::open_async` acquires the writer lease, repairs safe
  temporary files, opens/creates manifest state through browser storage, creates
  the default bucket when allowed, replays WAL, and attaches an async WAL front
  door.
- Browser async writes append WAL before publishing memtable deltas.
- Browser async writes are internally owned after acceptance so dropping the
  caller future does not interrupt WAL append, memtable publication, or commit
  slot termination.
- Browser named bucket creation uses async manifest publish.
- Browser `flush_async`, `compact_range_async`,
  `compact_range_with_budget_async`, and `run_maintenance_with_budget_async`
  own side-effecting maintenance work after acceptance.
- Browser synchronous mutation and synchronous maintenance paths fail clearly
  instead of bypassing async storage guarantees.

## Active Task Slice

```text
task531 [x] goal:add browser storage append/WAL rewrite/writer lease traits | scope:src/storage.rs | verify:browser/native checks
task532 [x] goal:add async WAL append/rewrite helpers | scope:src/wal.rs | verify:async WAL tests
task533 [x] goal:audit Db writable open native dependencies | scope:src/db.rs src/manifest.rs src/wal.rs src/recovery.rs | verify:documented blockers or patch plan
task534 [x] goal:add async recovery repair helpers | scope:src/recovery.rs | verify:async recovery repair test
task535 [x] goal:update evidence and roadmap | scope:.phrase docs | verify:docs diff
task536 [x] goal:open writable browser persistent DB through async storage | scope:src/db.rs src/manifest.rs src/recovery.rs | verify:wasm/native checks
task537 [x] goal:attach browser WAL front door and route async commits through WAL append | scope:src/db/commit.rs src/wal.rs | verify:wasm/native checks
task538 [x] goal:make browser sync mutation paths reject instead of bypassing WAL | scope:src/db/commit.rs src/db.rs | verify:wasm/native checks
task539 [x] goal:create named buckets through async browser manifest publish | scope:src/db.rs src/manifest.rs | verify:wasm/native checks
task540 [x] goal:add async table/blob write building blocks behind storage traits | scope:src/table.rs src/blob.rs | verify:wasm/native checks
task541 [x] goal:make browser flush cancellation-safe before exposing it | scope:src/db.rs src/manifest.rs src/table.rs src/blob.rs | verify:wasm/native checks
task542 [x] goal:make browser compaction and blob cleanup budgeted and resumable | scope:src/db.rs src/manifest.rs src/table.rs src/blob.rs | verify:wasm/native checks
```

## Known Blockers

- Browser write preflight cannot run async maintenance itself; under write
  pressure it returns `RuntimeBusy` and the caller must run async maintenance.
- Browser read paths still use full-object table/blob helpers in places where a
  smaller async read would be better.
- This phase proves compile-time browser integration; an in-browser persistence
  fixture remains useful follow-up evidence.

## Evidence

- Phase 121 allowed browser storage futures and handles to be thread-local.
- Phase 122 moved manifest read/publish/open/create onto async storage helpers.
- Phase 123 moved WAL recovery read/discovery onto async storage helpers.
- Phase 124 added a browser OPFS storage backend behind Trine storage traits.
- Phase 125 added async table/blob read helpers through storage traits.
- Phase 126 wired browser read-only `Db::open_async` through OPFS, async
  manifest/WAL/table/blob reads, and async recovery validation.
- Phase 127 added browser storage append, WAL rewrite, writer lease trait
  support, async WAL append/rewrite helpers, and async recovery repair helpers.
- Phase 128 wires writable browser `Db::open_async`, browser manifest
  open/create, default bucket manifest creation, safe temporary repair, Web
  Locks writer lease, WAL replay, browser WAL front door creation, async write
  WAL append, browser WAL persist, and named bucket async manifest creation.
- Phase 128 also wires cancellation-safe browser async flush, compaction,
  maintenance budgets, WAL rewrite after flush, obsolete table cleanup, pending
  blob cleanup, and blob GC through browser storage traits.
- Phase 128 keeps synchronous browser mutation and maintenance APIs rejected.

## Next Recommendation

- Add an in-browser persistence fixture and then tune browser table/blob read
  granularity if measurements show it matters.
