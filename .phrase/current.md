# Current Phase

## Status

Complete

## Goal

Wire read-only browser persistent open through async storage traits so OPFS can
load manifest, WAL recovery records, tables, and blobs without native blocking
storage wrappers.

## Scope

- Phase 126: read-only browser persistent open.

## Out Of Scope

- Writable browser persistent open.
- Browser writer lease protocol.
- Recovery-report repair and cleanup mutation.
- WAL append/front-door or WAL rewrite conversion.
- Optimizing browser table/blob reads after the first working open path.
- Changing storage formats, manifest format, WAL format, durability semantics,
  or MVCC behavior.

## Backend Boundary Receipt

- Trine operation names: browser persistent async open, manifest read, WAL
  recovery read, table read, blob read, table/blob listing, and safe-temporary
  detection.
- Owned interface: `Db::open_async`, storage read/list traits, manifest/WAL
  async helpers, table/blob async helpers, and recovery validation helpers.
- Chosen backend: browser persistent storage uses the existing OPFS-backed
  storage implementation on `wasm32-unknown-unknown`.
- Known backend limits: writable browser open still needs WAL append/front-door,
  WAL rewrite, strict durability decision, and writer lease.
- Leak-check scope: public API, docs, protocol, and phase text keep Trine-owned
  backend terminology; OPFS remains an implementation choice for the browser
  backend.
- Verification gate: native checks, WASI checks, browser checks, focused storage
  checks, full tests, formatting, clippy, diff check, forbidden-term scan,
  project-name scan, and backend-name leakage scan.

## Acceptance Gate

- `Db::open_async(DbOptions::browser_persistent().read_only())` uses OPFS on
  `wasm32-unknown-unknown`.
- Non-browser targets keep browser persistent open as `UnsupportedBackend`.
- Read-only browser open validates safe temporary files, referenced blobs, and
  unreferenced table/blob files through async storage traits.
- Browser read-only open replays WAL recovery streams and loads buckets from
  manifest-referenced tables.
- Writable browser open remains explicitly unsupported.

## Active Task Slice

```text
task527 [x] goal:add async recovery validation helpers | scope:src/recovery.rs | verify:recovery tests
task528 [x] goal:add browser read-only open path | scope:src/db.rs src/manifest.rs src/table.rs | verify:browser/native checks
task529 [x] goal:update read-only browser open evidence | scope:.phrase docs | verify:docs diff
task530 [x] goal:commit Phase 126 | scope:git | verify:git commit
```

## Known Blockers

- Writable browser persistent open remains unsupported.
- Browser writer lease is not implemented.
- Browser WAL append/front-door/rewrite remains incomplete.
- Browser recovery report repair and cleanup mutation remain incomplete.
- Browser read-only open may initially load full table/blob objects.

## Evidence

- Phase 121 allowed browser storage futures and handles to be thread-local.
- Phase 122 moved manifest read/publish/open/create onto async storage helpers.
- Phase 123 moved WAL recovery read/discovery onto async storage helpers.
- Phase 124 added a browser OPFS storage backend behind Trine storage traits.
- Phase 125 added async table/blob read helpers through storage traits.
- Phase 126 wired browser read-only `Db::open_async` through OPFS, async
  manifest/WAL/table/blob reads, and async recovery validation.
- Synchronous browser `Db::open` still returns `UnsupportedBackend`; writable
  browser open remains intentionally unsupported.

## Next Recommendation

- Move to writable browser WAL append/front-door/rewrite and a browser writer
  lease protocol.
