# Current Phase

## Status

Complete

## Goal

Add async table and blob read helpers over Trine storage traits so the next
browser read-only persistent open slice can load manifest-referenced table and
blob objects without native blocking storage wrappers.

## Scope

- Phase 125: async table/blob read boundary.

## Out Of Scope

- Persistent `Db::open` browser wiring.
- Browser writer lease protocol.
- Recovery-report and cleanup conversion.
- WAL append/front-door or WAL rewrite conversion.
- Optimizing browser blob indexed reads to avoid whole-file reads.
- Changing storage formats, manifest format, WAL format, durability semantics,
  or MVCC behavior.

## Backend Boundary Receipt

- Trine operation names: async table read, async table listing, async blob read,
  async blob listing, async blob indexed value read.
- Owned interface: `StorageReadBackend`, `StorageObjectListBackend`,
  `StorageReadObject`, and `StorageFuture`.
- Chosen backend: no new dependency. Existing native, memory, and browser OPFS
  storage backends enter through the same storage traits.
- Known backend limits: read-only browser open still needs recovery-report and
  cleanup decisions; writable browser open still needs WAL append/front-door,
  WAL rewrite, and writer lease.
- Leak-check scope: public API, docs, protocol, and phase text keep Trine-owned
  backend terminology; OPFS remains an implementation choice for the browser
  backend.
- Verification gate: native checks, WASI checks, browser checks, focused storage
  checks, full tests, formatting, clippy, diff check, forbidden-term scan,
  project-name scan, and backend-name leakage scan.

## Acceptance Gate

- Async table read helper can load a table through `StorageReadBackend`.
- Async table listing helper can enumerate table ids through
  `StorageObjectListBackend`.
- Async blob read/list helpers can load blob files, properties, and indexed
  values through storage traits.
- Existing native blocking table/blob behavior remains unchanged.
- Evidence records remaining browser open blockers.

## Active Task Slice

```text
task523 [x] goal:add async table read/list helpers | scope:src/table.rs | verify:table tests
task524 [x] goal:add async blob read/list helpers | scope:src/blob.rs | verify:blob tests
task525 [x] goal:update async read evidence | scope:.phrase docs | verify:docs diff
task526 [x] goal:commit Phase 125 | scope:git | verify:git commit
```

## Known Blockers

- Browser persistent open still returns `UnsupportedBackend`.
- Recovery-report, cleanup, WAL append/front-door, WAL rewrite, and browser
  writer lease remain incomplete.
- Async table read may initially load full table bytes for non-native backends
  instead of preserving native lazy table metadata reads.
- Async indexed blob reads may initially read whole blob files for non-native
  backends.

## Evidence

- Phase 121 allowed browser storage futures and handles to be thread-local.
- Phase 122 moved manifest read/publish/open/create onto async storage helpers.
- Phase 123 moved WAL recovery read/discovery onto async storage helpers.
- Phase 124 added a browser OPFS storage backend behind Trine storage traits.
- Persistent open still calls synchronous table/blob helpers through
  `NativeFileBackend`.

## Next Recommendation

- Move read-only browser persistent open to the async manifest/WAL/table/blob
  path, then decide recovery-report and cleanup handling before writable lease
  work.
