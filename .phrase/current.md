# Current Phase

## Status

Complete

## Goal

Add a real browser OPFS storage backend target behind Trine's storage traits so
browser persistence has an owned storage implementation to wire into the async
persistent engine path.

## Scope

- Phase 124: browser OPFS storage backend.

## Out Of Scope

- Persistent `Db::open` browser wiring.
- Browser writer lease protocol.
- Async table, blob, recovery-report, cleanup, WAL append/front-door, or WAL
  rewrite conversion.
- Changing storage formats, manifest format, WAL format, durability semantics,
  or MVCC behavior.

## Backend Boundary Receipt

- Trine operation names: OPFS object read, random read, object write, object
  delete, directory create, directory listing, manifest read, and manifest
  publish.
- Owned interface: `StorageReadBackend`, `StorageObjectReadBackend`,
  `StorageObjectWriteBackend`, `StorageObjectDeleteBackend`,
  `StorageDirectoryCreateBackend`, `StorageDirectoryListBackend`,
  `StorageManifestReadBackend`, `StorageManifestPublishBackend`, and
  `StorageFuture`.
- Chosen backend: browser persistent storage uses OPFS on
  `wasm32-unknown-unknown`, via a target-specific Rust OPFS adapter dependency.
- Known backend limits: browser writer lease, strict durability proof, persistent
  open wiring, WAL append/front-door/rewrite, and table/blob/recovery/cleanup
  async wiring remain incomplete.
- Leak-check scope: public API, docs, protocol, and phase text keep Trine-owned
  backend terminology; OPFS remains an implementation choice for the browser
  backend.
- Verification gate: native checks, WASI checks, browser checks, focused storage
  checks, full tests, formatting, clippy, diff check, forbidden-term scan,
  project-name scan, and backend-name leakage scan.

## Acceptance Gate

- `opfs` dependency is target-scoped to `wasm32-unknown-unknown`.
- Browser OPFS backend implements Trine storage read/object-read/write/delete,
  directory create/list, manifest read, and manifest publish traits.
- Browser OPFS backend compiles on `wasm32-unknown-unknown`.
- Native and WASI targets do not depend on OPFS code.
- Evidence records that this adds a backend target but does not complete browser
  persistent open.

## Active Task Slice

```text
task519 [x] goal:add target-scoped OPFS dependency | scope:Cargo.toml Cargo.lock | verify:wasm check
task520 [x] goal:add browser OPFS storage backend | scope:src/storage.rs | verify:wasm check
task521 [x] goal:update OPFS backend evidence | scope:.phrase protocol docs | verify:docs diff
task522 [x] goal:commit Phase 124 | scope:git | verify:git commit
```

## Known Blockers

- Browser persistent open still returns `UnsupportedBackend`.
- Browser writer lease is not implemented.
- Table, blob, recovery-report, cleanup, WAL append/front-door, and WAL rewrite
  still need async conversion or browser-safe alternatives.
- OPFS strict durability guarantees are not treated as native `SyncData` or
  `SyncAll`.

## Evidence

- Phase 121 allowed browser storage futures and handles to be thread-local.
- Phase 122 moved manifest read/publish/open/create onto async storage helpers.
- Phase 123 moved WAL recovery read/discovery onto async storage helpers.
- OPFS API evidence: MDN documents OPFS as an origin-private browser filesystem
  and `FileSystemWritableFileStream` writes through a temporary file until close.
- Rust adapter evidence: the `opfs` crate exposes directory, file, and writable
  stream traits suitable for a Trine storage backend.

## Next Recommendation

- Start the read-only browser persistent open slice by routing open/recovery
  through async manifest, WAL, table, blob, and recovery-report reads before
  adding writable browser lease support.
