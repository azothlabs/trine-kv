# Current Phase

## Status

Complete

## Goal

Prepare the storage backend boundary for a real browser persistent backend by
allowing browser-only storage futures and objects to be thread-local while
keeping native and WASI storage implementations thread-safe.

## Scope

- Phase 121: browser storage thread-bound boundary.

## Out Of Scope

- IndexedDB or OPFS implementation.
- Browser writer lease protocol.
- Atomic browser manifest publish.
- Replacing persistent database open, WAL, table, blob, recovery, or cleanup
  paths with async storage calls.
- Changing storage formats, durability semantics, MVCC behavior, or public data
  model behavior.

## Backend Boundary Receipt

- Trine operation names: storage futures, read objects, read backends, append
  objects, writer leases, and the existing manifest/WAL/table/blob/recovery
  storage traits.
- Owned interface: `StorageFuture`, `StorageReadFuture`,
  `StorageReadObject`, `StorageReadBackend`, `StorageAppendObject`, and
  `StorageWriterLeaseBackend::WriterLease`.
- Chosen backend: no new browser backend in this slice. Native and WASI keep
  `Send`/`Sync` storage bounds; `wasm32-unknown-unknown` may use thread-local
  storage futures and objects.
- Known backend limits: browser persistence still lacks an IndexedDB/OPFS
  adapter, writer lease, atomic manifest publish, and async persistent open path.
- Leak-check scope: storage API names, docs, protocol, and error boundaries
  must keep Trine-owned terminology and must not expose implementation-library
  names as the public abstraction.
- Verification gate: native checks, WASI checks, browser library check, tests,
  formatting, clippy, diff check, forbidden-term scan, project-name scan, and
  backend-name leakage scan.

## Acceptance Gate

- `StorageFuture` remains `Send` on native and WASI targets.
- `StorageFuture` does not require `Send` on `wasm32-unknown-unknown`.
- Storage object/backend trait bounds keep `Send`/`Sync` on native and WASI.
- Storage object/backend trait bounds allow thread-local browser
  implementations on `wasm32-unknown-unknown`.
- Existing native, WASI, and browser target checks keep passing.
- Evidence records that this only removes a boundary blocker; browser
  persistence itself remains unsupported until the async engine path and backend
  are implemented.

## Active Task Slice

```text
task507 [x] goal:conditionalize storage future send bound | scope:src/storage.rs | verify:cargo check targets
task508 [x] goal:conditionalize storage trait thread bounds | scope:src/storage.rs | verify:cargo check targets
task509 [x] goal:update browser persistence evidence | scope:.phrase docs | verify:docs diff
task510 [x] goal:commit Phase 121 | scope:git | verify:git commit
```

## Known Blockers

- Browser persistence still requires a true async browser object store, reliable
  writer lease, atomic manifest publish, and an async persistent open path.
- Current persistent database open, WAL, manifest, table, blob, recovery, and
  cleanup paths still call blocking storage adapters around `NativeFileBackend`;
  that cannot be used as a real browser persistent backend on the browser main
  thread.

## Evidence

- Prior verification: `wasm32-unknown-unknown` library compilation passes, so
  the browser blocker is backend integration and persistent-engine async wiring,
  not basic target compilation.
- Change: `StorageFuture` is non-`Send` only on `wasm32-unknown-unknown`, while
  native and WASI keep `Send`; storage read/backend/append/lease bounds now use
  target-aware internal marker traits.
- Browser compile proof: a `wasm32-unknown-unknown`-only storage object using
  `Rc<Cell<_>>` implements `StorageReadObject`, proving browser storage handles
  no longer need to be `Send`/`Sync`.
- Audit: persistent paths still reference `NativeFileBackend` and
  `BlockingStorage*` APIs across `db`, `wal`, `manifest`, `table`, `blob`, and
  `recovery`.
- Verification: `cargo check`, `cargo check --target wasm32-wasip2 --lib`,
  `cargo check --target wasm32-wasip2 --tests`, `cargo check --target
  wasm32-unknown-unknown --lib`, `cargo check --target wasm32-unknown-unknown
  --tests`, `cargo test --all-targets --all-features`, `cargo clippy
  --all-targets --all-features -- -D warnings`, `cargo fmt --check`, `git diff
  --check`, forbidden-term scan excluding local agent instructions, and diff
  backend-name leakage scan pass.

## Next Recommendation

- Start the next browser persistence slice by replacing one persistent subsystem
  at a time with async storage operations before wiring IndexedDB/OPFS.
