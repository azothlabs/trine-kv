# Current Phase

## Status

Complete

## Goal

Move the manifest subsystem onto a true async storage boundary so browser
persistence has an async metadata read/publish path before the wider persistent
engine is converted.

## Scope

- Phase 122: async manifest storage boundary.

## Out Of Scope

- IndexedDB or OPFS implementation.
- Browser writer lease protocol.
- Async WAL, table, blob, recovery, cleanup, or full persistent database open.
- Replacing the synchronous manifest call sites in `Db::open` yet.
- Changing manifest format, durability semantics, recovery semantics, or bucket
  behavior.

## Backend Boundary Receipt

- Trine operation names: manifest read, manifest publish, manifest open/create,
  and manifest bucket creation.
- Owned interface: `StorageManifestReadBackend`,
  `StorageManifestPublishBackend`, `StorageFuture`, `ManifestStore`, and
  manifest storage object ids.
- Chosen backend: no new browser backend in this slice. Native manifest storage
  gains async read/publish helpers over the existing storage traits; browser
  will later provide the same trait boundary.
- Known backend limits: browser persistence still lacks IndexedDB/OPFS,
  writer lease, atomic manifest publish proof, async WAL/table/blob/recovery
  paths, and async persistent open wiring.
- Leak-check scope: manifest APIs/docs/protocol must keep Trine-owned names and
  must not expose implementation-library names as the abstraction.
- Verification gate: native checks, WASI checks, browser checks, focused
  manifest tests, full tests, formatting, clippy, diff check, forbidden-term
  scan, project-name scan, and backend-name leakage scan.

## Acceptance Gate

- Manifest read has an async storage-trait helper.
- Manifest publish has an async storage-trait helper.
- `ManifestStore` can open/create through the async manifest helpers.
- At least one manifest edit can publish through the async helper without
  advancing in-memory state before durable publish succeeds.
- Existing synchronous manifest behavior remains unchanged.
- Evidence records that this is only the manifest boundary, not a complete
  browser persistent backend.

## Active Task Slice

```text
task511 [x] goal:add async manifest read/publish helpers | scope:src/manifest.rs | verify:manifest tests
task512 [x] goal:add async ManifestStore open/create and bucket publish | scope:src/manifest.rs | verify:manifest tests
task513 [x] goal:update async manifest evidence | scope:.phrase protocol | verify:docs diff
task514 [x] goal:commit Phase 122 | scope:git | verify:git commit
```

## Known Blockers

- Current persistent database open still calls synchronous manifest helpers.
- WAL, table, blob, recovery, and cleanup paths still rely on blocking storage
  adapters around `NativeFileBackend`.
- Browser persistence still requires a true async browser object store, reliable
  writer lease, atomic manifest publish, and async persistent open path.

## Evidence

- Prior Phase 121 evidence: browser storage futures and object handles may be
  thread-local on `wasm32-unknown-unknown`, while native and WASI keep
  thread-safe storage bounds.
- Persistent path audit still points at manifest as one of the blocking
  subsystems that must be converted before browser persistence can be real.
- Change: manifest read/publish now have async storage-trait helpers, and
  `ManifestStore` can open/create and create a bucket through the async manifest
  path.
- Failure behavior: async bucket publish keeps the in-memory manifest unchanged
  when durable publish fails, matching the synchronous path.
- Verification: `cargo test manifest::tests`, `cargo check`, `cargo check
  --target wasm32-wasip2 --lib`, `cargo check --target wasm32-wasip2 --tests`,
  `cargo check --target wasm32-unknown-unknown --lib`, `cargo check --target
  wasm32-unknown-unknown --tests`, `cargo test --all-targets --all-features`,
  `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt
  --check`, `git diff --check`, forbidden-term scan excluding local agent
  instructions, project-name diff scan, and backend-name diff scan pass.

## Next Recommendation

- Start the next browser persistence slice by converting the next persistent
  subsystem along the open/recovery path.
