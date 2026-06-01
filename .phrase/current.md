# Current Phase

## Status

Complete

## Goal

Move WAL recovery reads and shard discovery onto async storage-trait helpers so
the browser persistent path has an async log-reading boundary after manifest.

## Scope

- Phase 123: async WAL recovery read boundary.

## Out Of Scope

- Async WAL append/front-door workers.
- WAL rewrite conversion.
- IndexedDB or OPFS implementation.
- Browser writer lease protocol.
- Async table, blob, recovery-report, cleanup, or full persistent database open.
- Changing WAL format, merge ordering, durability semantics, or commit behavior.

## Backend Boundary Receipt

- Trine operation names: WAL object read, WAL shard discovery, WAL batch decode,
  and WAL recovery stream read.
- Owned interface: `StorageObjectReadBackend`, `StorageDirectoryListBackend`,
  `StorageFuture`, WAL storage object ids, and WAL stream merge helpers.
- Chosen backend: no new browser backend in this slice. Native WAL recovery read
  gains async helpers over the existing storage traits; browser will later
  provide the same trait boundary.
- Known backend limits: WAL append/front-door/rewrite, table/blob/recovery,
  writer lease, atomic manifest publish proof, and persistent open wiring remain
  incomplete for browser persistence.
- Leak-check scope: WAL APIs/docs/protocol must keep Trine-owned names and must
  not expose implementation-library names as the abstraction.
- Verification gate: native checks, WASI checks, browser checks, focused WAL
  tests, full tests, formatting, clippy, diff check, forbidden-term scan,
  project-name scan, and backend-name leakage scan.

## Acceptance Gate

- WAL object read has an async storage-trait helper.
- WAL shard discovery has an async storage-trait helper.
- WAL recovery streams can be read through async storage helpers.
- Async WAL discovery preserves ordering and malformed-name validation.
- Async WAL batch read preserves replay-floor filtering.
- Existing synchronous WAL behavior remains unchanged.
- Evidence records that this is only WAL recovery reads, not async WAL writes or
  a complete browser persistent backend.

## Active Task Slice

```text
task515 [x] goal:add async WAL object read helper | scope:src/wal.rs | verify:wal tests
task516 [x] goal:add async WAL discovery and stream helpers | scope:src/wal.rs | verify:wal tests
task517 [x] goal:update async WAL evidence | scope:.phrase protocol | verify:docs diff
task518 [x] goal:commit Phase 123 | scope:git | verify:git commit
```

## Known Blockers

- WAL append/front-door workers still use blocking append objects and worker
  threads.
- Persistent database open still calls synchronous WAL recovery helpers.
- Table, blob, recovery-report, and cleanup paths still rely on blocking storage
  adapters around `NativeFileBackend`.
- Browser persistence still requires a true async browser object store, reliable
  writer lease, atomic manifest publish, and async persistent open path.

## Evidence

- Phase 122 moved manifest read/publish/open/create onto async storage helpers.
- Persistent path audit still points at WAL recovery read/discovery as one of
  the blocking subsystems that must be converted before browser persistence can
  be real.
- Change: WAL object reads, WAL shard discovery, and WAL recovery streams now
  have async storage-trait helpers.
- Async WAL discovery preserves legacy/shard ordering and malformed-name
  validation through the shared path parser.
- Async WAL batch read preserves replay-floor filtering.
- Verification: `cargo test wal::tests`, `cargo check`, `cargo check --target
  wasm32-wasip2 --lib`, `cargo check --target wasm32-wasip2 --tests`, `cargo
  check --target wasm32-unknown-unknown --lib`, `cargo check --target
  wasm32-unknown-unknown --tests`, `cargo test --all-targets --all-features`,
  `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt
  --check`, `git diff --check`, forbidden-term scan excluding local agent
  instructions, project-name diff scan, and backend-name diff scan pass.

## Next Recommendation

- Start the next browser persistence slice by converting the next open/recovery
  subsystem.
