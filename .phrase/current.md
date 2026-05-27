# Current Phase

## Status

Active

## Goal

Define the async-first public API and portable storage boundary before changing
open, read, write, cursor, durability, runtime, or backend implementation.

## Scope

- Specify the primary async `Db`, `Bucket`, `Transaction`, and cursor API.
- Specify the native blocking API as an adapter over the async engine.
- Specify storage backend capabilities and typed unsupported-capability errors.
- Specify manifest publish as a backend protocol operation.
- Specify durability mapping through backend capabilities.
- Specify cancellation rules for async write and maintenance futures.
- Specify WASM readiness constraints for memory, WASI, and browser-style
  backends.
- Link the new protocol from the v1 protocol, decision framework, and roadmap.
- Record the implementation relationship between async-first storage work and
  the no-global-lock foreground write path.

## Out Of Scope

- Rust behavior changes in this phase.
- Choosing a concrete async runtime crate.
- Implementing WASI or browser persistence in this phase.
- Promising identical durability strength on every platform.
- Making CPU-only LSM search, decode, MVCC, or merge logic async.
- Changing SSTable, WAL, manifest, blob, compaction, or transaction semantics
  beyond the API/storage boundary rules recorded here.
- Implementing async API migration and no-global-lock write-path changes in one
  slice.

## Acceptance Gate

- `.phrase/protocol/async-first-portable-storage-and-wasm.md` exists and
  covers async API shape, blocking adapter, storage capabilities, manifest
  publish, durability mapping, cancellation, background work, backend families,
  recovery, observability, tests, and staging.
- `.phrase/protocol/trine-kv-v1-spec.md` links to the new protocol and updates
  API/storage/durability/cursor/error/test/benchmark language.
- `.phrase/decision.md` records async-first storage and WASM readiness as
  durable boundaries.
- `.phrase/roadmap.md` records Phase 45 and marks Phase 44 complete.
- The async-first and foreground write-path protocols both state the staged
  implementation relationship.
- Markdown/spec checks pass where applicable.

## Active Task Slice

```text
task163 [x] goal:read current decision context | scope:.phrase decision/roadmap/current | verify:manual
task164 [x] goal:inspect v1 API/storage/durability sections | scope:v1 protocol | verify:manual
task165 [x] goal:write async-first portable storage protocol | scope:.phrase/protocol | verify:markdown review
task166 [x] goal:link durable source-of-truth docs | scope:v1 protocol decision roadmap current | verify:git diff
task167 [x] goal:close evidence and checks | scope:.phrase/evidence.md changed docs | verify:git diff --check and scans
task168 [x] goal:record async/write-path implementation relationship | scope:async protocol lock-free protocol v1 current evidence | verify:git diff
```

## Known Blockers

- No Rust implementation has started for this phase.
- The exact async runtime boundary and trait signatures still need code-level
  design during implementation.
- WASI and browser persistent backends require separate capability probes and
  fixtures before they can be accepted.
- Existing synchronous public API code will need a staged compatibility plan.
- Async API/storage migration and no-global-lock write-path changes must stay
  as separate implementation slices even though they share the final design.

## Evidence

- Current decision framework, roadmap, current phase, and v1 protocol were read.
- The v1 API shape was still synchronous at the persistent database boundary.
- Persistent storage language assumed native local files in places that should
  be backend capabilities.
- Async-first storage is required for future WASM support because persistent
  browser-style storage cannot safely depend on blocking calls.
- Async-first storage and no-global-lock writes are co-designed, but the first
  implementation must isolate API/storage/cancellation changes from commit
  visibility, WAL sharding, and delta publication changes.

## Next Recommendation

- Review and accept the async-first portable storage protocol, then implement a
  first compatibility slice that introduces async primary handles while keeping
  the current native behavior reachable through a blocking adapter.
