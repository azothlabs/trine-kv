# Current Phase

## Status

Complete

## Goal

Close the remaining verification gap with deterministic randomized model
testing.

## Entry Condition

- Phase 29 completed blob lifecycle hardening with full local verification.
- P10 verification expansion was open.

## Scope

- Add a random operation model test against a simple MVCC reference.
- Cover point writes, point deletes, range deletes, point reads, range scans,
  snapshots, flush, compaction, and reopen.
- Keep the test deterministic and small enough for normal `cargo test`.

## Out Of Scope

- New public API.
- Storage format changes.
- Large benchmark policy decisions.
- External test services.

## Acceptance Gate

- Random operation testing compares Trine against a simple MVCC reference
  model.
- Existing crash/reopen, corruption, long scan, and benchmark gates remain in
  the verification list.
- Full local Rust verification passes.

## Active Task Slice

```text
task101 [x] goal:add deterministic MVCC model test | scope:tests | verify:focused model test
task102 [x] goal:update verification evidence and close remaining roadmap | scope:.phrase | verify:full Rust gate plus evidence
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.

## Evidence

- Phase 30 added deterministic randomized comparison against a simple MVCC
  reference model.
- The model test exposed a partial-compaction point-delete retention bug:
  deleting a key in an upper level could be cleaned too early while an older
  value still lived in a lower level.
- Partial compaction now keeps point deletes unless the whole live keyspace
  participates in the rewrite.
- Full local Rust verification passed.

## Next Recommendation

- Push to remote CI when ready. No local P0-P10 LSM hardening item remains open
  in the current roadmap.
