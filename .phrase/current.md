# Current Phase

## Status

In progress

## Goal

Build the v1 engine in measured slices without silently changing the accepted
protocol.

## Entry Condition

- Phase 2 crate scaffold is complete.
- `cargo fmt --check`, `cargo clippy`, and scaffold tests passed.
- The v1 protocol remains the implementation source of truth.

## Scope

- Implement one behavior slice at a time.
- Keep each slice aligned with MVCC, write batch, snapshot, WAL, SSTable,
  manifest, compaction, transaction, prefix-filter, compression, and
  search-policy contracts.
- Add tests before claiming a slice works.

## Out Of Scope

- Implementing multiple adjacent engine subsystems in one unverified jump.
- Changing public protocol behavior without updating the spec or an ADR.
- Adding external codec crates before codec behavior and fixtures are ready.
- Persistent crash recovery before in-memory MVCC point semantics exist.

## Acceptance Gate

- The v1 acceptance gate in `.phrase/protocol/trine-kv-v1-spec.md` passes.
- Each slice records its verification evidence and remaining blockers.

## Active Task Slice

```text
task003 [x] goal:in-memory MVCC point writes, deletes, and snapshot reads work | scope:src/db.rs,src/keyspace.rs,src/write_batch.rs,src/blob.rs,src/snapshot.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task004 [x] goal:in-memory range and prefix iteration return snapshot-consistent ordered live keys | scope:src/db.rs,src/keyspace.rs,src/iterator.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task005 [x] goal:in-memory range deletes affect point, range, and prefix reads with snapshot safety | scope:src/db.rs,src/write_batch.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
task006 [ ] goal:optimistic transaction point/range read conflict validation works in memory | scope:src/transaction.rs,src/db.rs,tests | verify:cargo fmt --check + cargo clippy + cargo test
```

## Known Blockers

- Optimistic transaction validation, persistent WAL, SSTable flush, manifest,
  recovery, compaction, blob files, compression crates, and optimized search
  policies are not implemented yet.

## Evidence To Record

- Phase 2 scaffold gate results.
- Transaction conflict validation results.
- Remaining blocker category after transaction validation.
