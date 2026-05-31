# Current Phase

## Status

Complete

## Goal

Route table and blob cleanup deletion through the storage backend object delete
operation.

## Scope

- Add an internal object delete backend operation for storage objects.
- Add object delete capability reporting.
- Implement native-file object delete for table and blob objects.
- Keep object delete idempotent for missing table/blob files.
- Reject manifest object deletion through the generic object delete path.
- Route pending obsolete table cleanup through the backend delete operation.
- Route pending obsolete blob cleanup through the backend delete operation.
- Route failed flush/compaction output cleanup through the backend delete
  operation.
- Preserve snapshot safety, manifest pending-blob deletion authority, storage
  formats, public API behavior, and durability publish ordering.

## Out Of Scope

- Choosing a concrete async runtime crate.
- Introducing public async APIs.
- Moving WAL append, blob reads, blob listing, parent-directory sync, writer
  lease handling, or manifest publish into different backend operations.
- Changing table or blob object formats.
- Changing cleanup eligibility rules, snapshot pin behavior, manifest pending
  deletion semantics, or repair policy.
- Changing compaction planning, blob GC selection, MVCC visibility, or public
  API behavior.

## Acceptance Gate

- Roadmap records the object delete phase at phase granularity.
- Current phase records which protocol stages are complete and which storage
  operations remain.
- Native-file backend reports object delete capability.
- Native-file backend deletes table and blob objects through the generic object
  delete operation and treats missing table/blob objects as already deleted.
- Native-file backend rejects manifest objects through generic object delete.
- Cleanup paths use backend deletion while preserving snapshot and manifest
  safety checks.
- Existing persistent cleanup, recovery, compaction, and publish-failure tests
  pass.
- `cargo fmt --check`, focused Rust tests, clippy, `cargo test --all-targets
  --all-features`, `git diff --check`, and forbidden-term scan pass.
- Evidence records how this adapter prepares storage backend migration.

## Active Task Slice

```text
task217 [x] goal:start object delete backend slice | scope:current roadmap evidence protocol | verify:manual
task218 [x] goal:add native-file object delete operation | scope:src/storage.rs | verify:storage tests
task219 [x] goal:route cleanup deletion through backend | scope:src/db.rs | verify:persistent cleanup tests
task220 [x] goal:record evidence and next step | scope:.phrase/evidence.md current roadmap | verify:git diff
```

## Known Blockers

- None identified for this object delete slice.
- Public async API, async runtime selection, WAL append, blob reads, blob
  listing, writer lease handling, parent-directory sync routing, and production
  in-memory object routing remain later phases.

## Evidence

- Phase 56 routed table output-file creation through backend object writes.
- Phase 57 routed blob file creation through backend object writes.
- Cleanup deletion was the next storage operation still using direct native
  file removal in core cleanup paths.
- `StorageCapability::ObjectDelete` now names backend delete support.
- `NativeFileBackend` now deletes table and blob objects through
  `StorageObjectDeleteBackend`, treats missing table/blob objects as deleted,
  and rejects manifest objects.
- Pending obsolete table cleanup, pending obsolete blob cleanup, and failed
  flush/compaction output cleanup now call the backend delete operation.
- Snapshot-count checks and manifest pending-blob deletion authority remain
  unchanged.
- Verification passed: `cargo test storage --lib`, `cargo test cleanup --lib`,
  focused persistent publish-failure/pending-deletion tests, and
  `cargo test persistent --all-targets`, `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `git diff --check`, and
  forbidden-term scan.

## Next Recommendation

- Route blob reads or blob listing through storage backend operations next, then
  move toward WAL append once table/blob object operations are fully behind the
  backend boundary.
