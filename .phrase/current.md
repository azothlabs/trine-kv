# Current Phase

## Status

Complete

## Goal

Harden the release-candidate logic risks found during review, starting with the
scan/flush consistency issue that can make a range or prefix scan miss committed
data.

## Scope

- LSM scan source capture order across active memtable, immutable memtables,
  delta mirrors, range tombstones, and the current table version.
- Commit failure semantics after WAL accept or partial publication, if review
  confirms a code path with unclear visible-sequence outcome.
- Checksum strength for WAL/table/blob formats, limited to changes that are
  either storage-compatible or explicitly recorded as a storage-format decision.
- Public API boundary cleanup, limited to hiding internals that are not meant to
  define the release contract.

## Out Of Scope

- Replacing the LSM MVCC engine design.
- Depending on another storage engine.
- Release publishing, tagging, or crate version metadata changes.
- Browser persistence fixture automation unless it becomes necessary to verify
  this phase.

## Acceptance Gate

- Scan snapshots cannot miss committed records when a flush publishes an L0
  table before removing the flushed immutable memtable.
- Focused regression coverage demonstrates the scan/flush ordering.
- Any commit failure path touched in this phase has a clear caller-visible
  result and does not leave the visible sequence stuck silently.
- Checksum and public API changes are either landed with focused verification or
  recorded as deferred decisions with a reason.
- `cargo fmt --check`, focused tests, and `git diff --check` pass.

## Active Task Slice

```text
task556 [x] goal:fix scan snapshot ordering across flush publish/removal | scope:src/lsm/scan.rs tests | verify:focused scan/flush test
task557 [x] goal:classify and harden commit failure outcome | scope:src/db/commit.rs | verify:focused failure-path test
task558 [x] goal:classify checksum and public API boundary hardening | scope:src/{wal,table,blob,lib}.rs .phrase | verify:CRC test plus full release-facing tests
task559 [x] goal:close public Rustdoc coverage gap | scope:src public API .phrase | verify:cargo rustdoc --all-features -- -D missing-docs
task560 [x] goal:upgrade core public Rustdoc from coverage to user guidance | scope:src/{lib,db,bucket,options,transaction,write_batch}.rs .phrase | verify:cargo rustdoc --all-features -- -D warnings && cargo test --doc --all-features
```

## Known Residuals

- Format-check modules `blob`, `codec`, `manifest`, `table`, and `wal` remain
  publicly reachable but hidden from generated docs because integration tests
  currently use them to inspect durable files. Fully private format helpers
  need a later test-boundary migration.

## Evidence

- `.phrase/evidence.md` records the scan/flush snapshot-ordering review.
- `.phrase/evidence.md` records the public Rustdoc coverage audit and gate.
- `.phrase/evidence.md` records the follow-up user-documentation quality
  correction.
- `cargo test scan_snapshot --lib`, the partial commit failure regression, and
  the CRC-32C check value test pass.
- `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, `cargo fmt --check`,
  `cargo check --all-targets --all-features`,
  `cargo rustdoc --all-features -- -D missing-docs`,
  `cargo rustdoc --all-features -- -D warnings`,
  `cargo test --doc --all-features`, and `git diff --check` pass.

## Next Recommendation

- Keep the format-helper modules hidden from docs for this release candidate,
  then migrate durable-file inspection helpers behind a dedicated internal test
  boundary in a later phase if the public API needs to be made stricter.
- Keep missing public Rust documentation as a release-candidate gate.
- Keep doctests and strict rustdoc warnings in the documentation quality gate.
