# Current Phase

## Status

Complete

## Goal

Wire the Titan-like `BlobFile` format into flush, compaction output, table
properties, read stats, and persistent recovery.

## Entry Condition

- Phase 34 completed the standalone `BlobIndex` and `BlobFile` format.
- User asked to finish the remaining spec integration work from that phase.

## Scope

- Make persistent table writes separate large inline values into the new
  `BlobFile` format and store `ValueRef::BlobIndex` in SSTables.
- Keep small values inline and keep in-memory mode on inline memtable values.
- Record per-table blob reference metadata: file id, referenced bytes, record
  count, and key span.
- Validate manifest-referenced blob files during persistent open.
- Count blob reads and bytes in `DbStats`.
- Add focused persistent tests for new blob output, reopen reads, corrupt blob
  detection, manifest/table blob references, and blob read stats.

## Out Of Scope

- Snapshot-safe blob GC rewrite.
- Level Merge.
- Blob compression tuning beyond writing the stable compression id in the
  record format. The first integrated writer stores blob values uncompressed.

## Acceptance Gate

- Flush writes the new `BlobFile` format for values at or above
  `blob_threshold_bytes`.
- SSTables store `BlobIndex` for separated values and inline bytes for small
  values.
- Persistent reopen validates referenced blob files and fails closed on corrupt
  blob content.
- Table and manifest metadata preserve blob reference details.
- Point and range/prefix reads only read blob bytes after an LSM record is
  visible.
- Rust verification passes.

## Active Task Slice

```text
task115 [x] goal:wire flush to new BlobFile and BlobIndex | scope:src/blob.rs src/table.rs | verify:persistent blob flush test
task116 [x] goal:validate referenced blob files on open | scope:src/recovery.rs src/db.rs | verify:corrupt referenced blob reopen test
task117 [x] goal:record blob reference/read stats | scope:src/table.rs src/manifest.rs src/stats.rs src/db.rs src/iterator.rs src/lsm/read.rs | verify:stats assertions + cargo test
task118 [x] goal:run full verification and record evidence | scope:repo .phrase docs | verify:cargo test + clippy + diff checks
```

## Known Blockers

- Snapshot-safe blob GC rewrite is still a follow-up phase.
- Remote CI cannot be executed locally; it must run after push.

## Evidence

- `cargo test persistent_flush_writes_blob_index_file_and_reopen_reads_large_values --all-features`
  passes.
- `cargo test persistent_reopen_fails_on_corrupt_referenced_blob_file --all-features`
  passes.
- `cargo test --all-targets --all-features` passes.
- `cargo clippy --all-targets --all-features` passes.

## Next Recommendation

- Start the next implementation slice for snapshot-safe blob GC rewrite and
  publish/delete metadata once the user wants to continue the large-value
  lifecycle work.
