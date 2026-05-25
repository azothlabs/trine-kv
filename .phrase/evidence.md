# Evidence

Record only evidence that can change planning or durable decisions.

## Template

### YYYY-MM-DD: <topic>

**Observation**:

- What was directly observed.

**Interpretation**:

- What the observation likely means.

**Verification**:

- Test, trace, benchmark, audit, manual check, or other proof.

**Remaining Blockers**:

- What still prevents completion.

**Recommended Next Action**:

- What the next phase or task should do.

## 2026-05-25: V1 Spec Baseline

### Observation

- Repository is a clean project skeleton with phrase workflow files and local
  Rust skills.
- User wants a new independent embedded KV, not a comparison project and not a
  previous-engine continuation.
- User requires LSM-tree based storage, MVCC, persistence, in-memory mode, and
  first-version completeness.

### Interpretation

- The first useful deliverable is a durable spec, not Rust code.
- Trine should be specified and implemented from its own docs and tests.

### Recommended Next Action

- Review `.phrase/protocol/trine-kv-v1-spec.md`.
- If accepted, start Phase 2 by scaffolding the Rust crate and module layout.

## 2026-05-25: Search Policy Added To Spec

### Observation

- Binary search can be a measurable CPU cost in immutable table indexes and
  block restart indexes.
- The useful alternatives are not universal replacements: Eytzinger layout fits
  immutable search arrays, while galloping search fits cursor movement with a
  position hint.

### Interpretation

- Trine should expose stable `seek` and `advance_to` index APIs while keeping
  the algorithm behind an internal search policy.
- Primary SSTable record order should remain sorted for range scans,
  validation, and simple recovery.

### Recommended Next Action

- When implementation reaches SSTable indexes, add canonical sorted-search
  tests first, then add optimized search layouts behind benchmarked thresholds.

## 2026-05-25: Prefix Filters And Compression Policy Added

### Observation

- Prefix scan is a common KV operation and should not depend only on caller-side
  range construction.
- SSTable block decompression sits on the read path. Fast decompression is more
  important for hot blocks than maximum compression ratio.
- A compact second codec was considered earlier, but this is superseded by the
  later V1 compression narrowing below.

### Interpretation

- Prefix extractor and prefix filter support must be part of v1 table format,
  keyspace options, tests, and metrics.
- Trine should default to a fast block codec implemented with `lz4_flex`.
- On-disk codec ids should be Trine names, not Rust crate names.

### Recommended Next Action

- During crate scaffolding, add codec and prefix-filter modules as first-class
  boundaries instead of burying them inside SSTable reader code.

## 2026-05-25: V1 Spec Accepted For Scaffolding

### Observation

- User stated the spec, protocol, and related docs are detailed enough and asked
  to begin implementation.
- Phase 1 acceptance files already exist:
  `.phrase/adr/0001-v1-lsm-mvcc-engine.md` and
  `.phrase/protocol/trine-kv-v1-spec.md`.

### Interpretation

- Phase 1 can be treated as accepted for implementation planning.
- The next measured slice is Phase 2 crate scaffolding, not engine behavior.

### Verification

- Manual review of roadmap, current phase, ADR, protocol spec, and evidence.

### Remaining Blockers

- No Rust crate exists yet.

### Recommended Next Action

- Create the crate skeleton and run the Phase 2 formatting, lint, and test gate.

## 2026-05-25: Phase 2 Scaffold Gate Passed

### Observation

- Rust crate scaffold was added with modules matching the v1 protocol boundary:
  API handles, typed errors, MVCC, WAL, memtable, SSTable, manifest,
  VersionSet, compaction, transaction, prefix/filter, codec, search, cache,
  blob, stats, and write batches.
- `cargo fmt --check`, `cargo clippy`, and `cargo test` passed.

### Interpretation

- Phase 2 is complete.
- The next useful implementation slice is in-memory MVCC point semantics,
  because it exercises sequence assignment, write batches, snapshots, typed
  errors, and keyspace boundaries without pulling in WAL/SSTable complexity.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`

### Remaining Blockers

- No point write/read behavior exists yet.
- Persistent WAL, SSTable, manifest, recovery, compaction, range deletes,
  range/prefix iteration, and optimistic transaction validation remain future
  blockers.

### Recommended Next Action

- Implement in-memory MVCC point writes, point deletes, and snapshot reads.

## 2026-05-25: In-Memory MVCC Point Slice Passed

### Observation

- In-memory keyspaces now store point versions in an ordered
  `BTreeMap<InternalKey, ValueRef>`.
- Write batches assign one commit sequence and apply point inserts/deletes
  atomically after validating keyspaces and unsupported operations.
- Snapshot reads use the snapshot sequence and continue seeing older point
  versions after later writes and deletes.
- Duplicate keys inside one batch use later batch operations first through the
  internal-key batch index tie-breaker.

### Interpretation

- The first Phase 3 behavior slice is complete.
- The next blocker is not persistence yet; it is ordered in-memory iteration,
  because range/prefix scans should reuse the same MVCC visibility rules before
  SSTable and compaction work exists.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`

### Remaining Blockers

- Range iteration and prefix iteration are still unsupported.
- Range deletes, WAL, SSTable flush/read, manifest, recovery, compaction,
  compression crates, optimized index policies, blob files, and optimistic
  transaction validation remain future blockers.

### Recommended Next Action

- Implement snapshot-consistent in-memory range and prefix iteration.

## 2026-05-25: In-Memory Range And Prefix Iteration Passed

### Observation

- In-memory range scans now return one newest visible live value per user key in
  lexicographic order.
- Prefix scans return only keys that start with the requested byte prefix.
- Reverse range and reverse prefix scans share the same visible key set and only
  reverse output order.
- Snapshot range and prefix scans keep seeing older point versions after later
  writes and point deletes.
- Existing keyspace handles now reject mismatched options instead of silently
  ignoring the new options.
- Write batch operation count is checked before memtable writes begin, so batch
  index conversion cannot cause partial application.

### Interpretation

- Task004 is complete.
- The next correctness blocker is range delete support, because point reads,
  range scans, prefix scans, and future compaction must all honor range
  tombstones under the same MVCC visibility rule.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`

### Remaining Blockers

- Range deletes are still rejected by write batches.
- Persistent WAL, SSTable flush/read, manifest, recovery, compaction,
  compression crates, optimized index policies, blob files, and optimistic
  transaction validation remain future blockers.

### Recommended Next Action

- Implement in-memory range deletes for point reads, range scans, and prefix
  scans while preserving snapshot reads.

## 2026-05-25: In-Memory Range Deletes Passed

### Observation

- In-memory range tombstones now carry range bounds, commit sequence, and
  batch index.
- Point reads, range scans, and prefix scans check visible range tombstones
  before returning point values.
- Snapshot reads still see values that were live before a later range delete.
- Same-batch order is preserved: a later insert survives an earlier range
  delete, and a later range delete hides an earlier insert.

### Interpretation

- Task005 is complete for in-memory behavior.
- The next memory-engine correctness blocker is optimistic transaction
  validation, because point and range read tracking must conflict with writes
  and range deletes committed after the transaction read sequence.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`

### Remaining Blockers

- Optimistic transaction validation still returns unsupported.
- Persistent WAL, SSTable flush/read, manifest, recovery, compaction,
  compression crates, optimized index policies, and blob files remain future
  blockers.

### Recommended Next Action

- Implement in-memory optimistic transaction point/range read conflict
  validation.

## 2026-05-25: In-Memory Optimistic Transaction Validation Passed

### Observation

- Transactions now record point keys and key ranges read at the transaction
  read sequence.
- Commit validation runs while holding the writer coordinator lock.
- Point reads conflict with later point writes, point deletes, and covering
  range deletes.
- Range reads conflict with later point mutations inside the range and later
  overlapping range deletes.
- Writes outside a tracked read range do not conflict.
- Transactions can commit staged writes through the same batch commit path as
  direct writes.

### Interpretation

- Task006 is complete for in-memory behavior.
- The next blocker should be persistence, starting with a small WAL append and
  replay loop for committed batches before SSTable or manifest work.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Persistent mode still returns unsupported.
- WAL framing/checksums, manifest, SSTable flush/read, recovery reports,
  compaction, compression crates, optimized index policies, and blob files
  remain future blockers.

### Recommended Next Action

- Implement persistent mode with a minimal WAL file for committed batches and
  deterministic replay on reopen.

## 2026-05-25: Persistent WAL Append And Replay Passed

### Observation

- Persistent mode now opens a database directory, appends committed batches to
  `trine.wal`, and replays WAL batches on reopen.
- WAL frames include magic, format version, payload length, header checksum,
  payload checksum, and a binary payload for inserts, point deletes, and range
  deletes.
- WAL append happens after sequence assignment and before memtable update.
- Reopen restores point values, point deletes, range deletes, cross-keyspace
  batches, and the last committed sequence.

### Interpretation

- Task007 is complete for the minimal persistent WAL loop.
- The next useful WAL slice is recovery behavior under bad WAL bytes: torn
  final record should be ignored, but checksum corruption in a complete record
  should fail closed.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- WAL corruption/torn-tail behavior needs explicit tests.
- Keyspace options are not durable without a manifest.
- SSTable flush/read, manifest, recovery reports, compaction, compression
  crates, optimized index policies, and blob files remain future blockers.

### Recommended Next Action

- Add WAL corruption and torn-tail recovery tests, then tighten WAL parsing if
  those tests expose gaps.

## 2026-05-25: WAL Torn Tail And Corruption Behavior Passed

### Observation

- A torn final WAL record is ignored during persistent reopen.
- A complete WAL record with a corrupted payload checksum fails closed with
  `Error::Corruption`.
- The existing append/replay tests still pass after adding the bad-WAL tests.

### Interpretation

- Task008 is complete.
- The next persistent-mode blocker is manifest state. WAL replay currently
  recreates keyspaces with default options, so non-default keyspace options are
  not durable yet.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Manifest creation/options and WAL replay floor are not implemented.
- SSTable flush/read, recovery reports, compaction, compression crates,
  optimized index policies, and blob files remain future blockers.

### Recommended Next Action

- Implement a small manifest that records keyspace creation/options and the WAL
  replay floor, without adding SSTable flush yet.

## 2026-05-25: Manifest Keyspace State Passed

### Observation

- Persistent DB startup now opens `MANIFEST`, restores declared keyspaces and
  their options before WAL replay, and stores the manifest behind a mutex for
  later edits.
- Creating a persistent keyspace publishes its options to `MANIFEST` before the
  in-memory registry is updated.
- WAL replay now skips records at or below the manifest replay floor and fails
  closed if a newer WAL record references a keyspace missing from the manifest.
- A regression test removes `MANIFEST` after writing WAL data and confirms
  reopen fails instead of recreating the keyspace with default options.
- The direct commit and WAL replay methods were moved from `src/db.rs` into
  `src/db/commit.rs` to keep the DB API, read path, and commit path separated.

### Interpretation

- Task009 is complete for keyspace creation/options and replay-floor handling.
- The next blocker is the first SSTable flush/read slice, because advancing the
  replay floor is only useful once flushed table files exist.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- SSTable flush/read and table manifest edits are not implemented.
- Recovery reports, compaction, blob files, compression crates, and optimized
  search policies remain future blockers.

### Recommended Next Action

- Implement a small SSTable writer/reader for flushed memtable contents, then
  publish table metadata and advance the WAL replay floor through the manifest.

## 2026-05-25: First SSTable Flush And Read Passed

### Observation

- `Db::flush()` now writes per-keyspace table files for current point records
  and range tombstones in persistent mode.
- The manifest records table metadata and advances the WAL replay floor in the
  same publish step after all table files are written.
- Persistent open loads manifest table metadata, verifies table properties
  against the referenced table file, and installs the table before WAL replay.
- Point reads, range/prefix scans, transaction conflict checks, and snapshots
  now read from both memtables and loaded table files.
- Flush clears the memtable only after the table file is written, the manifest
  is published, and the table is installed in the current process.
- Tests remove the WAL after flush and confirm reopen still reads point
  values, point deletes, and range deletes from the table file.
- Snapshot tests confirm versions written before flush remain visible at older
  read sequences.

### Interpretation

- Task010 is complete for the first table-file slice.
- The next blocker is recovery hardening for table files, especially missing
  files, checksum mismatch, and manifest/table metadata mismatch.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- The table format is still a simple checked file, not the full block/index
  SSTable layout from the v1 protocol.
- Table recovery hardening, recovery reports, compaction, blob files,
  compression crates, and optimized search policies remain future blockers.

### Recommended Next Action

- Add table corruption and missing-table recovery tests, then tighten startup
  errors before moving into compaction.

## 2026-05-25: SSTable Recovery Fail-Closed Coverage Passed

### Observation

- Startup now returns `Error::Corruption` if a manifest-referenced table file
  cannot be opened or read.
- A missing table file referenced by the manifest fails persistent reopen.
- A table file with a corrupted payload checksum fails persistent reopen.
- A table file whose valid payload properties differ from the manifest metadata
  fails persistent reopen.

### Interpretation

- Task011 is complete for the current table-file format.
- The first table recovery risks are now pinned by tests, so the next useful
  engine slice is manual compaction over flushed tables.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Compaction is still unsupported.
- SSTable block/index layout, recovery reports, blob files, compression crates,
  and optimized search policies remain future blockers.

### Recommended Next Action

- Implement a small manual compaction path that rewrites flushed tables into a
  replacement table while preserving MVCC visibility and manifest publish rules.

## 2026-05-25: V1 Compression Narrowed To LZ4

### Observation

- User explicitly decided V1 compression should use only `lz4_flex`, not
  `flate2`.
- The protocol previously named a compact zlib/DEFLATE codec, which conflicted
  with the new V1 boundary.

### Interpretation

- V1 should expose only `none` and `fast-lz4-block` codec ids.
- `CompressionProfile` should only distinguish uncompressed blocks from the
  fast default codec.
- Any future second codec needs a new protocol decision and fixtures.

### Verification

- Protocol, decision framework, current task brief, codec ids, compression
  profiles, manifest codec encoding, table codec encoding, and tests were
  scanned and updated for the narrowed V1 rule.

### Remaining Blockers

- `lz4_flex` is not wired into table blocks yet.

### Recommended Next Action

- Keep compaction as the next implementation slice, then add `lz4_flex` once
  the block/index table layout exists.

## 2026-05-25: Manual Compaction Slice Passed

### Observation

- `Db::compact_range` now rewrites overlapping flushed tables into one
  replacement table per keyspace in persistent mode.
- The first compaction slice keeps all internal versions and range tombstones,
  so older snapshots keep their expected read results.
- Manifest replacement validates every keyspace and input table before
  publishing one batch edit, which avoids partial multi-keyspace manifest
  updates.
- After manifest publish and in-process table-list switch, obsolete table files
  are removed from disk.
- Tests cover snapshot visibility across compaction, range tombstone results,
  reopen without WAL after compaction, obsolete table cleanup, and keyspace
  separation.

### Interpretation

- Task012 is complete for manual table rewrite and manifest cutover.
- The remaining compaction work is cleanup policy, scheduling, and leveled input
  selection; this slice intentionally does not drop older versions yet.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- SSTable files still use the simple checked payload format rather than the v1
  block/index layout.
- Version cleanup needs an active-snapshot boundary before it can safely drop
  older versions or tombstones.
- Blob files, lz4_flex block compression, prefix filters, recovery reports, and
  optimized search policies remain future blockers.

### Recommended Next Action

- Implement the checked table block/index layout with codec id `none`, then
  retest point/range reads, recovery, and compaction against the new layout.

## 2026-05-25: Checked Table Block/Index Layout Passed

### Observation

- Table files now encode checked sections for data blocks, range tombstones,
  indexes, properties, and a footer with section offsets.
- Data blocks store sorted internal-key records, restart offsets, codec id,
  uncompressed length, encoded length, and block checksum.
- Index blocks map internal-key bounds to data block handles and are validated
  for sorted, contiguous coverage before table records are trusted.
- Properties are stored in a checked properties block and must match the decoded
  records before startup accepts the table.
- The current writer only emits codec id `none`; reading a data block marked as
  `fast-lz4-block` fails closed until task014 wires the codec.
- Tests cover multi-block table round trip, unsupported block codec failure,
  point/range reads from the new layout before and after reopen, and manifest
  metadata mismatch without depending on byte offsets inside the table file.

### Interpretation

- Task013 is complete for checked `none`-codec table layout.
- The next blocker is real `lz4_flex` block compression; the format can already
  name the fast codec, but the implementation intentionally refuses it today.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- `lz4_flex` is not connected to checked block encode/decode yet.
- Prefix filters, blob files, recovery reports, version cleanup, and optimized
  search policies remain future blockers.

### Recommended Next Action

- Add `lz4_flex`, route `CompressionProfile::Fast` to codec id
  `fast-lz4-block`, and keep `none` as the mandatory fallback.

## 2026-05-25: LZ4 Block Compression Passed

### Observation

- `lz4_flex` is now the implementation behind codec id `fast-lz4-block`.
- `CompressionProfile::Fast` writes new table blocks with codec id
  `fast-lz4-block`; `CompressionProfile::None` continues to write codec id
  `none`.
- Checked block headers store codec id, uncompressed length, encoded length, and
  checksum over the encoded bytes before decode.
- Table open verifies every checked section uses the codec recorded in table
  properties.
- Tests cover direct none/lz4 codec round trips, fast table layout round trip,
  unknown block codec failure, and persistent reopen for both fast and none
  keyspace compression profiles.

### Interpretation

- Task014 is complete for V1 fast block compression.
- The next useful slice is prefix filters, because table blocks now have stable
  checked encode/decode and per-table codec metadata.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Prefix filters are still not written or consulted.
- Blob files, recovery reports, version cleanup, and optimized search policies
  remain future blockers.

### Recommended Next Action

- Implement prefix filters as an advisory table-skip path, while keeping MVCC
  and range tombstone checks authoritative after any filter hit.

## 2026-05-25: Prefix Filter Table Skip Passed

### Observation

- Table files now include a checked filter section.
- Newly written tables can store an exact prefix set for `FixedLen` or
  `Separator` extractors when prefix filters are enabled for the keyspace.
- Prefix scans consult compatible table prefix filters to skip point records
  that cannot match the requested prefix.
- Range tombstones are still collected from all tables, including tables whose
  point records are skipped by a prefix filter.
- Queries that are shorter than the configured extractor prefix fall back to
  reading candidate tables instead of trusting an incompatible filter lookup.
- Tests cover prefix-filter table skipping, short-prefix fallback, range
  tombstone correctness, and reopen from filtered tables.

### Interpretation

- Task015 is complete for the first advisory prefix-filter path.
- The next blocker is separated blob values, because table blocks can now carry
  checked metadata, compression, and advisory filters for inline values.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Blob references still return unsupported errors on the read path.
- Recovery reports, version cleanup, and optimized search policies remain
  future blockers.

### Recommended Next Action

- Implement blob file writing/reading for values above the keyspace threshold,
  then prove reopen, flush, and compaction keep those values readable.

## 2026-05-25: Separated Blob Values Passed

### Observation

- Flush now writes values at or above the keyspace blob threshold into blob
  files named by table id, and stores `Blob` references in the table records.
- Blob references encode file id, offset, length, and checksum in checked table
  blocks.
- Reads resolve blob references through the persistent database path and verify
  blob checksums before returning bytes.
- Persistent open validates manifest-referenced tables and their blob
  references, so missing blob files fail closed during recovery.
- Compaction preserves existing blob references while rewriting table files.
- Tests cover blob values after flush, reopen without WAL, compaction, inline
  values in the same table, and missing blob-file recovery failure.

### Interpretation

- Task016 is complete for first-version separated blob values.
- The current task list in `.phrase/current.md` is complete. Remaining work is
  no longer the same slice; it should be selected from the latest evidence.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Blob cleanup is not implemented; old blob files remain if table references
  move or become obsolete.
- Recovery reports, version cleanup, and optimized search policies remain
  future blockers.

### Recommended Next Action

- Start the next phase selection from the remaining blockers instead of adding
  more behavior to the completed current task list.

## 2026-05-25: Point-Key Filter Table Skip Passed

### Observation

- Table filter sections now store point-key filters separately from prefix
  filters.
- Point reads use compatible point-key filters to skip table point records that
  cannot contain the requested key.
- Range tombstones are still collected from all tables before MVCC visibility is
  decided, so a skipped table's tombstones can still hide older point records.
- Tests cover point-key filter round trip, missing-key rejection, point-read
  correctness when an otherwise skipped table carries a range tombstone, and
  reopen from filtered tables.

### Interpretation

- Task017 is complete for point-key table skipping.
- SSTable filter coverage now includes point-key and prefix filters, but the
  read path still does not use block indexes/restart offsets for block-level
  seek.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Block-level index seek and restart seek are still not used by point/range
  reads.
- Partitioned filters/indexes, blob cleanup, version cleanup, recovery reports,
  and optimized search policies remain future blockers.

### Recommended Next Action

- Implement a table read path that uses index entries and restart offsets for
  point/range candidate selection before adding optimized search policies.

## 2026-05-25: SSTable Block Candidate Read Path Passed

### Observation

- Loaded tables now keep one sorted record array plus data-block metadata:
  record ranges, block key bounds, and restart positions as record indexes.
- Table open validates that each encoded restart offset lands exactly on a
  decoded record boundary, starts at the first record, and remains sorted.
- Point, range, and prefix table reads use block bounds and restart positions
  to collect candidate records instead of scanning every table record.
- DB point/range/prefix collectors now call the table candidate APIs; range
  transaction conflict checks use the same bounded point-record path.
- Tests cover direct table candidate reads for point/range/prefix queries and
  persistent point/range/prefix reads before and after reopening from SSTables.

### Interpretation

- Task018 is complete for block-level candidate selection using the canonical
  table index and data-block restart positions.
- This is still a simple in-memory table reader after open; partitioned
  filters/indexes and optimized search policies remain separate future work.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Partitioned filters/indexes, optimized search policies, recovery reports,
  blob cleanup, and version-cleaning compaction remain future blockers.

### Recommended Next Action

- Choose between recovery reporting and cleanup/version-cleaning compaction
  next; optimized search policies should wait until the remaining correctness
  gaps are narrower.

## 2026-05-25: Recovery Repair Report Passed

### Observation

- Persistent startup now checks for known safe temporary files before loading
  the manifest: `MANIFEST.tmp`, `RECOVERY_REPORT.tmp`, `table-*.tmp`, and
  `blob-*.tmp`.
- Default recovery still fails closed if such files are present, leaving the
  temporary files untouched for inspection.
- `FailOnCorruptionPolicy::RepairSafeTemporaryFiles` removes only those known
  temporary files and writes a deterministic `RECOVERY_REPORT`.
- The recovery report has a small public reader and records repaired file names
  in sorted order.
- Tests cover fail-closed startup, explicit repair with report output, original
  data still reading after repair, and report encode/decode.

### Interpretation

- Task019 is complete for the protocol rule that safe startup repairs must
  record a repair report.
- Recovery still does not detect obsolete unreferenced files; that should stay
  separate from this repair-report slice.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Obsolete-file detection, blob cleanup, version-cleaning compaction,
  partitioned filters/indexes, and optimized search policies remain future
  blockers.

### Recommended Next Action

- Implement snapshot-safe version cleanup in compaction before blob cleanup, so
  blob files can later be removed only after no live table references them.

## 2026-05-25: Snapshot-Safe Point-Version Cleanup Passed

### Observation

- `Snapshot` now pins its read sequence through a shared tracker owned by the
  database; cloning a snapshot adds a pin, and dropping it releases the pin.
- `Db::stats()` now reports active snapshot handles.
- Manual compaction now computes the oldest active snapshot sequence and cleans
  point records per user key: it keeps every version newer than that sequence
  and the newest record at or below it.
- If there is no active snapshot, compaction uses the latest committed sequence
  as the cleanup floor and keeps only the newest point record per user key.
- Tests cover snapshot pin clone/drop counts, active-snapshot floor cleanup,
  no-old-snapshot cleanup, point tombstone preservation as the newest record,
  and the existing persistent snapshot-through-compaction behavior.

### Interpretation

- Task020 is complete for snapshot-safe point-version cleanup.
- Range tombstone and point tombstone cleanup rules are still separate because
  they require checking whether older covered records remain in the relevant
  compaction scope.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Tombstone cleanup, obsolete-file detection, blob cleanup, partitioned
  filters/indexes, and optimized search policies remain future blockers.

### Recommended Next Action

- Implement tombstone cleanup under the same snapshot safety boundary, keeping
  tombstones whenever older covered data may still exist in the compaction
  scope.

## 2026-05-25: Tombstone Cleanup Passed

### Observation

- Manual compaction now removes point deletes when the compacted point-record
  group has no older record left for that user key.
- Full-keyspace compaction now removes range deletes that no longer cover any
  retained older `Put` record.
- Partial compaction retains range deletes, because it cannot prove there is no
  older covered data just outside the input tables.
- Manifest replacement now supports compaction outputs that remove input tables
  without writing a replacement table when cleanup produces no records.
- Tests cover point-delete cleanup, point-delete retention while older records
  remain, range-delete cleanup and partial-compaction retention, and persistent
  compaction that removes a delete-only output without leaving an empty table.

### Interpretation

- Task021 is complete for conservative tombstone cleanup inside the current
  manual compaction model.
- Blob cleanup can now rely on compaction removing obsolete point records and
  tombstones before deciding whether old blob files are still referenced.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Obsolete-file detection, blob cleanup, partitioned filters/indexes, and
  optimized search policies remain future blockers.

### Recommended Next Action

- Implement blob cleanup by scanning live table references after compaction and
  removing unreferenced blob files only after manifest publish succeeds.
