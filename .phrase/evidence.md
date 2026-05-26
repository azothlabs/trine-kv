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

## 2026-05-25: Blob Cleanup Passed

### Observation

- Manual compaction now scans live table blob references after manifest publish
  and the in-memory table switch.
- Blob files not referenced by the live table set are removed after obsolete
  compacted table files are removed.
- Blob removal is skipped while any snapshot or short read pin is active, so a
  reader holding an older table handle cannot lose a referenced blob file.
- Point, range, and prefix read paths now create short read pins around visible
  record collection and blob reads.
- Tests cover compaction removing the blob file for a dropped old version,
  compaction removing the blob file after a delete-only cleanup, and reopen
  after cleanup.

### Interpretation

- Task022 is complete for post-compaction blob cleanup in the current manual
  compaction model.
- Startup obsolete-file detection remains separate because recovery needs a
  deterministic audit of unreferenced files before opening or repairing.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Obsolete-file detection, partitioned filters/indexes, and optimized search
  policies remain future blockers.

### Recommended Next Action

- Implement obsolete-file detection during persistent startup, reporting
  unreferenced table/blob files without weakening fail-closed recovery behavior.

## 2026-05-25: Obsolete File Detection Passed

### Observation

- Persistent startup now compares formal `table-*.trinet` files against the
  manifest's referenced table ids.
- Startup now compares formal `blob-*.trineb` files against blob references
  found in loaded live tables.
- Unreferenced formal table/blob files return `Corruption` and are left on disk
  for operator review.
- `RepairSafeTemporaryFiles` remains limited to known temporary files and does
  not delete formal table/blob files.
- Tests cover unreferenced table-file detection, unreferenced blob-file
  detection under the temporary-file repair policy, malformed formal file-name
  detection, and preservation of those files after startup fails.

### Interpretation

- Task023 is complete for conservative startup detection of unreferenced
  storage files.
- The next remaining blockers move out of recovery cleanup and into read-path
  structure and search-policy work.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Partitioned filters/indexes and optimized search policies remain future
  blockers.

### Recommended Next Action

- Implement partitioned filters/indexes so point, range, and prefix reads can
  skip incompatible table sections without changing visible results.

## 2026-05-25: Partitioned Filter/Index Read Path Passed

### Observation

- Data block index entries now carry per-block point-key and prefix filters in
  addition to key bounds and block handles.
- Table reads use block-local point filters for point-key candidates and
  block-local prefix filters for prefix candidates while retaining ordered scan
  and MVCC checks.
- Table decode validates that block-local filters do not miss keys or prefixes
  actually present in the indexed block.
- Tests cover index-entry filter round-trip, prefix reads through decoded
  partitioned filters, and fail-closed validation for a block filter false
  negative.

### Interpretation

- Task024 is complete for the current in-memory-loaded SSTable model: filters
  now skip table partitions, and the data block index remains the ordered
  source for block selection.
- The next remaining v1 blocker is wiring the search-policy module into table
  and block candidate lookup without changing read results.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Optimized search policies remain future work.

### Recommended Next Action

- Implement search-policy dispatch for table/block candidate lookup, keeping
  canonical sorted index order as the validation and traversal source.

## 2026-05-25: Search Policy Dispatch Passed

### Observation

- The search module now exposes a policy-dispatched partition-point helper.
- Table reads use the configured keyspace search policy when choosing candidate
  data blocks and when seeking inside block restart points.
- Linear, binary, auto, Eytzinger, and galloping-with-hint policy variants all
  preserve the same point, range, and prefix read results over canonical sorted
  arrays.
- Tests cover search boundary dispatch, table candidate stability across all
  policies, and persistent point/range/prefix reads across all policies before
  and after reopen.

### Interpretation

- Task025 is complete for wiring search policy into the current table read
  path without changing visible results.
- Specialized layouts or cursor hints can now be added behind the search module
  boundary if benchmarks justify them, without changing table correctness
  tests.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- No named implementation blocker remains from the current task list; the v1
  acceptance audit is still pending.

### Recommended Next Action

- Run a v1 protocol acceptance audit and turn any real remaining gap into the
  next measured slice.

## 2026-05-25: V1 Acceptance Audit Found Remaining Gaps

### Observation

- The current test gate passes, and the latest slices cover blob cleanup,
  startup unreferenced-file detection, partitioned table filters, and
  search-policy dispatch.
- The protocol startup flow requires an exclusive process lock, but no lock file
  or equivalent directory ownership mechanism exists yet.
- The protocol calls for leveled/background compaction, while the current engine
  exposes manual compaction over the table list.
- `DbStats` only reports a small subset of required metrics, cache types exist
  without cache behavior, and no benchmark outputs or durability documentation
  exist in the repository.

### Interpretation

- Phase 3 should continue; v1 is not ready to close.
- The next correctness blocker is persistent process locking, because two
  writers opening the same directory can violate the storage contract before
  compaction, metrics, or docs matter.

### Verification

- Manual audit against `.phrase/protocol/trine-kv-v1-spec.md` sections 25,
  28, 30, and 31.
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Persistent process locking.
- Leveled/background compaction.
- Required metrics and cache behavior.
- Required benchmark outputs.
- Durability documentation.

### Recommended Next Action

- Implement persistent process locking as the next measured slice, failing
  closed on a second simultaneous opener and releasing the lock when the last
  database handle drops.

## 2026-05-25: Persistent Writer Lock Passed

### Observation

- Write-mode persistent open now creates a `LOCK` file before recovery,
  manifest, table, blob, or WAL work begins.
- A second write-mode opener, or an existing `LOCK` file, fails closed and
  leaves the lock evidence untouched for operator review.
- The lock owner marker is removed by `close()` after the writer coordinator is
  idle, and also by dropping the final database handle.
- Read-only open follows the protocol exception and does not take the writer
  directory lock.
- Tests cover simultaneous write open rejection, stale lock fail-closed
  handling, close/drop release, and read-only coexistence with a writer.

### Interpretation

- Task027 is complete for the current single-process test harness and standard
  filesystem semantics: write-mode startup owns the database directory before
  recovery can mutate or publish state.
- Existing `LOCK` files are intentionally not treated as safe temporary files;
  stale crash markers require deliberate operator action.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Leveled/background compaction.
- Required metrics and cache behavior.
- Required benchmark outputs.
- Durability documentation.

### Recommended Next Action

- Start the leveled compaction work with an explicit table-level metadata slice,
  then verify that reads preserve newest-before-older ordering across levels.

## 2026-05-25: Compaction Level Metadata Passed

### Observation

- Table properties now include a compaction level in both table files and the
  manifest; the table and manifest format versions were advanced for the new
  required field.
- Flush outputs are recorded as L0 tables.
- Manual compaction writes a lower-level replacement: pure L0 inputs move to
  L1, and compaction that already includes a lower-level table stays at that
  deepest input level.
- Keyspace table handles are sorted by level and recency after recovery,
  flush, and compaction.
- Tests verify L0 flush metadata, L0-to-L1 compaction, newer L0 reads over an
  older L1 table, another compaction back into L1, and reopen correctness.

### Interpretation

- Task028 is complete as the first leveled-compaction slice: the engine now has
  durable level metadata and stable in-memory ordering.
- The remaining compaction blocker is not table metadata anymore; it is a
  level-aware input picker and background scheduling.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Level-aware compaction input picking.
- Background compaction scheduling.
- Required metrics and cache behavior.
- Required benchmark outputs.
- Durability documentation.

### Recommended Next Action

- Implement compaction planning that chooses L0 inputs and overlapping
  lower-level tables explicitly before wiring automatic scheduling.

## 2026-05-25: Level-Aware Compaction Planning Passed

### Observation

- Compaction planning now lives in `src/compaction.rs` instead of being an
  ad hoc table-list filter inside `Db`.
- The planner selects L0 tables that overlap the requested range, expands that
  set to include overlapping L0 neighbors, and includes lower-level tables that
  overlap the selected L0 key span.
- A single L0 table can now compact with an overlapping lower-level table;
  a single L0 table without lower-level overlap is skipped.
- When no L0 table is selected, manual compaction keeps a same-level fallback
  for the shallowest overlapping level.
- `Db::compact_range` now uses the planner's input table ids and output level
  before reusing the existing rewrite, manifest publish, and cleanup path.

### Interpretation

- Task029 is complete for explicit compaction input picking.
- The remaining compaction blocker is automatic scheduling after flush/L0
  pressure; the rewrite and publish path can already consume planner output.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Background compaction scheduling.
- Required metrics and cache behavior.
- Required benchmark outputs.
- Durability documentation.

### Recommended Next Action

- Add an automatic compaction trigger after flush when L0 table count exceeds
  the configured threshold, using the level-aware planner.

## 2026-05-25: Flush L0 Pressure Trigger Passed

### Observation

- `Db::flush` now checks live L0 table counts after publishing flushed tables.
- If any keyspace exceeds `max_l0_files`, flush releases the writer
  coordinator and invokes `compact_range(KeyRange::all())`.
- The compaction call reuses the level-aware planner and existing manifest
  publish/cleanup path.
- Tests verify that a second flush with `max_l0_files = 1` automatically moves
  L0 data into L1, keeps reads correct, leaves one new L0 table below the
  pressure limit, and survives reopen.

### Interpretation

- Task030 is complete for the v1 automatic compaction trigger.
- Compaction is now enabled without manual calls once L0 pressure exceeds the
  configured threshold. The remaining v1 blockers have moved to observability,
  cache behavior, benchmark output, and durability docs.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Required metrics and cache behavior.
- Required benchmark outputs.
- Durability documentation.

### Recommended Next Action

- Implement live database stats for tables, L0 pressure, blob files, and
  compaction counters before adding cache behavior.

## 2026-05-25: Live Stats Passed

### Observation

- `DbStats` now reports total tables, L0 tables, per-level table counts and
  bytes, total table bytes, live/obsolete blob file counts and bytes, and
  compaction run/input/output counters.
- `Db::stats` derives table, level, memtable, and blob stats from the current
  keyspace state; persistent table and obsolete blob byte counts use filesystem
  metadata best-effort.
- Successful compaction publishes cumulative run, input table, output table,
  input byte, and output byte counters.
- Tests cover memtable bytes before flush, L0/table stats after flush, blob
  stats, auto-compaction counters, per-level stats after L1 output, and obsolete
  blob stats.

### Interpretation

- Task031 is complete for the live-state and compaction-counter stats required
  before cache behavior.
- Some deeper observability items remain naturally tied to their subsystems:
  cache hit/miss stats belong with cache behavior, and benchmark/durability
  reporting belongs with the documentation and benchmark slices.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Block cache behavior and cache hit/miss stats.
- Required benchmark outputs.
- Durability documentation.

### Recommended Next Action

- Implement block cache behavior for table reads and expose cache hit/miss
  stats without changing visible read results.

## 2026-05-25: Block Cache Stats Passed

### Observation

- `src/cache.rs` now contains a capacity-bounded block cache keyed by table id
  and data block index, with hit and miss counters.
- Table point, range, and prefix reads record data-block cache access when
  `Db` supplies its block cache.
- `DbStats` now includes block cache hit and miss counters.
- Tests verify that a first table-block read records a miss, a second read of
  the same key records a hit, and read results remain unchanged.

### Interpretation

- Task032 is complete for cache behavior in the current loaded-table model.
- The cache boundary is ready for a future on-demand block reader; this slice
  deliberately does not change visible read semantics.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`

### Remaining Blockers

- Required benchmark outputs.
- Durability documentation.

### Recommended Next Action

- Add a reproducible benchmark harness and record v1 benchmark output before
  writing final durability documentation.

## 2026-05-25: V1 Benchmark Harness Passed

### Observation

- `benches/v1_bench.rs` now runs the required v1 benchmark set through a
  deterministic harness with fixed row and operation counts.
- The harness covers point and batch writes, present and missing reads, bounded
  range scans, prefix scans, matching and nonmatching table partitions,
  snapshot reads while writes continue, optimistic transaction success and
  conflict, WAL replay, flush and compaction throughput, large inline values,
  separated blob values, block-cache warm reads, cold table reads, search-policy
  comparisons across small/medium/large table indexes, iterator `advance_to`
  targets, and codec comparisons for none and fast block compression over Trine
  block payloads.
- `docs/benchmarks/v1-baseline.md` records the local 2026-05-25 benchmark
  command, workload inputs, result table, and checksums.

### Interpretation

- Task033 is complete for the v1 benchmark-output acceptance requirement.
- Benchmark numbers are local baselines, not portable performance claims. Future
  comparisons should use the same harness, machine class, and build profile.

### Verification

- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`
- forbidden terminology scan over source, tests, phase notes, benchmark files,
  and docs
- `cargo clippy --bench v1_bench`
- `cargo bench --bench v1_bench`

### Remaining Blockers

- Durability documentation.

### Recommended Next Action

- Write v1 durability documentation that clearly states each durability mode,
  recovery behavior, checksummed file boundaries, repair policy, and known
  non-goals before closing the v1 acceptance gate.

## 2026-05-25: Durability Documentation Passed

### Observation

- `docs/durability.md` now describes v1 persistent and in-memory durability
  scope, write durability modes, commit ordering, WAL replay, flush and
  manifest publish behavior, SSTable/blob checks, compaction cleanup, repair
  policy, process locking, read-only open behavior, and operator guidance.
- The documentation states current limits directly, including that `SyncAll`
  is strongest for WAL commits and that parent-directory sync coverage is not
  claimed portably for every rename or create.
- `DbOptions::durability` is now enforced as a database-level floor for every
  commit. Per-write options can request a stronger mode but cannot weaken the
  database-level mode selected at open time.
- A unit test covers the durability-floor rule.

### Interpretation

- Task034 is complete for the v1 durability-doc acceptance requirement.
- The durability configuration field now has a concrete effect on the write
  path, which removes a documentation-discovered spec gap.

### Verification

- Manual document review against `.phrase/protocol/trine-kv-v1-spec.md`
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`
- forbidden terminology scan over source, tests, phase notes, benchmark files,
  and docs

### Remaining Blockers

- No implementation blockers are recorded after task034.
- Final v1 acceptance audit still needs to decide whether Phase 3 can close.

### Recommended Next Action

- Run the final v1 acceptance audit against the protocol gate, then update
  roadmap/current/evidence with the phase result.

## 2026-05-25: Phase 3 V1 Acceptance Gate Passed

### Observation

- The final audit checked `.phrase/protocol/trine-kv-v1-spec.md` section 31.
- Public API concepts in the spec are represented in crate modules and public
  exports for database open modes, keyspaces, write batches, snapshots,
  transactions, range/prefix iteration, WAL, SSTables, compaction, filters,
  compression, search policy, stats, and recovery.
- Persistent crash/recovery coverage includes WAL replay, torn WAL tail,
  checksum corruption, missing/corrupt tables and blobs, manifest/keyspace
  recovery, lock behavior, repair reports, and unreferenced storage-file
  rejection.
- In-memory logical coverage includes point writes/deletes, batch atomicity,
  snapshots, range scans, reverse scans, prefix scans, range deletes, and
  optimistic transaction conflicts.
- Persistent SSTable coverage includes flush/reopen, compaction, snapshot-safe
  cleanup, blob survival/cleanup, prefix filters, point-key filters, table block
  indexes, compression profiles, index search policies, stats, block cache
  hits/misses, and automatic L0 compaction.
- Benchmark output is recorded in `docs/benchmarks/v1-baseline.md`, and the
  final audit reran `cargo bench --bench v1_bench` successfully.
- Durability tradeoffs are documented in `docs/durability.md`.

### Interpretation

- Phase 3 satisfies the v1 acceptance gate.
- No implementation blocker remains recorded in the current phase brief.
- The next phase should not be inferred from the old task list; it should start
  from new evidence and a new current phase brief.

### Verification

- Manual acceptance audit against `.phrase/protocol/trine-kv-v1-spec.md`
  section 31
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `cargo bench --bench v1_bench`
- `git diff --check`

### Remaining Blockers

- None recorded for Phase 3.

### Recommended Next Action

- Treat Phase 3 as complete. Start the next phase only after choosing a fresh
  phase goal from release readiness, API polish, external integration, or
  production-hardening evidence.

## 2026-05-25: Usage Documentation Passed

### Observation

- `README.md` now gives the repository entry point, minimal API example,
  verification commands, and links to usage, durability, and benchmark docs.
- `docs/usage.md` covers opening in-memory and persistent databases, keyspace
  options, point writes/reads, batches, range and prefix scans, snapshots,
  optimistic transactions, durability modes, flush/compaction, stats, read-only
  open, and recovery boundaries.
- `examples/quickstart.rs` runs through the main public API path and validates
  persistence by flushing, syncing, reopening, and reading back data.
- While validating the example, the handle lifetime rule was made explicit:
  `Keyspace` keeps the database open, so callers should release keyspace
  handles before reopening the same directory in one process.
- The crate-level docs in `src/lib.rs` now describe the completed v1 API instead
  of the old scaffold phase.

### Interpretation

- Phase 4 satisfies the usage-documentation acceptance gate.
- The docs give users a verification path instead of relying only on prose.

### Verification

- `cargo run --example quickstart`
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`
- forbidden terminology scan over source, tests, phase notes, benchmark files,
  docs, README, and examples

### Remaining Blockers

- None recorded for Phase 4.

### Recommended Next Action

- Choose the next phase from fresh evidence. API polish, release packaging,
  integration examples, or production-hardening audits are the likely next
  candidates.

## 2026-05-25: API Polish Helpers Passed

### Observation

- `Db` now exposes `open_memory`, `open_persistent`, and `open_read_only` for
  common open paths.
- `DbOptions` now has `persistent_read_only`, `with_durability`, and
  `read_only` helpers.
- `KeyspaceOptions` now has helpers for prefix extractor and blob threshold
  configuration.
- `WriteOptions` now has named constructors for buffered, flush, sync-data, and
  sync-all writes.
- `Keyspace` now has `insert_with_options`, `remove_with_options`, and
  `remove_range_with_options` for single-key helpers that need explicit write
  options while preserving the existing no-options helpers.
- README, usage docs, quickstart, scaffold tests, and persistent helper tests
  now use or validate the shorter API paths.

### Interpretation

- Phase 5 satisfies the API-polish acceptance gate for common open and
  single-key write paths.
- Storage behavior is unchanged; the new helpers route through the existing
  option structs and write path.

### Verification

- `cargo run --example quickstart`
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`
- forbidden terminology scan over source, tests, phase notes, benchmark files,
  docs, README, and examples

### Remaining Blockers

- None recorded for Phase 5.

### Recommended Next Action

- Start production hardening with an audit slice that checks operational failure
  modes before choosing code changes.

## 2026-05-25: Manifest Publish Failure Hardening Passed

### Observation

- Production-hardening audit checked recovery, table/blob write, manifest
  publish, process-lock, and WAL replay boundaries.
- The first local risk found was in `ManifestStore`: manifest edits changed
  in-memory state before the manifest file publish succeeded.
- A failed publish could leave the running process believing a keyspace, table,
  compaction edit, or WAL replay floor had advanced even though the on-disk
  manifest had not.
- `ManifestStore` now builds the next manifest state separately, publishes it,
  and installs it in memory only after publish succeeds.
- A regression test forces manifest publish failure by removing the parent
  directory and verifies the in-memory manifest state remains unchanged.

### Interpretation

- Task038 is complete for the first production-hardening audit slice.
- Risk category: local durability/metadata cutover bug.
- The fix does not change the manifest format or public storage contract; it
  tightens the existing atomic publish boundary.

### Verification

- Manual audit of recovery/table/blob/WAL/db publish paths
- `cargo test manifest_state_stays_put_when_publish_fails`
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`
- forbidden terminology scan over source, tests, phase notes, benchmark files,
  docs, README, and examples

### Remaining Blockers

- Continue production-hardening audit for startup cleanup and WAL/resource
  bounds.

### Recommended Next Action

- Audit startup cleanup and WAL decode/resource limits, then fix any local risk
  with a focused regression test.

## 2026-05-25: WAL Decode Resource Bound Passed

### Observation

- The hardening audit found that a corrupt-but-checksummed WAL payload could
  declare a very large operation count before the decoder allocated the
  operation vector.
- WAL decode now checks the declared operation count against the remaining
  payload bytes before allocation. The check uses the smallest possible encoded
  operation size, so valid payloads still decode normally.
- A regression test feeds a payload with `u32::MAX` operations and no operation
  bytes, and verifies the decoder rejects it without large allocation.

### Interpretation

- Task039 is complete for the WAL operation-count resource bound.
- Risk category: local recovery resource exhaustion.

### Verification

- Manual audit of WAL decode count allocation
- `cargo test wal_decode_rejects_operation_count_before_large_allocation`
- `cargo fmt --check`
- `cargo clippy`

### Remaining Blockers

- Continue hardening audit for startup cleanup and manifest/table decode
  resource bounds.

### Recommended Next Action

- Audit startup cleanup plus manifest/table decode count allocation, then fix
  any local risk with a focused regression test.

## 2026-05-25: Manifest And Table Decode Resource Bounds Passed

### Observation

- Startup cleanup was audited through `Db::open` and `recovery`: writer opens
  take a process lock, read-only opens do not take the writer lock, safe
  temporary files fail closed by default, explicit repair removes only known
  temporary files and writes a recovery report, and unreferenced table/blob
  files fail closed for operator review. Existing persistent recovery tests
  cover these paths.
- Manifest decode directly reserved a table list from the encoded table count.
- Table decode directly reserved vectors from encoded counts in index blocks,
  data blocks, range tombstone blocks, filter blocks, and data-block restart
  lists.
- Manifest and table decode now compare each declared count against the
  remaining payload bytes using the smallest valid encoded item size before
  reserving memory.
- Regression tests feed impossible `u32::MAX` counts and verify the decoders
  fail before large allocation.

### Interpretation

- Task040 is complete for startup cleanup audit and manifest/table decode
  resource bounds.
- Risk category: local recovery/read-path resource exhaustion from corrupted
  storage files.
- The fix does not change the manifest or table format; it rejects impossible
  existing-format inputs earlier.

### Verification

- Manual audit of startup cleanup, safe temporary file repair, unreferenced
  file handling, manifest decode, and table block decode.
- `cargo test manifest_decode_rejects_table_count_before_large_allocation`
- `cargo test table_decode_rejects`
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`
- forbidden terminology scan over source, tests, phase notes, benchmark files,
  docs, README, and examples

### Remaining Blockers

- Continue production hardening for flush/compaction cleanup and diagnostics
  after partial file writes or publish failures.

### Recommended Next Action

- Audit flush/compaction cleanup and diagnostics next, then fix any local risk
  with a focused regression test.

## 2026-05-26: Flush And Compaction Publish-Failure Cleanup Passed

### Observation

- Flush wrote table/blob files before publishing the manifest edit. If manifest
  publish failed after those files were written, the operation returned an error
  without removing the unpublished formal files.
- Compaction already removed unpublished table files on manifest publish
  failure, but large-value compaction output can also create a blob file with
  the same id as the output table.
- A shared cleanup helper now removes unpublished table files and matching blob
  files together after table-write failure or manifest-publish failure in flush
  and compaction.
- Regression tests force manifest publish failure by blocking the manifest temp
  path after database open. The flush test verifies unpublished table/blob
  files are removed and the memtable value remains readable. The compaction
  test verifies pre-existing table/blob files remain while unpublished
  replacement table/blob files are removed.

### Interpretation

- Task041 is complete.
- Risk category: local file cleanup after durable metadata publish failure.
- Phase 6 satisfies its acceptance gate: concrete operational risks were
  audited, local fixes have focused regression tests, and the full verification
  gate passed.

### Verification

- Manual audit of flush table/blob write, compaction table/blob write, manifest
  publish, and startup unreferenced-file behavior.
- `cargo test persistent_flush_publish_failure_removes_unpublished_table_and_blob_files`
- `cargo test persistent_compaction_publish_failure_removes_unpublished_table_and_blob_files`
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`
- forbidden terminology scan over source, tests, phase notes, benchmark files,
  docs, README, and examples

### Remaining Blockers

- None recorded for Phase 6.

### Recommended Next Action

- Choose the next phase from release packaging, CI/release verification,
  integration examples, or another targeted hardening audit based on fresh
  user priority.

## 2026-05-26: Release Packaging Gate Passed

### Observation

- User chose release packaging before integration examples and requested
  Semantic Versioning.
- `Cargo.toml` now records release-facing metadata for `0.1.0`, including
  readme, docs.rs documentation URL, keywords, category, and an explicit
  package include list.
- Initial `cargo package --list` included local workflow and skill files. The
  package include list now limits the crate package to source, tests, examples,
  benches, docs, changelog, license files, README, Cargo files, and Cargo's
  generated package metadata.
- `CHANGELOG.md`, `LICENSE-MIT`, `LICENSE-APACHE`, and `docs/release.md` were
  added. README and usage docs now show `trine-kv = "0.1"` for the release
  line plus the local path dependency form for checkout development.
- The long-term decision file now records the SemVer rule for public crate
  versions.

### Interpretation

- Task042 is complete.
- Phase 7 satisfies its acceptance gate.
- The crate release candidate remains `0.1.0`: v1 is the storage-engine
  protocol/version, while crate SemVer can start at `0.1.0`.

### Verification

- `cargo package --allow-dirty --list`
- `cargo package --allow-dirty`
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `cargo run --example quickstart`
- `git diff --check`
- forbidden terminology scan over source, tests, phase notes, Cargo metadata,
  benches, docs, README, examples, and changelog

### Remaining Blockers

- None recorded for Phase 7.

### Recommended Next Action

- Start Phase 8 integration examples.

## 2026-05-26: Integration Examples Gate Passed

### Observation

- Added `examples/user_store.rs`, a repository-style wrapper around `Db` and a
  `users` keyspace. It demonstrates persistent open, keyspace options,
  length-prefixed record encoding, prefix listing, transaction-backed
  conditional update, flush, reopen, and cleanup.
- Added `examples/event_index.rs`, a two-keyspace event log. It stores event
  payloads and an account lookup key in one write batch, then resolves account
  queries through the index keyspace.
- README, usage docs, and release checklist now list the integration examples.
- `cargo clippy --examples` found one minor string assignment issue in
  `user_store`; it was fixed with `clone_into`.

### Interpretation

- Task043 is complete.
- Phase 8 satisfies its acceptance gate.
- The examples did not reveal a public API blocker; the existing `Db`,
  `Keyspace`, transaction, prefix scan, and batch APIs were enough.

### Verification

- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo clippy --examples`
- `cargo package --allow-dirty --list`
- `cargo fmt --check`
- `cargo clippy`
- `cargo test`
- `git diff --check`
- forbidden terminology scan over source, tests, phase notes, Cargo metadata,
  benches, docs, README, examples, and changelog

### Remaining Blockers

- None recorded for Phase 8.

### Recommended Next Action

- Choose the next phase from CI/release verification, publishing workflow,
  more targeted hardening, or user-requested API changes.

## 2026-05-26: CI And Publishing Workflow Gate Passed

### Observation

- Added `.github/workflows/ci.yml` for pull requests, pushes to `main`, and
  manual dispatch. It runs formatting, strict clippy, all-target tests, the
  three examples, package-content guard, and package verification.
- Added `.github/workflows/publish.yml` as a manual workflow with `version` and
  `mode` inputs. It checks the requested SemVer version against `Cargo.toml`
  and `CHANGELOG.md`, runs the full gate, always runs `cargo publish --dry-run
  --locked`, and only runs `cargo publish --locked` when `mode=publish`.
- Release docs now explain CI verification, manual publish inputs, the
  `CARGO_REGISTRY_TOKEN` secret, the optional protected `crates-io`
  environment, and the recommended dry-run-first flow.
- Strict all-target clippy exposed old test style issues. Tests now use
  concrete option types instead of ambiguous `Default::default()` calls, and
  SSTable test helpers borrow table-write options instead of moving them.

### Interpretation

- Task044 is complete.
- Phase 9 satisfies its acceptance gate.
- The repository has a repeatable local release gate plus guarded GitHub
  workflows for CI and crates.io publishing.

### Verification

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- YAML syntax parse for `.github/workflows/ci.yml` and
  `.github/workflows/publish.yml`
- `cargo package --allow-dirty --list`
- `cargo package --allow-dirty --locked`
- `cargo publish --dry-run --allow-dirty --locked`
- `git diff --check`
- forbidden terminology scan over workflows, source, tests, phase notes, Cargo
  metadata, benches, docs, README, examples, and changelog

### Remaining Blockers

- GitHub Actions was not executed locally; the remote workflow must run after
  push.
- Real publish remains intentionally manual and requires `CARGO_REGISTRY_TOKEN`
  plus `mode=publish`.
- The first sandboxed `cargo package --allow-dirty --locked` attempt could not
  reach the crates.io index; the same command passed with approved network
  access.

### Recommended Next Action

- Configure the publish secret/environment, then use the `Publish` workflow with
  `mode=dry-run` before any release publish.

## 2026-05-26: Targeted Pre-Publish Directory Sync Hardening Passed

### Observation

- The publish paths for manifest, SSTable, blob, and recovery-report files
  already wrote a temporary file, synced the file contents, and renamed the
  temporary file into place.
- Those paths did not sync the parent directory after rename. On Unix
  filesystems, the rename changes the directory entry, and a crash can lose
  that directory entry unless the parent directory is synced.
- Added a shared `durability::sync_parent_dir_after_rename` helper. On Unix it
  opens and syncs the parent directory after the rename; on non-Unix it keeps
  the previous best-effort path because Rust `std` has no portable directory
  sync.
- Manifest, table, blob, and recovery-report publish paths now call the helper
  after successful rename.
- `docs/durability.md` now describes the Unix parent-directory sync behavior
  and the non-Unix boundary.

### Interpretation

- Task045 is complete.
- Phase 10 satisfies its acceptance gate.
- Risk category: local crash durability for newly published storage-file names.
- The change does not alter public API or any v1 file format.

### Verification

- `cargo test sync_parent_dir_after_rename_accepts_published_file`
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo package --allow-dirty --list`
- `cargo package --allow-dirty --locked`
- `cargo publish --dry-run --allow-dirty --locked`
- `git diff --check`
- forbidden terminology scan over workflows, source, tests, phase notes, Cargo
  metadata, benches, docs, README, examples, and changelog

### Remaining Blockers

- GitHub Actions was not executed locally; the remote workflow must run after
  push.
- Real publish still requires `CARGO_REGISTRY_TOKEN` plus an explicit
  `mode=publish` manual workflow dispatch.

### Recommended Next Action

- Configure the publish secret/environment, run the `Publish` workflow with
  `mode=dry-run`, review CI, then decide whether to publish.

## 2026-05-26: Windows Directory Sync Hardening Passed

### Observation

- Phase 10 left non-Unix parent-directory sync as best-effort because Rust
  `std` has no single portable directory sync API.
- Windows can still be handled concretely by opening the parent directory with
  backup semantics and calling `sync_all` on that handle.
- `durability::sync_parent_dir_after_rename` now has three platform branches:
  Unix opens and syncs the parent directory, Windows opens the directory with
  `FILE_FLAG_BACKUP_SEMANTICS` and share flags before `sync_all`, and other
  targets remain best-effort.
- `docs/durability.md` now states that Unix and Windows sync the parent
  directory after rename, while other targets keep the conservative best-effort
  path.

### Interpretation

- Task046 is complete.
- Phase 11 satisfies its acceptance gate.
- The supported desktop/server targets now cover parent-directory sync after
  atomic file publish without changing public API or v1 file formats.

### Verification

- `rustup target add x86_64-pc-windows-gnu`
- `cargo check --target x86_64-pc-windows-gnu`
- `cargo test sync_parent_dir_after_rename_accepts_published_file`
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo package --allow-dirty --list`
- `cargo package --allow-dirty --locked`
- `cargo publish --dry-run --allow-dirty --locked`
- `git diff --check`
- forbidden terminology scan over workflows, source, tests, phase notes, Cargo
  metadata, benches, docs, README, examples, and changelog

### Remaining Blockers

- GitHub Actions was not executed locally; the remote workflow must run after
  push.
- The Windows branch was compile-checked but not run on a real Windows
  filesystem in this environment.
- Targets other than Unix and Windows remain best-effort for parent-directory
  sync.

### Recommended Next Action

- Push and let CI run, then use the `Publish` workflow with `mode=dry-run`
  before any real publish.

## 2026-05-26: Pre-Publish Benchmark Tuning Passed

### Observation

- The current release-profile benchmark baseline showed the largest
  release-relevant cost in persistent write-heavy paths after adding
  parent-directory sync.
- `separated blob values` took 52841 us before tuning. That workload writes a
  blob file and an SSTable before publishing the manifest.
- Table/blob output paths previously synced the parent directory per output
  file. That made one table write with separated values pay for both blob and
  table directory syncs before the manifest directory sync.
- The implementation now syncs table/blob file contents before rename, batches
  one database-directory sync after all table/blob renames, and only then
  publishes the manifest.
- Manifest and recovery-report files still sync the parent directory
  immediately after their own rename because they are direct durable cutover or
  report files.
- `docs/benchmarks/v1-prepublish-tuning.md` records the before/after numbers
  and the scoped conclusion.

### Interpretation

- Task047 is complete.
- Phase 12 satisfies its acceptance gate.
- Risk category: local durable-write syscall cost.
- The tuning keeps the durability boundary: files are synced and their
  directory entries are synced before the manifest points at them.
- The reliable benchmark win is the separated-blob path: 52841 us before tuning
  versus a 46869 us post-tuning median across three runs, about 11.3 percent
  faster in this local session.
- Flush did not improve because its directory sync count is unchanged.
  Compaction remained noisy, so it should not be claimed as a win without a
  larger benchmark.

### Verification

- `cargo bench --bench v1_bench` before tuning
- `cargo bench --bench v1_bench` after tuning, three runs
- `cargo test sync_parent_dir_after_rename_accepts_published_file`
- `cargo test publish_failure_removes_unpublished_table_and_blob_files`
- `cargo check --target x86_64-pc-windows-gnu`
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo package --allow-dirty --list`
- `cargo package --allow-dirty --locked`
- `cargo publish --dry-run --allow-dirty --locked`
- `git diff --check`
- forbidden terminology scan over workflows, source, tests, phase notes, Cargo
  metadata, benches, docs, README, examples, and changelog

### Remaining Blockers

- GitHub Actions was not executed locally; the remote workflow must run after
  push.
- Benchmark numbers are local and noisy; compare only within the same machine
  and session.

### Recommended Next Action

- Push and let CI run, then use the `Publish` workflow with `mode=dry-run`
  before any real publish.

## 2026-05-26: Rust 1.85 CI Compatibility Fix Passed

### Observation

- Remote CI runs Rust 1.85 because `Cargo.toml` declares
  `rust-version = "1.85"` and the CI workflow installs
  `dtolnay/rust-toolchain@1.85.0`.
- Rust 1.85 rejects `Vec::len` and `Vec::is_empty` inside `const fn` bodies.
- `ValueRef::len`, `ValueRef::is_empty`, and
  `CompactionTable::has_key_bounds` no longer use `const fn`.
- The local machine has Rust 1.87 as the default toolchain and does not have
  Rust 1.85 installed.

### Interpretation

- Task048 is complete.
- Phase 13 satisfies its local acceptance gate.
- Keeping Rust 1.85 in CI is intentional because it protects the crate's
  declared MSRV. A newer stable job can be added later, but it should not
  replace the MSRV gate while `rust-version` remains 1.85.
- The fix changes compile-time const availability only; runtime behavior,
  public operation semantics, and storage formats are unchanged.

### Verification

- `cargo fmt --check`
- `cargo check --target x86_64-pc-windows-gnu`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo package --allow-dirty --list`
- `cargo package --allow-dirty --locked`
- `cargo publish --dry-run --allow-dirty --locked`
- `git diff --check`
- forbidden terminology scan over workflows, source, tests, phase notes, Cargo
  metadata, benches, docs, README, examples, and changelog
- targeted scan for the removed `const fn` names

### Remaining Blockers

- GitHub Actions was not executed locally; the remote workflow must run after
  push to confirm the exact Rust 1.85 environment.

### Recommended Next Action

- Push and let CI run. If the MSRV job passes, consider adding a second
  `stable` CI job for newer-toolchain coverage without weakening the Rust 1.85
  gate.

## 2026-05-26: Lazy Range Iterator Passed

### Observation

- User review identified eager range iteration as the wrong engine shape:
  range/prefix scans should advance with source cursors instead of prebuilding
  all visible rows.
- `Iter` now supports a lazy scan path that owns a snapshot pin, database path,
  range tombstones, and one source cursor per memtable/table source.
- Memtable scan setup uses user-key bounds against the `BTreeMap`, so it seeks
  into the requested key span before cloning the small source slice needed by
  the cursor.
- SSTable scans use `TablePointCursor`, which seeks to the first or last
  candidate block and advances by user-key groups. Block-cache access is
  recorded only when a table cursor advances into a block.
- The lazy merge chooses the next user key across sources, combines records for
  that key, sorts only that key's internal records, and applies MVCC point and
  range-delete visibility before returning the row.
- `persistent_range_iterator_defers_table_block_reads_until_next` confirms that
  constructing a range cursor leaves block-cache misses at zero, and the first
  `next()` touches the table block.
- Release benchmark spot-check after memtable seek optimization: bounded range
  scan 1529 us, prefix scan 2781 us, prefix table matching scan 2445 us in this
  local run.

### Interpretation

- Task049 is complete.
- Phase 14 satisfies its acceptance gate.
- The implementation fixes the range iterator shape without changing public
  scan semantics or storage formats.
- Prefix scan is slightly slower than the old eager baseline on this local
  microbenchmark, but bounded range scan is back within the previous baseline
  range after replacing whole-memtable cloning with bounded memtable seek.

### Verification

- `cargo test --test in_memory_iteration`
- `cargo test --test in_memory_range_delete`
- `cargo test persistent_range_iterator_defers_table_block_reads_until_next --test persistent_wal`
- `cargo test persistent_table_block_index_reads_points_and_ranges --test persistent_wal`
- `cargo test persistent_index_search_policies_preserve_table_reads --test persistent_wal`
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo check --target x86_64-pc-windows-gnu`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo bench --bench v1_bench`
- `git diff --check`
- forbidden terminology scan over workflows, source, tests, phase notes, Cargo
  metadata, benches, docs, README, examples, and changelog
- targeted scan for the removed eager range/prefix builders

### Remaining Blockers

- GitHub Actions was not executed locally; the remote workflow must run after
  push to confirm the exact Rust 1.85 environment.
- Tables are still loaded into memory as complete table objects. This phase
  makes scan advancement and blob value reads lazy over those loaded tables; it
  does not introduce an on-demand file-block decoder.

### Recommended Next Action

- Push and let CI run. If CI passes, consider a follow-up prefix-scan tuning
  slice only if release benchmarks keep showing the small local slowdown.

## 2026-05-26: Lazy Range Iterator Hardening Passed

### Observation

- Review found that the lazy range iterator still built a vector of all matching
  active memtable records before returning the iterator.
- `KeyspaceState` now stores the active memtable as an `Arc<Memtable>`.
- Range/prefix iterators clone the active memtable handle at creation and the
  memtable cursor advances by user-key group under bounded `BTreeMap` ranges.
- Flush publishes the SSTable, then swaps in a fresh active memtable. Existing
  iterators keep their old active memtable handle.
- `Transaction::read_range` now consumes its range cursor before recording the
  read range, so read-path errors are returned before the read set is accepted.

### Interpretation

- The Phase 14 lazy scan shape now covers both memtable and SSTable sources.
- Flush cannot change the active memtable records visible to an iterator that
  was created before the flush.
- The transaction range-read API keeps its previous validation behavior even
  though ordinary range scans are lazy.

### Verification

- `cargo test persistent_range_iterator_keeps_active_memtable_after_flush --test persistent_wal`
- `cargo test persistent_transaction_read_range_consumes_scan_before_tracking --test persistent_wal`
- `cargo test --test in_memory_transaction`
- `cargo test --test in_memory_iteration`
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo check --target x86_64-pc-windows-gnu`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo bench --bench v1_bench`
- `git diff --check`

### Remaining Blockers

- GitHub Actions was not executed locally; the remote workflow must run after
  push to confirm the exact Rust 1.85 environment.
- Tables are still loaded as complete table objects; this remains outside the
  Phase 14 cursor-shape fix.

### Recommended Next Action

- Push and let CI run. If CI passes, continue with the next evidence-selected
  release-readiness task.
