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

## 2026-05-26: Point Read Hot Path Passed

### Observation

- User benchmark review identified four point-read costs: snapshot-backed reads
  took a second snapshot pin, block-cache hits used one global exclusive lock,
  point lookup collected and sorted records, and memtable point lookup scanned
  the full active memtable.
- Snapshot-backed `Keyspace::get_at` now tells `Db` when the caller already
  holds a snapshot pin.
- Point reads now seek the active memtable by internal-key bounds and keep only
  one newest visible candidate across memtable and SSTables.
- SSTable point reads now use a focused newest-visible-record path instead of
  building a record vector for the key.
- Block-cache metadata is split into 64 shards. Hits use a shard read lock;
  misses use the shard write lock for insertion and eviction accounting.
- Local release benchmark spot-check: random get improved from 10233 us to
  783 us, missing get from 9276 us to 403 us, and block cache warm read from
  1373 us to 913 us.

### Interpretation

- Task051 is complete.
- Point read still returns an owned value because the v1 public API requires it,
  but intermediate record vectors, sorting, and repeated snapshot pinning are
  removed from the hot path.
- The block-cache change should reduce multi-thread hit contention; the real
  4-thread vs 32-thread proof still belongs to the external benchmark harness.

### Verification

- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --test in_memory_mvcc --test in_memory_range_delete --test persistent_wal`
- `cargo test persistent_block_cache_records_hits_and_misses --test persistent_wal`
- `cargo test --all-targets --all-features`
- `cargo check --target x86_64-pc-windows-gnu`
- `cargo fmt --check`
- `cargo bench --bench v1_bench`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `git diff --check`
- forbidden terminology scan over source, tests, benches, docs, README, Cargo
  metadata, changelog, examples, and phrase files

### Remaining Blockers

- GitHub Actions was not executed locally; remote CI must run after push.
- The external multi-thread benchmark should be rerun to confirm 32-thread
  scaling after the block-cache sharding and point-read changes.

### Recommended Next Action

- Run the external benchmark harness that exposed the 4-thread vs 32-thread
  scaling issue.

## 2026-05-26: LSM Write Path And WAL Lifecycle Passed

### Observation

- User audit identified three P1 write-path risks: only an active memtable, no
  size-driven freeze/flush path, and WAL replay that decoded old flushed
  batches forever.
- Keyspaces now keep an immutable memtable queue beside the active memtable.
- Persistent writes freeze all non-empty active memtables when
  `write_buffer_bytes` is reached.
- Reads, range/prefix scans, range tombstones, and transaction validation now
  include immutable memtables before SSTables.
- `Db::flush()` first freezes active memtables, then flushes immutable
  memtables to L0 SSTables.
- `max_immutable_memtables` is now write-side pressure: queued immutable
  memtables are flushed before accepting the next write.
- After flushed SSTables are manifest-published, the WAL is atomically rewritten
  through `trine.wal.tmp` to keep only batches newer than the replay floor.
- Startup now uses `read_batches_after(replay_floor)`, which validates old WAL
  frames but does not rebuild operation lists for batches already represented by
  published SSTables.

### Interpretation

- Phase 16 is complete for the foreground LSM write path.
- Auto-freeze happens after a successful write; pressure flush happens before
  the next write so an I/O error does not make the new write's result
  ambiguous.
- The WAL format did not change; this phase adds a safe checkpoint rewrite path
  and safe recovery cleanup for interrupted WAL rewrites.

### Verification

- `cargo test --test persistent_wal`
- `wal_decode_after_floor_skips_old_operation_payloads`
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo check --target x86_64-pc-windows-gnu`
- `git diff --check`

### Remaining Blockers

- GitHub Actions was not executed locally; remote CI must run after push.
- SSTables are still loaded as complete table objects at open time.
- Background worker scheduling, target-size table splitting, probabilistic
  Bloom bits, and concrete Eytzinger/galloping layouts remain later phases.

### Recommended Next Action

- Move next to file-backed SSTable block loading and table-cache work, because
  the write path now has a stable active/immutable/flush/WAL lifecycle to build
  on.

## 2026-05-26: File-Backed SSTable Reader Passed

### Observation

- User audit identified full SSTable loading at open time as the P0 production
  risk for startup time, memory, and random reads.
- Persistent table open now reads only table header, footer, properties, index,
  and filter sections. Data blocks remain on disk until a read cursor, point
  read, compaction, stats pass, or blob cleanup needs their records.
- Data block reads now verify checksum, codec, decoded record order, index
  bounds, and filter false-negative safety before returned records are used.
- `BlockCache` stores decoded data blocks behind sharded locks. Hits return
  shared decoded blocks; misses read and verify outside the cache lock.
- `KeyspaceOptions::block_bytes` now controls data block splitting.
- Table and manifest metadata now carry referenced blob file ids. Startup can
  reject unreferenced formal blob files without scanning data blocks, and
  compaction outputs can retain references to older blob files safely.
- Table and manifest on-disk format versions moved to 3 for the metadata
  change.

### Interpretation

- Phase 17 is complete for the P0/P1 table-read slice.
- Missing-key point reads can avoid data block reads when table/filter metadata
  rules out the key.
- In-memory mode still uses memory-resident memtables and all in-memory tests
  pass.
- This phase intentionally leaves exact-set filters, compaction streaming, and
  tombstone query indexing for later phases.

### Verification

- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- Focused coverage:
  - `persistent_reopen_defers_data_block_checksum_until_read`
  - `persistent_filter_miss_does_not_read_corrupt_data_block`
  - `persistent_block_cache_records_hits_and_misses`
  - `configured_block_bytes_controls_data_block_count`
  - blob compaction/reopen/recovery tests in `tests/persistent_wal.rs`

### Remaining Blockers

- GitHub Actions was not executed locally; remote CI must run after push.
- Bloom filters are still exact sets and should be replaced before claiming
  realistic filter memory costs.
- Compaction still collects complete input records and does not split output by
  target table size.
- Range tombstones still use a table-level on-demand list instead of a query
  structure.

### Recommended Next Action

- Implement real Bloom bitsets and partitioned filters now that SSTable reads
  have a true metadata/data-block boundary.

## 2026-05-26: Real Bloom Filters Passed

### Observation

- Point-key and prefix filters now store compact Bloom bitsets instead of
  complete key or prefix sets.
- `bits_per_key` and `bits_per_prefix` determine Bloom bit count. Hash count is
  derived from the bit budget and capped to keep read-path CPU bounded.
- Table-level and block-level filters share the same Bloom implementation.
- Table filter/index block encoding now writes Bloom metadata and bit bytes.
- Block validation checks every decoded record against its block filters and
  fails closed if a filter misses its own key or prefix.
- `FilterPolicy::Bloom` and `PrefixFilterPolicy::Bloom` reject zero bit budgets
  during keyspace option validation.

### Interpretation

- Phase 18 is complete.
- Filter memory now follows the configured cost model instead of growing with
  full key/prefix byte length.
- Bloom false positives can read an extra candidate block, but false negatives
  are treated as table corruption before records affect reads.

### Verification

- `cargo check --all-targets --all-features`
- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- Focused coverage:
  - `point_filter_bit_count_tracks_bits_per_key`
  - `point_filter_round_trips_from_parts`
  - `prefix_filter_uses_extractor_prefixes`
  - `data_block_filter_false_negative_fails_closed`
  - `prefix_block_filter_false_negative_fails_closed`
  - `persistent_filter_miss_does_not_read_corrupt_data_block`

### Remaining Blockers

- GitHub Actions was not executed locally; remote CI must run after push.
- Compaction still collects complete input records and does not split output by
  target table size.
- Range tombstones still use a table-level on-demand list instead of a query
  structure.

### Recommended Next Action

- Move to compaction output sizing and level scoring, unless delete-heavy
  workloads make range tombstone query structures the sharper next risk.

## 2026-05-26: Leveled Compaction And Range Tombstone Queries Passed

### Observation

- Range tombstones now use an ordered query index shared by memtable, SSTable,
  point-read, transaction, and scan setup paths.
- Point reads ask only for tombstones whose bounds can cover the requested key.
- Range and prefix scans collect only tombstones overlapping the scan selector.
- SSTable tombstone blocks remain on disk and are loaded on demand through the
  table tombstone query path.
- Compaction planning now uses L0 file pressure and L1+ level-size pressure.
- L0 compaction groups overlapping L0 tables and overlapping L1 tables.
- L1+ compaction moves selected inputs down one level with overlapping
  next-level inputs.
- Compaction merges table cursors by user key and splits output SSTables at
  user-key boundaries according to `target_table_bytes`.
- Full-keyspace compaction can drop range tombstones with no retained covered
  put and clips retained tombstones to output key spans. Partial compaction
  keeps original tombstone bounds.

### Interpretation

- Phase 19 is complete for the P3/P4 hardening slice.
- The engine no longer creates one giant compaction output for a large input
  set.
- Range tombstone reads no longer need to inspect every tombstone in the
  database for point reads or scan setup.
- Compaction still runs synchronously under the writer coordinator; background
  scheduling remains a separate phase.

### Verification

- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --all`
- Focused coverage:
  - `covering_key_returns_only_possible_covering_tombstones`
  - `overlapping_range_returns_only_intersecting_tombstones`
  - `l0_plan_expands_overlapping_l0_group_and_lower_level_tables`
  - `no_l0_fallback_moves_shallowest_overlapping_level_down`
  - `overfull_level_score_picks_largest_pressure_ratio`
  - `persistent_compaction_splits_outputs_and_moves_overfull_l1_down`

### Remaining Blockers

- GitHub Actions was not executed locally; remote CI must run after push.
- Scan source merge is still linear across sources; a heap-based merge remains
  later iterator hardening.
- Flush and compaction are still foreground maintenance paths.

### Recommended Next Action

- Move to iterator merge hardening if read-path benchmarks remain the sharpest
  risk; otherwise move to background flush/compaction scheduling and write
  backpressure.

## 2026-05-26: Iterator Merge And Background Maintenance Passed

### Observation

- Lazy scan source selection now uses a heap keyed by user key and scan
  direction instead of checking every source on each `next()`.
- Source groups with the same user key are consumed together so MVCC visibility
  and range deletes still see the complete candidate set for that key.
- Persistent databases start maintenance worker threads when
  `background_worker_count > 0`.
- `background_worker_count == 0` keeps flush and compaction on explicit or
  foreground pressure paths.
- Background maintenance flushes immutable memtables first and then compacts L0
  pressure when needed.
- Background maintenance failures are stored once and returned by later writes,
  `flush()`, or `compact_range()`.
- In-memory databases still avoid background worker threads.

### Interpretation

- Phase 20 is complete for the P5/P6 slice.
- Range and prefix iterators now pay heap cost per candidate group instead of a
  linear scan across all sources per step.
- The write path now has an opt-in persistent background maintenance loop while
  preserving explicit maintenance semantics by default.

### Verification

- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- Focused coverage:
  - `source_heap_orders_forward_and_reverse_keys`
  - `lazy_scan_heap_merge_preserves_forward_and_reverse_order`
  - `maintenance_success_does_not_clear_unreported_error`
  - `persistent_background_workers_flush_and_compact_pressure`
  - `persistent_background_maintenance_error_surfaces_to_later_write`

### Remaining Blockers

- GitHub Actions was not executed locally; remote CI must run after push.
- Background work is thread-based and currently scoped to persistent databases.
- Benchmark follow-up should measure heap merge behavior against wide-source
  scan workloads and maintenance behavior under sustained write pressure.

### Recommended Next Action

- Run benchmark-guided follow-up for wide iterator merges and sustained
  write-load maintenance, or move to release-readiness review if CI is clean.

## 2026-05-26: LSM Core Boundary Spec Added

### Observation

- `db.rs` still owns database-wide coordination and one-keyspace tree rules in
  the same module.
- The mixed responsibilities include tree state, point read visibility, range
  and prefix scan setup, range tombstone lookup, flush input selection,
  compaction retention, and transaction conflict helpers.
- `.phrase/protocol/lsm-core-boundary-spec.md` now defines the target boundary
  between the database layer and one-keyspace `LsmTree`.
- The v1 protocol now links to the LSM core boundary spec as the internal module
  boundary source of truth.

### Interpretation

- Phase 21 should proceed as an incremental refactor, not a storage or public
  API change.
- MVCC visibility belongs in the LSM core so point read, scan, transaction
  validation, range tombstone handling, and compaction share one rule set.
- The first safe code slice is to introduce `src/lsm/` and move tree state
  behind `LsmTree` before moving read behavior.

### Verification

- `.phrase/protocol/lsm-core-boundary-spec.md` written.
- `.phrase/protocol/trine-kv-v1-spec.md` links the boundary spec.
- `git diff --check`
- forbidden-term scan

### Remaining Blockers

- No Rust code has been moved yet.
- Remote CI cannot be executed locally; it must run after push.
- `AGENTS.md` has a pre-existing unstaged edit outside this phase.

### Recommended Next Action

- Start task069: create the internal LSM module and move tree state behind
  `LsmTree` without changing behavior.

## 2026-05-26: LSM Tree State Boundary Moved

### Observation

- `src/lsm/` now contains the internal LSM module.
- `LsmTree` owns the one-keyspace tree state that previously lived directly in
  `db.rs`: keyspace options, active memtable, active range tombstones,
  immutable memtable queue, and table list.
- `RangeTombstone` and `ImmutableMemtable` moved into the LSM module with
  crate-local visibility.
- `DbInner.keyspaces` now stores `Arc<LsmTree>`.
- Table read ordering is now maintained through
  `LsmTree::sort_tables_for_reads`.

### Interpretation

- Task069 is complete.
- This slice changes ownership and module boundaries only. It does not change
  public API behavior, storage formats, WAL, manifest, MVCC rules, or
  compaction behavior.
- `Db` still calls tree fields directly for read, scan, flush, compaction, and
  transaction validation. That remaining coupling is the next task.

### Verification

- `cargo check --all-targets --all-features`
- `cargo fmt --all`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`

### Remaining Blockers

- Point read visibility still lives in `db.rs`.
- Range and prefix scan setup still lives in `db.rs`.
- Flush planning, compaction planning, and transaction conflict checks still
  live in `db.rs`.
- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Move point read visibility into `LsmTree` as task070, then run point read,
  tombstone, transaction, persistent, and full Rust verification.

## 2026-05-26: Point Read Visibility Moved To LSM Core

### Observation

- `src/lsm/read.rs` now owns point read visible-version selection.
- `LsmTree::read_visible_point` checks active memtable, immutable memtables,
  table candidates, point deletes, and range tombstone coverage for one user
  key.
- `LsmTree::memtable_range_tombstones` now owns the active plus immutable
  memtable tombstone query structure.
- `Db::get_at_with_pin_state` now delegates point read semantics to `LsmTree`
  after resolving the keyspace and read pin.

### Interpretation

- Task070 is complete.
- The point read path no longer has DB-level MVCC visible-version logic.
- Range and prefix scan setup still lives in `db.rs`, so scan visibility and
  source construction are the next extraction target.

### Verification

- `cargo check --all-targets --all-features`
- `cargo fmt --all`
- `cargo test point --all-targets --all-features`
- `cargo test tombstone --all-targets --all-features`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`

### Remaining Blockers

- Range and prefix scan setup still lives in `db.rs`.
- Flush planning, compaction planning, and transaction conflict checks still
  live in `db.rs`.
- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Move range and prefix scan setup into `LsmTree` as task071 while preserving
  lazy heap merge behavior and existing iterator tests.

## 2026-05-26: LSM Core Boundary Slice Completed

### Observation

- `src/lsm/scan.rs` now owns range and prefix scan source construction plus
  selector-scoped range tombstone collection for one tree.
- `src/lsm/write.rs` now owns write application, active memtable byte
  accounting, immutable memtable counting, and active memtable freeze.
- `src/lsm/flush.rs` now owns flush input planning and immutable memtable
  removal after a flushed table is installed.
- `src/lsm/compact.rs` now owns compaction input planning, point-version
  retention, range tombstone cleanup, output splitting, and table list
  replacement for one tree.
- `src/lsm/conflict.rs` now owns transaction point and range conflict checks.
- `Db` still coordinates WAL append/rewrite, manifest publish, sequence
  assignment, snapshots, process lock, background worker lifecycle, persistent
  file paths, and cross-keyspace batch boundaries.
- In-memory mode continues to use the same `LsmTree` path.

### Interpretation

- Phase 21 is complete: the remaining tree-local read, write, flush,
  compaction, and conflict rules have moved behind the LSM core boundary
  without public API or storage-format changes.
- `db.rs` is still the database coordinator, but no longer owns the extracted
  one-keyspace rules from the Phase 21 task slice.

### Verification

- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test scan --all-targets --all-features`
- `cargo test prefix --all-targets --all-features`
- `cargo test flush --all-targets --all-features`
- `cargo test compaction --all-targets --all-features`
- `cargo test transaction --all-targets --all-features`
- `cargo test range --all-targets --all-features`
- `cargo test --all-targets --all-features`
- `cargo fmt --check`
- `git diff --check`
- forbidden-term scan

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- `AGENTS.md` has a pre-existing unstaged edit outside this phase.

### Recommended Next Action

- Run remote CI. Choose the next phase from fresh CI, benchmark, or review
  evidence rather than old coupling notes.

## 2026-05-26: LSM Tree Improvement Suggestions Classified

### Observation

- User review supplied P0-P10 as improvement suggestions for the LSM tree, not
  as a request to report whether the previous phase had completed all of them.
- User review identified that the next useful LSM core improvement is explicit
  invariants, version handles, and real level layout.
- Current `LsmTree` still stores tables as a flat `RwLock<Vec<Arc<Table>>>`.
- Current flush and compaction install mutate the table list after
  database-layer publish, instead of constructing and swapping a validated tree
  version.
- Current read paths do not hold a named version handle for the whole read.
- A version handle should reduce table-layout lock access from repeated lock
  reads during one operation to one current-version handle acquisition at read
  setup.
- The next review list also identifies later risks in memtable accounting,
  SSTable lookup details, filters, compaction picking, MVCC delete semantics,
  read-path level optimization, blob GC, and randomized verification.

### Interpretation

- Phase 21 should remain closed as the boundary extraction slice, but it is not
  the same as a mature versioned LSM design.
- The next phase should focus narrowly on `LsmVersion`, `LevelState`, and
  version publish semantics before table-block, filter, blob, or benchmark
  work.
- A RocksDB-style SuperVersion target should be interpreted as a stable
  read-held bundle of active memtable, immutable memtables, and current table
  version. Trine can start with `Arc<LsmVersion>` plus existing memtable handles
  before adding deeper lifetime accounting.
- The immediate performance target is not only cleaner ownership; it is also to
  make point reads and scan setup take one version handle and then run without
  repeated table-layout lock access.

### Verification

- Manual code audit of `src/lsm/tree.rs`, `src/lsm/flush.rs`,
  `src/lsm/compact.rs`, `src/db.rs`, and
  `.phrase/protocol/lsm-core-boundary-spec.md`.

### Remaining Blockers

- No `LsmVersion` or `LevelState` type exists yet.
- Level-overlap validation is not enforced at install time.
- Read path still starts from table snapshots rather than one named version
  handle.
- Later P3-P10 suggestions remain valid follow-up candidates after the version
  boundary is in place.

### Recommended Next Action

- Start Phase 22 with a spec addendum for version and level invariants, then
  implement `LsmVersion` and move read/install paths onto version handles.

## 2026-05-26: Versioned Level Layout First Slice

### Observation

- `.phrase/protocol/lsm-core-boundary-spec.md` now records the version and
  level-layout invariants: L0 overlap, L1+ non-overlap, flush into L0,
  version-swap install, and one read-held version handle.
- `src/lsm/version.rs` adds `LsmVersion` and `LevelState`.
- `LsmTree` now stores `current_version: RwLock<Arc<LsmVersion>>` instead of
  `RwLock<Vec<Arc<Table>>>`.
- Flush and compaction install build a replacement `LsmVersion` and swap the
  current handle after database-layer publish succeeds.
- Point reads, scan setup, transaction conflict checks, compaction planning,
  stats, recovery, and in-memory setup now use version handles.
- Point read, scan setup, and transaction conflict checks acquire the current
  version once per operation setup and reuse that handle underneath.
- Version construction validates L0 newest-first ordering and L1+ non-overlap.
- `Table::has_key_bounds()` now distinguishes empty-key tables from
  tombstone-only tables without point-key bounds.

### Interpretation

- Phase 22 has completed the version-handle and level-layout foundation, but
  should stay open.
- The next correctness blocker is old file lifetime: `Arc<LsmVersion>` keeps
  old table objects alive, but compaction still removes obsolete table files
  immediately after install. Lazy readers can hold old handles that have not
  loaded their data blocks yet.
- P3-P10 should remain later work until the old-version file lifetime rule is
  closed.

### Verification

- `cargo fmt`
- `cargo clippy --all-targets`
- `cargo test`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- Add a long lazy iterator plus compaction regression before changing old table
  file cleanup.

### Recommended Next Action

- Implement task079: protect old version file lifetimes after compaction, then
  rerun full local Rust verification.

## 2026-05-26: Phase 22 Versioned Level Layout Completed

### Observation

- Compaction now queues obsolete table ids and deletes their files only when no
  snapshot/read pin is active.
- `Db::flush()`, `Db::close()`, and `DbInner::drop()` attempt pending obsolete
  table cleanup when it is safe.
- `persistent_compaction_keeps_lazy_iterator_table_files_until_pin_released`
  proves a lazy range iterator can be created before compaction, read an old
  table block after compaction publishes, and then allow cleanup after its pin
  is released.
- `persistent_compaction_rewrites_tables_and_preserves_reads` now checks that
  snapshot-pinned old tables stay on disk until the snapshot is dropped.
- `lsm::version::tests::old_version_handle_keeps_previous_tables_after_replacement`
  proves version replacement does not mutate an old held version handle.

### Interpretation

- Phase 22 acceptance is met locally: tree versions, level layout, read-held
  version handles, publish-time validation, recovery/in-memory parity, and old
  compacted-table file lifetime protection are in place.
- The next phase should move to P3: memtable accounting and flush scheduling.

### Verification

- `cargo fmt`
- `cargo fmt --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo test persistent_compaction --test persistent_wal`
- `cargo test --all-targets --all-features`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, and `tests`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Start Phase 23 with task080: audit memtable byte accounting, keyspace-local
  freeze behavior, and immutable queue pressure before code changes.

## 2026-05-26: Phase 23 Memtable And Flush Scheduling Completed

### Observation

- `Memtable` now maintains an atomic byte estimate on insert/replace, so normal
  write-buffer checks no longer scan the full `BTreeMap`.
- Active range tombstone bytes are tracked incrementally and folded into
  active memtable byte checks.
- Frozen immutable memtables carry their byte estimate, so stats no longer
  re-scan immutable memtable entries to compute memtable bytes.
- Post-commit write-buffer freezing now checks only keyspaces touched by the
  committed batch.
- Immutable pressure flush before a write now flushes only keyspaces whose
  immutable queue reached the configured limit.
- In-memory mode now uses the same write-buffer freeze path and reads frozen
  immutable memtables.

### Interpretation

- Phase 23 acceptance is met locally: byte accounting is incremental, hot
  keyspaces no longer move unrelated keyspaces during freeze/pressure flush,
  pressure behavior is tested, and in-memory mode shares the immutable read
  path.
- The next phase should move to P4 SSTable read-path details.

### Verification

- `cargo fmt --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo test estimated_bytes_tracks_insert_and_replace --lib`
- `cargo test write_buffer --test persistent_wal`
- `cargo test immutable_pressure --test persistent_wal`
- `cargo test write_buffer --test in_memory_mvcc`
- `cargo test --all-targets --all-features`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Start Phase 24 with task083: audit SSTable read-path detail gaps in
  `src/table.rs`, `src/cache.rs`, and focused table/cache tests.

## 2026-05-26: Phase 24 SSTable Read Path Detail Hardening Completed

### Observation

- Persistent table open already reads footer/properties/index/filter metadata
  first and keeps data blocks lazy.
- Decoded data blocks now build an in-memory user-key hash index so point reads
  jump directly to the contiguous version range for a key inside the block.
- Block cache keys now include block kind classes for data, index, filter,
  range-tombstone, and blob-related blocks.
- Cache hits now promote the touched key before eviction decisions, including
  the race path where another reader inserted the block during a miss.
- Persistent `Table` now keeps an open file handle and lazy block/range-tombstone
  reads clone that handle instead of opening the table path for each block.
- No public API or storage format changed.

### Interpretation

- Phase 24 acceptance is met locally: point lookup has a per-block hash-index
  path, cache keys are ready for additional block classes, cache replacement is
  no longer FIFO-only on hits, and file-handle reuse is covered by a focused
  block-read test.
- The next phase should move to P5: filter observability and stronger prefix
  skip-path proof.

### Verification

- `cargo fmt --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo test data_block_point_lookup_uses_hash_index --lib`
- `cargo test block_read_uses_cached_file_handle --lib`
- `cargo test cache_ --lib`
- `cargo test --all-targets --all-features`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- Filter hit/miss/false-positive stats are not exposed yet; they are the next
  phase.

### Recommended Next Action

- Start Phase 25 with task086: audit filter read-path and stats gaps before
  changing counters or prefix-scan behavior.

## 2026-05-26: Phase 25 Filter Strategy Observability Completed

### Observation

- `DbStats` now exposes table/block point and prefix filter hit, miss, and
  false-positive counters.
- Tables own filter counters and `Db::stats()` aggregates them across the
  current version handles.
- Table point filters count hits/misses during point table selection.
- Block point and prefix filters count hits/misses when deciding whether to load
  a data block.
- False positives are counted only after a filter-allowed candidate is checked
  and no matching user key/prefix exists in the loaded block.
- Prefix scans with a matching extractor now have a regression proving a
  nonmatching prefix avoids data-block reads and increments filter miss stats.
- No storage format changed.

### Interpretation

- Phase 25 acceptance is met locally: filter behavior is observable, prefix miss
  skip behavior is tested, and the next tuning decisions can use stats instead
  of guessing.
- The next phase should move to P6 compaction picker hardening.

### Verification

- `cargo fmt --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo test persistent_filter_miss_does_not_read_corrupt_data_block --test persistent_wal`
- `cargo test persistent_prefix_filter_stats_skip_nonmatching_tables --test persistent_wal`
- `cargo test --all-targets --all-features`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- Compaction picker refinements remain for Phase 26.

### Recommended Next Action

- Start Phase 26 with task089: audit compaction picker gaps before changing
  input selection or move behavior.

## 2026-05-26: Phase 26 Compaction Picker Hardening Completed

### Observation

- L0 compaction now plans a single overlapping L0 input even when there is no
  lower-level overlap, allowing the table to move down one level without
  rewriting.
- L1+ compaction now selects a narrow overfull-level input table and adds only
  overlapping next-level tables, instead of rewriting the whole level.
- Compaction outputs can include a direct table move that reuses the table id
  and file while publishing the new level in the manifest.
- Recovery treats the manifest level as the live placement and validates every
  other table property against the table file.
- Protocol docs now record manifest-level authority and direct one-table move
  rules.
- Tests cover L0 direct moves, narrow L1 input selection, lower-level overlap
  inclusion, persistent reopen after a direct move, stats for moved tables, and
  the updated split-output compaction expectation.

### Interpretation

- Phase 26 acceptance is met locally: picker scope is narrower, L0 overlap
  closure remains intact, direct table moves are supported without storage
  format changes, and target-size output splitting still works.
- The next phase should address P7: MVCC and deletion semantics hardening.

### Verification

- `cargo fmt --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo test single_l0_without_lower_overlap_is_planned_for_move --lib`
- `cargo test overfull_level_uses_narrow_input_and_lower_overlap --lib`
- `cargo test overfull_level_without_lower_overlap_selects_single_move_input --lib`
- `cargo test persistent_single_l0_compaction_moves_table_without_rewrite --test persistent_wal`
- `cargo test persistent_reopen_fails_when_table_metadata_differs_from_manifest --test persistent_wal`
- `cargo test persistent_compaction_splits_outputs_and_moves_overfull_l1_down --test persistent_wal`
- `cargo test --all-targets --all-features`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, and `tests`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- P7 MVCC/delete semantics, P8 read-path level optimization, P9 blob GC, and
  P10 verification expansion remain open.

### Recommended Next Action

- Start Phase 27 with task092: audit existing MVCC/delete retention tests and
  compaction cleanup rules before changing semantics.

## 2026-05-26: Phase 27 MVCC And Deletion Semantics Hardening Completed

### Observation

- Phase 26 made `compact_range(KeyRange::all())` capable of selecting a narrow
  L1+ input rather than every live table.
- Range tombstone cleanup previously used the requested range to decide whether
  tombstones could be clipped or removed.
- `CompactionInput` now records whether the selected input covers every live
  table in the keyspace; range tombstone cleanup and output clipping use that
  flag instead of only checking the requested range.
- Focused tests prove that a range-all request with a narrow input is not full
  keyspace cleanup, while all-live-table input is.
- Output payload tests prove partial compaction keeps original range tombstone
  bounds and full-keyspace compaction may clip to the output span.
- `RangeTombstoneIndex` now has a deterministic random reference test for
  covering-key and overlapping-range queries.
- Protocol docs now state that a request over all user keys is still partial
  when the picker did not include every live table.

### Interpretation

- Phase 27 acceptance is met locally: MVCC point-version retention remains
  covered, point-delete cleanup remains snapshot-aware, and range-delete cleanup
  is now tied to input coverage rather than the caller's requested range.
- The next phase should move to P8 read-path level optimization.

### Verification

- `cargo fmt --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo test range_all_compaction_is_not_full_when_picker_chooses_narrow_input --lib`
- `cargo test range_all_compaction_is_full_when_all_tables_are_inputs --lib`
- `cargo test partial_compaction_keeps_original_range_tombstone_bounds --lib`
- `cargo test full_compaction_clips_range_tombstone_to_output_span --lib`
- `cargo test randomized_queries_match_brute_force_reference --lib`
- `cargo test --all-targets --all-features`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, and `tests`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- P8 read-path level optimization, P9 blob GC, and P10 verification expansion
  remain open.

### Recommended Next Action

- Start Phase 28 with task095: audit current point/scan table selection against
  the versioned level layout before changing source selection.

## 2026-05-26: Phase 28 Level-Aware Read Path Optimization Completed

### Observation

- Point record reads already used `LsmVersion::point_lookup_tables`, but table
  range tombstone checks still looped all live tables.
- Range and prefix scan setup selected all live tables before table-local
  filtering.
- `Table` now exposes key-bound checks that do not consult point filters.
- `LsmVersion` now exposes point tombstone candidates by key and scan
  candidates by query range.
- Point reads use key-bound table candidates for range tombstone coverage, so
  point filters cannot hide range tombstones.
- Range and prefix scans use key-bound-overlap candidates before building table
  cursors or loading table range tombstones.
- Prefix filter stats tests now account for Bloom false positives and the fact
  that range tombstone metadata remains authoritative.
- Protocol docs now record level-aware point reads and range/prefix table
  selection.

### Interpretation

- Phase 28 acceptance is met locally: point reads keep L0 overlap behavior and
  one non-overlapping candidate per deeper level, scans skip unrelated tables,
  and range tombstones remain table scoped and authoritative.
- The next phase should move to P9 blob GC hardening.

### Verification

- `cargo fmt --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo test point_lookup_uses_l0_overlaps_and_one_deeper_table_per_level --lib`
- `cargo test range_scan_tables_skip_unrelated_non_overlapping_tables --lib`
- `cargo test range_tombstone_lookup_uses_key_bounds_without_point_filter --lib`
- `cargo test persistent_prefix_filter_stats_skip_nonmatching_tables --test persistent_wal`
- `cargo test --all-targets --all-features`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, and `tests`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- P9 blob GC and P10 verification expansion remain open.

### Recommended Next Action

- Start Phase 29 with task098: audit current blob stats, compaction cleanup,
  and recovery consistency before changing blob lifecycle behavior.

## 2026-05-26: Phase 29 Blob GC Hardening Completed

### Observation

- Existing compaction tests already covered live blob survival, stale blob file
  removal after dropped versions, delete cleanup, and failed publish cleanup.
- `DbStats` exposed live and obsolete blob bytes, but not the stale naming from
  the P9 checklist.
- Missing referenced blob files previously failed when the value was read, not
  at persistent open.
- `DbStats` now exposes `stale_blob_files` and `stale_blob_bytes` while keeping
  the existing obsolete fields.
- Persistent open now fails closed when a blob id referenced by manifest table
  metadata has no formal blob file.
- Protocol docs now list live, stale, and obsolete blob byte stats.

### Interpretation

- Phase 29 acceptance is met locally: blob lifecycle stats are clearer, cleanup
  remains compaction/version/snapshot gated, and recovery now verifies
  referenced blob file presence.
- The next phase should move to P10 verification expansion.

### Verification

- `cargo fmt --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo test persistent_reopen_fails_when_referenced_blob_file_is_missing --test persistent_wal`
- `cargo test persistent_stats_report_tables_blobs_and_compactions --test persistent_wal`
- `cargo test persistent_compaction_removes_blob_files_for_dropped_versions --test persistent_wal`
- `cargo test persistent_compaction_removes_blob_files_after_delete_cleanup --test persistent_wal`
- `cargo test --all-targets --all-features`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, and `tests`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- P10 verification expansion remains open.

### Recommended Next Action

- Start Phase 30 with task101: add a deterministic randomized MVCC model test
  against a small reference implementation.

## 2026-05-26: Phase 30 Verification Expansion Completed

### Observation

- Added `tests/model_reference.rs`, a deterministic randomized integration test
  that compares Trine with a small MVCC `BTreeMap` reference model.
- The model test covers point writes, point deletes, range deletes, point
  reads, full range scans, snapshots, flush, compaction, and reopen.
- The first run exposed a real partial-compaction bug: a point delete could be
  dropped after the selected input tables had no older local value, even though
  a lower live level still contained an older value for the same user key.
- Partial compaction now keeps point deletes unless the compaction input covers
  the whole live keyspace.
- A scan merge bug was also fixed: when multiple sources had the same user key,
  the previous first record is now retained before the next source group becomes
  the group head.
- Protocol docs now record that point-delete cleanup must be based on the live
  keyspace, not only selected compaction inputs.

### Interpretation

- Phase 30 acceptance is met locally. Randomized MVCC comparison now runs with
  normal `cargo test`, and it protects the point-delete/partial-compaction case
  that focused tests had missed.
- The P0-P10 LSM hardening checklist from the current roadmap has no remaining
  local implementation item. Remote CI remains the only unverified gate.

### Verification

- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo fmt --check`
- `cargo test point_tombstone --lib`
- `cargo test --test model_reference`
- `cargo test --all-targets --all-features`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, and `tests`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Push to CI when ready, then treat any remote failure as fresh evidence for
  the next phase.

## 2026-05-26: Phase 31 Default Bucket API Polish Completed

### Observation

- The public API now exposes `Bucket`, `BucketName`, and `BucketOptions` for
  named buckets, with `Db::open_bucket` and `Db::open_bucket_with_options` as
  the named-bucket entry points.
- `Db::put`, `Db::get`, `Db::range`, `Db::prefix`, and their snapshot/reverse
  variants now operate on the built-in default bucket.
- Memory and persistent open paths both create or load the default bucket
  before reads and writes run.
- `WriteBatch` and `Transaction` now expose `put`, `delete`, and
  `delete_range` helpers; public batch operations use `Put`, `Delete`, and
  `DeleteRange` variants.
- Usage docs, examples, public protocol docs, durable boundary docs, tests,
  benches, and stats naming now use bucket terminology.

### Interpretation

- Phase 31 acceptance is met locally: simple users can call direct `Db`
  helpers, while advanced users can still create named buckets for isolation or
  different options.
- The change is a pre-1.0 public API break and remains within the existing
  Semantic Versioning rule for the crate.

### Verification

- `cargo fmt --check`
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo test --test persistent_wal`
- `cargo test --test scaffold`
- `cargo test --all-targets --all-features`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `git diff --check`
- public old-name scan over current `.phrase` docs, `src`, `tests`,
  `examples`, `docs`, `README.md`, and `benches`
- forbidden-term scan over `.phrase`, `src`, `tests`, `examples`, `docs`,
  `README.md`, and `benches`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Commit the Phase 31 API polish, then push to CI when ready.

## 2026-05-26: Phase 32 Titan-Like Large-Value Storage Spec Completed

### Observation

- User requested a Titan-like large-value subsystem and explicitly required the
  first implementation step to be a spec update.
- Current Trine code already has `ValueRef::Inline` and primitive blob
  references, but the existing blob file is just value bytes at offsets and
  does not store the key/version metadata needed for robust GC validation.
- The new protocol `.phrase/protocol/titan-like-blob-storage-spec.md` defines
  the target design: small values stay inline, large values separate during
  flush/compaction, `BlobIndex` carries checksummed record metadata, blob
  records store internal-key metadata, and GC is snapshot-safe and recoverable.
- The v1 protocol now points large-value behavior at the new blob storage
  protocol.
- The durable decision framework now records that Titan can be used as a design
  reference only; Trine keeps its own code, file formats, tests, and recovery
  contract.

### Interpretation

- Phase 32 acceptance is met as a spec-only phase.
- The next implementation phase should stabilize `BlobIndex` and `BlobFile`
  encode/decode tests before changing flush behavior.
- Existing code still has the primitive blob path; that is intentional until
  Phase 33 replaces the format under tests.

### Verification

- Reviewed `.phrase/decision.md`, `.phrase/current.md`, `.phrase/roadmap.md`,
  and current blob-related protocol/code paths.
- Reviewed Titan overview/config/repo/article as design references only.
- Updated `.phrase/protocol/titan-like-blob-storage-spec.md`.
- Updated `.phrase/protocol/trine-kv-v1-spec.md`.
- Updated `.phrase/current.md`, `.phrase/roadmap.md`, `.phrase/evidence.md`,
  and `.phrase/decision.md`.

### Remaining Blockers

- New `BlobIndex` and `BlobFile` format are not implemented yet.
- Blob GC is not implemented yet.
- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Start Phase 33 with encode/decode tests for the new `BlobIndex` and
  `BlobFile` format.

## 2026-05-26: Phase 33 Bucket API Contract Hardening Completed

### Observation

- API audit found tests, benches, and protocol docs still treating `"default"`
  as a named bucket in some places.
- `WriteBatch` and `Transaction` default writes no longer require callers to
  pass a bucket name.
- Named-bucket staging methods now return `Result<()>` and reject empty names
  plus the reserved `"default"` name before a batch is submitted.
- Default bucket options are configured through `DbOptions`, including the new
  `with_default_bucket_options` helper.
- Usage docs and protocol docs now describe the default/named bucket boundary.

### Interpretation

- The public API is now clearer for the common path: `Db` operates directly on
  the default bucket, while named bucket methods are explicit.
- Rejecting `"default"` at `open_bucket` and named staging sites prevents the
  default bucket from being used accidentally through the named-bucket API.
- This is a pre-1.0 public API break and stays within the current Semantic
  Versioning rule.

### Verification

- `cargo fmt --all --check`
- `cargo check --all-targets --all-features`
- `cargo test --test in_memory_iteration --test in_memory_mvcc --test in_memory_range_delete --test in_memory_transaction`
- `cargo test --test persistent_wal --test scaffold`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, and `README.md`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Continue with Phase 34 `BlobIndex` and `BlobFile` format tests before wiring
  flush to the new large-value file format.

## 2026-05-26: Bucket API Naming Follow-Up Completed

### Observation

- User feedback identified `open_bucket` as too long and less accurate for the
  intended get-or-create behavior.
- `Db::bucket(name)` is now the primary named-bucket entrypoint.
- `Db::bucket_with_options(name, options)` is now the explicit custom-options
  entrypoint.
- The older `open_*` named-bucket methods were removed from the public API
  surface because this is still pre-1.0 API polish.

### Interpretation

- The common API now reads as `db.put(...)` for the default bucket and
  `db.bucket("users")?` for an optional named bucket.
- The custom-options path remains available only where the caller is deciding
  durable bucket configuration.

### Verification

- `cargo fmt --all --check`
- `cargo check --all-targets --all-features`
- `cargo test --test in_memory_iteration --test in_memory_mvcc --test persistent_wal --test scaffold`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Continue with Phase 34 `BlobIndex` and `BlobFile` format tests.

## 2026-05-26: README Capability Pass Completed

### Observation

- User feedback found the README too minimal for evaluating Trine's common
  capabilities.
- README now leads with a capability inventory and a common API example that
  covers the default bucket, named buckets, custom prefix extraction,
  snapshots, atomic batches, optimistic transactions, and range scans.
- README still points to the runnable quickstart for persistent open, flush,
  reopen, and stats.

### Interpretation

- New readers get a clearer first impression of what Trine can do before
  opening the longer usage guide.
- The README stays honest about boundaries by keeping durability and release
  details in the existing docs.

### Verification

- `cargo run --example quickstart`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, and `README.md`
- old bucket entrypoint scan over README, docs, examples, source, tests,
  benches, and current protocol docs

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Commit the bucket API and README polish, then continue with Phase 34
  `BlobIndex` and `BlobFile` format tests.

## 2026-05-26: Phase 34 Blob Format Foundation Completed

### Observation

- Added `BlobIndex` with file id, record offset, encoded length, decoded value
  length, value checksum, record checksum, and compression id.
- Added a Trine-owned blob file encoder/decoder with header, record frames,
  properties block, footer, checksum validation, ordered-record checks, and
  exact indexed-value reads.
- Table value-reference serialization can now carry `BlobIndex` records while
  the current flush path still writes the primitive blob shape.
- Focused tests cover round trip, corrupt header, corrupt footer, corrupt
  properties, corrupt record checksum, corrupt value checksum, unsupported
  record compression id, unordered records, and exact index validation.

### Interpretation

- Phase 34 acceptance is met for the standalone format foundation.
- Keeping flush on the old path is intentional; it confines this phase to
  storage-format validation before changing publish/recovery behavior.

### Verification

- `cargo test blob::tests --all-features`
- `cargo fmt --all --check`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, and `README.md`

### Remaining Blockers

- Flush does not yet emit the new `BlobFile` format.
- Manifest blob-file metadata and snapshot-safe blob GC are still follow-up
  phases.
- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Start the next implementation slice by wiring flush to write the new
  `BlobFile` format and table `BlobIndex` records, then add recovery checks for
  manifest-referenced blob files.

## 2026-05-26: Phase 35 Blob Flush And Recovery Integration Completed

### Observation

- Persistent table writes now separate values at or above
  `blob_threshold_bytes` into the new `BlobFile` format and store
  `ValueRef::BlobIndex` in SSTables.
- Blob records keep internal-key metadata and reads verify that the decoded
  blob record matches the visible LSM record owner.
- Table properties and manifest entries now carry per-blob-file reference
  metadata: file id, referenced bytes, referenced record count, and key span.
- Persistent open validates manifest-referenced blob files by decoding the blob
  file, checking the path/header file id, and comparing table blob-reference
  ids, bytes, and key spans against blob file properties.
- `DbStats` now reports blob read count and bytes.

### Interpretation

- The durable flush/reopen path now uses the Titan-like value model instead of
  the older primitive blob shape.
- The remaining large-value lifecycle gap is snapshot-safe blob GC rewrite and
  publish/delete metadata, which should remain its own phase.

### Verification

- `cargo test persistent_flush_writes_blob_index_file_and_reopen_reads_large_values --all-features`
- `cargo test persistent_reopen_fails_on_corrupt_referenced_blob_file --all-features`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo fmt --all --check`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, and `README.md`

### Remaining Blockers

- Snapshot-safe blob GC rewrite is not implemented in this phase.
- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Implement blob GC rewrite with snapshot-gated old-file deletion and
  idempotent recovery metadata when the large-value lifecycle work continues.

## 2026-05-26: Phase 36 Snapshot-Safe Blob GC Completed

### Observation

- `DbOptions` now exposes persistent blob GC controls:
  `blob_gc_enabled`, `blob_gc_discardable_ratio`, and
  `blob_gc_min_file_bytes`.
- `DbStats` now reports blob GC runs, input bytes, output bytes, and discarded
  bytes.
- Compaction publishes blob-file pending deletions in the manifest instead of
  physically deleting obsolete blob files immediately.
- Blob GC rewrites still-live records from a partially stale blob file into a
  new blob file and publishes equivalent `BlobIndex` records for the same
  internal keys.
- Pending blob cleanup is gated by active snapshots/read pins and refuses to
  delete a pending file that is still referenced by a manifest-live table.
- Writable open can clean safe manifest-pending blob deletions; v4 manifests
  decode with an empty pending-deletion map.
- README, usage docs, durability notes, and protocol docs now describe the
  implemented blob GC behavior.

### Interpretation

- The first Titan-like large-value lifecycle is now correct enough for normal
  persistent use: large values separate during flush/compaction, reads validate
  blob records, stale blob files can be reclaimed, and old blob files stay
  available while old table handles can reach them.
- Throughput tuning for blob GC should be evidence-led. Correctness coverage is
  in place, but there is not yet a dedicated GC benchmark.

### Verification

- `cargo check --all-targets --all-features`
- `cargo test persistent_blob_gc_rewrites_live_records_from_partially_stale_file --all-features`
- `cargo test persistent_blob_gc_keeps_old_blob_while_read_pin_can_reach_it --all-features`
- `cargo test persistent_recovery_cleans_manifest_pending_blob_deletion --all-features`
- `cargo test persistent_recovery_does_not_delete_referenced_pending_blob_deletion --all-features`
- `cargo test manifest_decode_accepts_previous_version_without_pending_blob_deletions --all-features`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo fmt --all --check`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, and `README.md`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- Blob GC throughput has no dedicated benchmark baseline yet.
- Titan Level Merge and value-lazy iterators remain out of scope.

### Recommended Next Action

- Commit Phase 36, then start a dedicated benchmark phase for large-value write,
  read, range scan, and GC rewrite throughput before tuning policy or layout.

## 2026-05-26: Phase 37 Large-Value Direct Read Completed

### Observation

- The benchmark harness now reports dedicated large-value rows:
  `blob point read`, `blob range scan`, and `blob GC rewrite`.
- Pre-tuning `cargo bench --bench v1_bench` after adding those rows showed:
  - `blob point read`: 1140632 us for 256 reads.
  - `blob range scan`: 1128654 us for 32 scans.
  - `blob GC rewrite`: 148488 us.
- `BlobIndex` reads previously decoded the full blob file and searched by
  offset.
- `BlobIndex` reads now validate the blob header, seek to the indexed record
  frame, verify the record checksum, decode that one record, and compare the
  resulting index/internal key with the expected owner.
- Post-tuning `cargo bench --bench v1_bench` showed:
  - `blob point read`: 13976 us for 256 reads.
  - `blob range scan`: 13719 us for 32 scans.
  - `blob GC rewrite`: 153518 us.
- Full blob-file decoding remains in recovery validation and GC scanning.

### Interpretation

- The largest measured large-value read-path cost was fixed without changing
  public API or visible MVCC behavior.
- Blob GC rewrite did not improve because this phase intentionally targeted
  value reads. That workload is now measured and can guide a future GC-specific
  phase if needed.

### Verification

- `cargo test blob::tests --all-features`
- `cargo bench --bench v1_bench` before and after tuning
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo fmt --all --check`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, and `README.md`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- Titan Level Merge, value-lazy iterators, and GC-specific throughput tuning
  remain future phases, not correctness gaps in this read-path phase.

### Recommended Next Action

- Commit Phase 37. If more performance work is desired, start a focused
  GC-throughput or value-lazy-iterator phase with fresh benchmark evidence.

## 2026-05-26: Phase 38 Blob Maintenance And Value-Lazy Iteration Completed

### Observation

- `BucketOptions::blob_level_merge_enabled` now defaults to false and is
  persisted in manifest v6.
- Manifest v5 bucket options decode with `blob_level_merge_enabled = false`.
- When Level Merge is enabled for a bucket, compaction reads retained
  `BlobIndex` values and writes fresh output blob records instead of carrying
  old blob indexes forward.
- `Db`, `Bucket`, and `Snapshot` now expose value-lazy range and prefix
  iterators.
- Value-lazy blob rows share the iterator read pin through `Arc<Snapshot>` and
  read blob bytes only when callers ask for the value.
- Blob GC candidate selection now reads blob footer/properties metadata without
  decoding every record payload.
- Blob GC rewrite copies live records by exact `BlobIndex` and internal key
  instead of building a full offset map from a decoded blob file.
- A table-driven recovery fault matrix now covers temporary manifest publish,
  missing table, missing blob, corrupt blob, and unreferenced formal blob
  failures.
- Benchmark harness rows now include `blob range lazy keys` and
  `blob level merge`.

### Interpretation

- The large-value subsystem now has a usable first pass for all requested
  post-GC pieces: optional Level Merge, value-lazy iteration, a narrower GC
  read path, and broader recovery fault coverage.
- Automatic Level Merge policy and multi-candidate GC batching remain policy
  work, not correctness gaps in this phase.

### Verification

- `cargo test blob::tests --all-features`
- `cargo test --test persistent_wal --all-features`
- `cargo test --test in_memory_iteration --all-features`
- `cargo test manifest_decode_v5_bucket_options_default_blob_level_merge --all-features`
- `cargo bench --bench v1_bench`
  - `blob range scan`: 17705 us for 32 scans.
  - `blob range lazy keys`: 174 us for 32 scans.
  - `blob GC rewrite`: 154265 us.
  - `blob level merge`: 143299 us.
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo fmt --all --check`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, and `README.md`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- Level Merge is explicit per bucket. Automatic policy should wait for
  workload evidence.
- Blob GC rewrites one selected candidate per maintenance pass.

### Recommended Next Action

- Commit Phase 38, then push to CI. Future large-value work should use the new
  benchmark rows to tune Level Merge policy and GC candidate batching.

## 2026-05-26: Phase 39 Automatic Blob Maintenance Policy Completed

### Observation

- Rust skill and SPEC-AGENTS current context were read before implementation.
- `BucketOptions` now stores `blob_level_merge_policy` instead of requiring
  users to reason about a Level Merge boolean. The default is
  `BlobLevelMergePolicy::Auto`.
- `with_blob_level_merge_enabled` remains as a compatibility builder and maps
  `true` to `Always`, `false` to `Disabled`.
- Manifest v7 writes the policy enum. v6 boolean manifests decode `true` as
  `Always` and `false` as `Auto`; v5 and older bucket options default to
  `Auto`.
- Auto Level Merge rewrites retained blob references when compaction output
  would keep references to multiple blob files, or when compaction drops part
  of an input blob file's references.
- `Disabled` prevents Level Merge rewrites and lets GC-only tests exercise
  stale blob cleanup directly.
- Blob GC candidate selection now returns every file that passes the configured
  discard threshold, and the rewrite plan handles all selected candidates in
  one publish.
- Benchmark notes distinguish the GC row from the Level Merge row by disabling
  Level Merge in the GC rewrite benchmark.

### Interpretation

- The large-value maintenance path no longer depends on users enabling an
  internal-looking knob to get sane compaction behavior.
- GC batching removes the prior one-candidate maintenance bottleneck while
  keeping the first pass simple: all selected live records are copied into one
  output blob file and old files remain snapshot-gated.
- A future workload-tuning phase can add GC batch byte limits if real
  deployments show long stalls.

### Verification

- `cargo test --test persistent_wal --all-features`
- `cargo test manifest_decode --all-features`
- `cargo test blob::tests --all-features`
- `cargo bench --bench v1_bench`
  - `blob range scan`: 12929 us for 32 scans.
  - `blob range lazy keys`: 173 us for 32 scans.
  - `blob GC rewrite`: 126649 us.
  - `blob level merge`: 123261 us.
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features`
- `cargo fmt --all --check`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, and `README.md`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- GC batches currently write one output blob file. Add a byte cap only with
  benchmark evidence.

### Recommended Next Action

- Commit Phase 39, then push to CI. After CI, use larger workload benchmarks to
  decide whether auto Level Merge and GC batching need additional policy
  thresholds.

## 2026-05-26: Phase 40 Table Read-Path Index Hardening Completed

### Observation

- Rust skill, SPEC-AGENTS current context, and the coding module were read
  before implementation.
- Data blocks now encode a checked user-key hash index. Point reads use the
  hash index to find candidate record ranges and compare the real key only for
  hash collision handling.
- `IndexSearchPolicy` now exposes only `Linear`, `Binary`, and `Auto`.
  Manifest tags for the retired policies decode to `Auto`.
- The benchmark harness no longer reports retired search-policy rows.
- Persistent table open now reads footer, properties, and the small top-level
  index. Partition index/filter blocks are read and cached on demand.
- Per-block point and prefix filters live in partition index blocks, so misses
  can skip the covered data block after only the relevant partition metadata is
  loaded.
- The top-level partition index is stored uncompressed so absolute partition
  offsets have a stable block length. Partition blocks still use the table
  codec.
- `cargo bench --bench v1_bench` after this phase reported:
  - `random get`: 1488 us for 2048 reads.
  - `missing get`: 665 us for 2048 reads.
  - `prefix scan table partitions nonmatching`: 82 us for 128 scans.
  - `block cache warm read`: 2856 us for 2048 reads.
  - `cold table read`: 174357 us for 32 reads.
  - `index seek policy auto large`: 20048 us for 2048 seeks.

### Interpretation

- The table read path now avoids both fake public search-policy switches and
  the previous all-index/all-filter persistent open behavior.
- Large tables still need a top-level index in memory, but detailed block
  filters and block handles are now behind lazy partition loads.
- Full table filters are no longer kept on persistent table handles; range
  tombstone checks still use key bounds and loaded tombstone metadata for
  correctness.

### Verification

- `cargo test randomized_operations_match_mvcc_reference_across_reopen --test model_reference --all-features`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --all --check`
- `cargo bench --bench v1_bench`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Commit Phase 40, then push to CI for the external release signal.

## 2026-05-26: Phase 40 Point-Read Hot Path Regression Fix

### Observation

- User audit found that point reads regressed after lazy partitioned
  index/filter loading: point seek entered a partition at its first block,
  metadata access cloned per-block filters, normal block decode rebuilt the
  hash index for validation, and persistent table handles no longer pinned any
  table-level point filter.
- The fix makes point seek binary-search inside the selected index partition,
  uses borrowed block metadata for point/range/prefix scans, pins table-level
  filters for L0 tables, and leaves full hash-index rebuild validation to the
  explicit test/verify decode path.
- `DbStats.read_path` now exposes aggregate point-read probes for tables,
  index partitions, block metadata, data-block reads, and point filter misses.

### Verification

- `cargo test --lib table::tests::`
- `cargo test --lib lsm::version::tests::range_tombstone_lookup_uses_key_bounds_without_table_filter`
- `cargo fmt --all --check`

### Remaining Blockers

- `cargo test --all-targets --all-features` currently fails in
  `tests/persistent_wal.rs` around background maintenance/default-worker
  behavior and directory cleanup. These failures reproduce outside the table
  hot-path slice and line up with the dirty Phase 41 maintenance changes in
  `src/db.rs` and `src/options.rs`.
- `cargo clippy --lib --all-features -- -D warnings` currently fails on the
  same dirty maintenance slice (`src/db.rs` unnested or-pattern and
  needless-pass-by-value).

## 2026-05-26: Phase 41 Background Maintenance Scheduling And Backpressure Completed

### Observation

- Rust skill, concurrency skill, SPEC-AGENTS current context, and the coding
  module were read before implementation.
- Persistent writable databases now start one maintenance worker by default.
  `background_worker_count == 0` keeps manual maintenance behavior, and
  in-memory/read-only opens do not start workers.
- Maintenance coordination now separates flush and compaction requests, tracks
  in-flight flushes and compaction key ranges, publishes progress notifications,
  records shutdown, and preserves the last background error until a foreground
  caller observes it.
- Writes apply pressure handling before taking the writer coordinator when
  immutable memtables or L0 tables exceed configured limits. They can wait for a
  background worker or help with one foreground maintenance pass.
- Flush table writing and compaction output construction now happen outside the
  writer coordinator. The writer lock is used for sequence/WAL/memtable commit,
  freeze cutovers, manifest publish, state install, and WAL replay-floor
  rewrite.
- Automatic L0 compaction can choose a local seed span and leave unrelated L0
  files for later passes. Explicit `compact_range` keeps requested-range
  behavior.
- Compaction reservations reject overlapping key ranges in the same bucket and
  allow non-overlapping ranges or different buckets.
- Protocol, usage docs, durability docs, README, roadmap, and current phase
  notes were updated.

### Interpretation

- The background maintenance path is now the normal persistent path instead of a
  disabled-by-default feature.
- The writer coordinator no longer owns heavy table build and compaction merge
  work, reducing write-path stalls under flush or compaction pressure.
- Local L0 compaction reduces avoidable rewrite work, but it may leave more
  tables behind than the previous broad compaction plan. Stats and tests now
  assert pressure control rather than forcing every pass to collapse the level
  into one table.
- The Phase 40 test/clippy blockers caused by dirty maintenance changes are
  resolved.

### Verification

- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --all --check`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.

### Recommended Next Action

- Commit Phase 41, then use remote CI as the external release signal.

## 2026-05-27: Phase 42 Persistent Read-Path Resource Policy Completed

### Observation

- Rust skill, performance skill, concurrency skill, SPEC-AGENTS current
  context, and coding guidelines were read before implementation.
- Baseline release-profile `cargo bench --bench v1_bench` reported `random get`
  at 907 us, `missing get` at 457 us, `block cache warm read` at 1539 us,
  `cold table read` at 176052 us, and `index seek policy auto large` at
  3739 us.
- Persistent tables previously held an `Arc<File>` but cloned the handle before
  every block seek/read. They now hold a shared table file handle and read blocks
  through a table-local locked seek/read path.
- L0/L1 table writes now include table filters. Persistent open pins table
  filters and index partitions for L0/L1 tables, while deeper levels keep
  partition metadata lazy.
- Deeper-level lazy index partitions use the global block cache when the read
  path supplies it.
- Block cache entries now have high/low priority queues. Index, filter, and
  range-tombstone metadata are high priority; data and blob blocks are low
  priority.
- The benchmark harness now reports `long shared-prefix get`.
- Post-change release-profile `cargo bench --bench v1_bench` reported `random
  get` at 1032 us, `missing get` at 497 us, `block cache warm read` at 1764 us,
  `cold table read` at 154176 us, `index seek policy auto large` at 3341 us,
  and `long shared-prefix get` at 2867 us.

### Interpretation

- The file-handle path now avoids per-block descriptor clone/open work and keeps
  block reads safe behind one table-local cursor lock.
- L0/L1 metadata pinning moves the hottest table/index/filter checks out of the
  lazy-load path, while deeper levels stay bounded through global cache policy.
- High-priority cache eviction gives metadata a protected path under data-block
  churn without adding public tuning knobs.
- Shared-prefix keys are measurably slower than the normal benchmark key shape,
  but a tighter data-block key encoding would require an SSTable record-format
  change. That belongs in a dedicated storage-format phase instead of this cache
  resource slice.

### Verification

- `cargo test --lib cache::tests --all-features`
- `cargo test --lib table::tests --all-features`
- `cargo test --test persistent_wal persistent_block_cache_records_hits_and_misses --all-features`
- `cargo test --test persistent_wal persistent_prefix_filter_stats_skip_nonmatching_tables --all-features`
- `cargo bench --bench v1_bench`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --all --check`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- Remote CI cannot be executed locally; it must run after push.
- Shared-prefix key encoding has benchmark evidence but remains a storage-format
  follow-up, not a cache-policy patch.

### Recommended Next Action

- Commit Phase 42, then use remote CI as the external release signal. If
  shared-prefix workloads remain important, start a focused data-block key
  encoding phase with explicit compatibility tests.

## 2026-05-27: Public Maintenance Barrier Semantics

### Observation

- `Db::flush()` reused the same non-blocking guard helper as background
  maintenance. If another flush already held the guard, public `flush()` could
  return success without publishing the caller's already committed data out of
  memtables.
- `compact_range()` had the same shape for compaction reservation conflicts:
  a busy guard could look like a successful no-op to the public API.

### Interpretation

- Background workers and write-pressure paths should keep best-effort helpers,
  but foreground APIs need barrier behavior.
- The flush barrier needs a captured sequence boundary so concurrent writers do
  not make `flush()` chase new writes indefinitely.
- Public compaction should wait/retry on active overlapping reservations while
  preserving the existing one-pass/no-plan return behavior.

### Verification

- `cargo test --lib flush_waits_for_existing_flush_guard --all-features`
- `cargo test --lib flush_returns_after_default_background_flush_publishes_tables --all-features`
- `cargo test --lib compact_range_is_not_silent_best_effort --all-features`
- `cargo test --test model_reference randomized_operations_match_mvcc_reference_across_reopen --all-features`
- `cargo test --test persistent_wal persistent_flush_auto_compacts_when_l0_pressure_exceeds_limit --all-features`
- `cargo test --test persistent_wal persistent_stats_report_tables_blobs_and_compactions --all-features`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --all --check`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- Remote CI still has to run after push.

### Recommended Next Action

- Commit Phase 43, then use remote CI as the external release signal.

## 2026-05-27: Point-Read Source Diagnosis

### Observation

- Stable threaded reads with background workers disabled still showed a
  4-thread to 32-thread plateau after smaller hot-path fixes.
- `BucketReader` now has a value-handle point-read path, so inline SSTable values
  can be read from decoded block bytes without allocating an owned `Vec<u8>`
  for the benchmark result.
- The Azoth benchmark adapter now uses the reader value-handle path.
- Benchmark after the value-handle path: random reads were about 3989ms,
  3416ms, 4203ms, and 4191ms for 4, 8, 16, and 32 threads.
- An experiment that pinned decoded L0 data blocks with per-block `OnceLock`
  removed global block-cache hits and improved 4/8-thread reads to about
  3081ms/3083ms, but regressed 16/32-thread reads to about 5473ms/5387ms.
  That experiment was rejected and not kept.

### Interpretation

- Returning owned values was a real API problem, but it is not the dominant
  cause of the 16/32-thread plateau.
- Unbounded table-local decoded block pinning is not an acceptable replacement
  for the global cache.
- The next source-level blocker is the shared block-cache hit path plus L0
  read amplification under CPU oversubscription.

### Verification

- `cargo check --manifest-path /Users/poria/Developer/Work/Azoth/trine-kv/Cargo.toml`
- `cargo check --manifest-path benchmark-crates/azoth-kv-bench/Cargo.toml --benches`
- `cargo clippy --manifest-path /Users/poria/Developer/Work/Azoth/trine-kv/Cargo.toml --all-targets -- -D warnings`
- `cargo test --manifest-path /Users/poria/Developer/Work/Azoth/trine-kv/Cargo.toml`
- `env BENCH_DIAGNOSTICS=1 TRINE_BENCH_BACKGROUND_WORKERS=0 BENCH_ONLY=threaded-reads cargo bench --manifest-path benchmark-crates/azoth-kv-bench/Cargo.toml --bench trine_benchmark`

### Remaining Blockers

- Need a bounded block-cache design with cheaper concurrent hit reads.
- Need a read-amplification decision for L0-heavy states: compact before
  read-heavy workloads, or make overlapping L0 lookup cheaper.

### Recommended Next Action

- Design the block cache replacement as a bounded shared read structure before
  implementation. Avoid more edge tuning until the cache hit path has a clear
  concurrency model and benchmark gate.

## 2026-05-27: Block-Cache Hit Path Contention

### Observation

- The kept block-cache change uses per-thread counter shards for cache hits and
  misses, with each counter shard aligned to avoid false sharing.
- Cache hit recency remains best effort: readers update queue order only when
  the shard write lock is immediately available.
- The block-cache shard count was calibrated with the Azoth threaded-read
  workload. `128` shards gave the best balanced result after the counter fix.
- Final kept benchmark with background workers disabled:
  3694ms, 3348ms, 4139ms, and 4169ms for 4, 8, 16, and 32 threads.
- Previous value-handle baseline was about 3989ms, 3416ms, 4203ms, and 4191ms.
- A second-chance/CLOCK-style recent bit improved 4/8 threads but regressed
  16/32 threads, so it was rejected.
- Larger cache shard counts helped 4/8 threads but regressed 16/32 threads:
  `256` measured about 3524ms, 3212ms, 4288ms, and 4202ms; `512` was worse at
  high thread counts.

### Interpretation

- One global cache hit counter was real shared-state contention in point reads.
- Updating a per-entry recent bit on every hit is still too expensive for this
  workload; the non-blocking queue update is the better current shape.
- Block-cache sharing is improved but not the remaining dominant source.
- Diagnostics still show two L0 tables and about 10.15M table probes for about
  5.15M data-block reads, so overlapping L0 lookup is now the next source
  problem.

### Verification

- `cargo fmt --check`
- `cargo test cache`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`
- `cargo check --manifest-path benchmark-crates/azoth-kv-bench/Cargo.toml --benches`
- `env BENCH_DIAGNOSTICS=1 TRINE_BENCH_BACKGROUND_WORKERS=0 BENCH_ONLY=threaded-reads cargo bench --manifest-path benchmark-crates/azoth-kv-bench/Cargo.toml --bench trine_benchmark`

### Remaining Blockers

- L0 overlap still doubles point-read table probes in the measured state. This
  is addressed by the following evidence entry.

### Recommended Next Action

- Keep the cache counter/shard fix.
- Next, solve the L0 overlap source problem without adding unbounded decoded
  block pinning or changing MVCC/recovery semantics.

## 2026-05-27: Overlapping L0 Read Amplification

### Observation

- The benchmark state had two overlapping L0 tables. The newer L0 table missed
  most random point reads by table filter, then the older table served the hit.
- Before the L0 overlap fix, diagnostics showed about 10.15M table probes for
  about 5.15M data-block reads.
- L0 pressure now triggers at foreground `flush()` when background workers are
  disabled and L0 table key spans overlap, even if the raw L0 file count is
  below `max_l0_files`.
- Disjoint L0 tables below the file-count limit are not treated as overlap
  pressure.
- Final threaded-read benchmark with background workers disabled:
  3494ms, 2416ms, 1996ms, and 2019ms for 4, 8, 16, and 32 threads.
- Final diagnostics show `l0=0`, `levels=L1:16`, and table probes matching
  data-block reads instead of doubling them.

### Interpretation

- The main source of the read explosion was overlapping L0 read amplification,
  not returned-value allocation or decoded-block ownership alone.
- Earlier local compaction of overlapping L0 files is the right source fix for
  this measured state.
- This preserves MVCC, WAL, recovery, and table format semantics; it only
  changes when maintenance is requested.

### Verification

- `cargo test l0_overlap_pressure_uses_key_bounds`
- `cargo test persistent_flush_auto_compacts_overlapping_l0_below_file_limit`
- `cargo fmt --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`
- `env BENCH_DIAGNOSTICS=1 TRINE_BENCH_BACKGROUND_WORKERS=0 BENCH_ONLY=threaded-reads cargo bench --manifest-path benchmark-crates/azoth-kv-bench/Cargo.toml --bench trine_benchmark`

### Remaining Blockers

- The broad benchmark suite should be run to check the write/read tradeoff.

### Recommended Next Action

- Keep overlapping-L0 compaction pressure.
- Run the broad benchmark suite before making broader tuning decisions.

## 2026-05-27: Point-Reader Source Snapshot Review

### Observation

- Pre-commit review found that pinning only an `LsmVersion` in `BucketReader`
  was not enough. If a reader was created while a committed value lived in the
  active memtable, a later flush could remove that memtable source and publish
  the value into a newer table version not held by the reader.

### Interpretation

- A repeated point-read handle must keep the memtable sources and table version
  together. It is acceptable for the snapshot to see a record twice during a
  flush race, because MVCC ordering chooses the newest visible candidate, but it
  must never miss a committed source.

### Verification

- `cargo test --test persistent_wal persistent_bucket_reader_keeps_memtable_source_after_flush --all-features`
- `cargo test --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --all --check`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- Remote CI still has to run after push.

### Recommended Next Action

- Commit the point-read source fix and use remote CI as the external release
  signal.

## 2026-05-27: Public API Copy Review

### Observation

- README and `quickstart` still showed `bucket_with_options` in the common
  named-bucket path, even though `db.bucket("users")?` is the intended default.
- `bucket_with_options` remains useful for prefix filters and storage tuning,
  so it belongs in the custom bucket-options section rather than the first API
  example.
- Public docs for `BucketReader` and `PointValue` needed to explain the new
  repeated point-read path without promising owned-value allocation avoidance
  in cases where blob or owned values are returned.

### Interpretation

- The first public path should minimize caller setup: direct `Db` operations
  for the default bucket, then `db.bucket(name)` for named buckets.
- Advanced bucket configuration should stay documented, but not as the normal
  quickstart path.

### Verification

- `cargo run --example quickstart`
- `cargo fmt --all --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo test --doc`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- None for this public-copy pass.

### Recommended Next Action

- Commit the public-copy polish with the point-read documentation alignment.

## 2026-05-27: Reader Get API Shape

### Observation

- User feedback showed that `get_value` made the hot point-read path feel like
  a special long-form API, while ordinary point reads should remain `db.get`
  and `bucket.get`.
- `BucketReader` is already an explicit repeated-read handle, so its shortest
  `get` method can return `PointValue` without complicating the common `Db`
  and `Bucket` APIs.

### Interpretation

- Keep `Db::get` and `Bucket::get` as the one-call owned-value path.
- Make `BucketReader::get` return `PointValue`, and keep the owned conversion
  explicit through `BucketReader::get_owned`.
- Usage docs should introduce `BucketReader` only under repeated point reads,
  not as the normal way to read one key.

### Verification

- `cargo fmt --all --check`
- `cargo test --test persistent_wal persistent_bucket_reader_keeps_memtable_source_after_flush --all-features`
- `cargo run --example quickstart`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo test --doc`
- `git diff --check`
- forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`
- public `get_value` scan over `src`, `tests`, `benches`, `examples`, `docs`,
  and `README.md`

### Remaining Blockers

- None for this API/docs rename.

### Recommended Next Action

- Commit the API/docs rename with the public-copy polish.

## 2026-05-27: Lock-Free Foreground Write Path Spec

### Observation

- User chose the stronger production-grade direction: Trine should pursue
  foreground point reads and blind writes without a global write lock, while
  keeping background I/O and maintenance as explicit owner workers.
- A fully lock-free database is not the correct target because WAL file writes,
  fsync, manifest publish, segment rollover, compaction scheduling, and cleanup
  have real ordering and I/O boundaries.
- The useful target is zero avoidable implementation conflict: blind writes do
  not abort because of shared write-path structure, reads do not wait for
  writers, and transactions only fail for real isolation conflicts.

### Interpretation

- The next implementation work should be driven by
  `.phrase/protocol/lock-free-foreground-write-path.md`.
- The central correctness risks are visible-sequence gaps, multi-shard commit
  publication, WAL-shard recovery, and delta-chain read amplification.
- Writer-local construction is useful, but published writer-local chains must
  be routed into bucket/key shards so point reads do not scan every writer.

### Verification

- Read `.phrase/decision.md`, `.phrase/roadmap.md`, `.phrase/current.md`,
  `.phrase/protocol/trine-kv-v1-spec.md`, and ADR 0001.
- Read `.rust-skills/AGENTS.md`, `m07-concurrency`, `m10-performance`,
  `m04-zero-cost`, and `coding-guidelines`.
- Added `.phrase/protocol/lock-free-foreground-write-path.md`.
- Linked the new protocol from `.phrase/protocol/trine-kv-v1-spec.md`.
- Updated `.phrase/roadmap.md` and `.phrase/current.md`.
- `git diff --check`
- forbidden-term scan over changed `.phrase` files.

### Remaining Blockers

- No Rust implementation has started.
- Exact atomic ordering choices must be made and tested during implementation.
- WAL shard recovery must prove mixed durability and missing sequence behavior.

### Recommended Next Action

- Review and accept the protocol.
- Start implementation with the commit tracker and visible sequence behind the
  existing write coordinator before changing WAL sharding or delta publication.

## 2026-05-27: Async-First Portable Storage And WASM Spec

### Observation

- The v1 spec still described the public database API as synchronous, including
  `Db::open`, point reads, range creation, writes, flush, and compaction.
- That shape conflicts with a portable storage design because open, recovery,
  cache-miss reads, WAL append, manifest publish, and maintenance barriers can
  wait on platform storage.
- Future WASM support cannot rely on blocking filesystem calls, native threads,
  process locks, memory mapping, or local-file rename as the only publish
  mechanism.

### Interpretation

- The primary Trine API should be async-first, with native blocking calls as an
  adapter rather than the core contract.
- The engine core should depend on database-level storage operations and
  explicit backend capabilities instead of direct native file APIs.
- Durability options must map through backend capabilities and return typed
  unsupported errors rather than silently downgrading.
- WASM readiness requires memory-mode build coverage now, while persistent WASI
  and browser backends should be staged later behind the same storage contract.
- Async-first storage and the no-global-lock foreground write path should share
  the final design, but implementation must separate API/storage/cancellation
  migration from commit tracker, WAL sharding, and shard-delta publication.

### Verification

- Read `.phrase/decision.md`, `.phrase/roadmap.md`, `.phrase/current.md`, and
  `.phrase/protocol/trine-kv-v1-spec.md`.
- Added `.phrase/protocol/async-first-portable-storage-and-wasm.md`.
- Updated `.phrase/protocol/trine-kv-v1-spec.md` with async-first public API,
  portable storage, durability, cursor, error, test, benchmark, and acceptance
  language.
- Updated `.phrase/decision.md`, `.phrase/roadmap.md`, and
  `.phrase/current.md`.
- Added explicit implementation relationship rules to the async-first storage
  protocol, foreground write-path protocol, and v1 spec.
- `git diff --check`.
- Forbidden-term scan over changed spec/current/evidence files.

### Remaining Blockers

- No Rust implementation has started.
- Async runtime boundary and trait signatures must be designed during the first
  implementation slice.
- WASI and browser persistent backends need backend-specific capability probes
  and failure fixtures.

### Recommended Next Action

- Review and accept the async-first portable storage protocol.
- Start with an async primary API compatibility slice before changing storage
  backend internals.

## 2026-05-31: Block Manager Extraction Before Async Storage

### Observation

- User clarified that Block Manager should own block content lifecycle: write,
  read, compression, checksum verification, and cache coordination.
- The existing implementation already had those responsibilities, but checked
  block encoding/decoding and checked block reads lived inside `table.rs`.
- `table.rs` also owned data/index/filter/tombstone table semantics, making
  direct async storage migration likely to spread storage waiting boundaries
  through table-format code.

### Interpretation

- Block Manager is a better first implementation slice before async storage
  because later backend reads can enter through a single checked-block content
  layer.
- Extent allocation remains out of scope because Trine's current persistent
  model is immutable SSTable/blob objects plus manifest-controlled lifetime.
- The extraction should not change public API, SSTable format, cache semantics,
  MVCC, manifest, compaction, transaction, or blob GC behavior.

### Verification

- Read `.phrase/decision.md`, `.phrase/roadmap.md`, `.phrase/current.md`, and
  Rust skills for routing, coding guidelines, and safe refactoring.
- Added `src/block.rs` with `BlockHandle`, `BlockManager`, checked block
  append/encode/decode, checksum, bounds checks, and source-backed checked block
  reads.
- Updated `src/table.rs` to call `BlockManager` for checked block lifecycle
  while keeping table section layout and table-level semantics in `table.rs`.
- Moved codec tag conversion onto `CodecId` so table and block code share the
  same table-format codec ids.
- `cargo test table --lib`
- `cargo test block --all-targets`
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `git diff --check`
- Forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- None for the Block Manager extraction.

### Recommended Next Action

- Start async storage migration by replacing the checked-block read source
  behind `BlockManager` before changing the public async API shape.

## 2026-05-31: Block Read Source Boundary

### Observation

- After Block Manager extraction, source-backed checked block reads still
  accepted ad hoc closures from `table.rs`.
- The closures preserved behavior, but they did not give later storage backend
  work a named boundary to replace.
- The current persistent table path already has a concrete read source: table
  path plus optional cached table file handle.

### Interpretation

- A small synchronous `BlockReadSource` boundary is the right next slice before
  introducing async storage traits.
- The boundary should sit below checked block validation and above native-file
  reads, so later backend work can replace byte reads without touching checksum,
  codec, or table block semantics.
- Public async API and async trait design remain later work.

### Verification

- Added `BlockReadSource` in `src/block.rs`.
- Updated `BlockManager` source-backed checked block reads to call
  `read_exact_at` through `BlockReadSource` instead of accepting closures.
- Added `TableBlockReadSource` in `src/table.rs` over the existing table path
  and optional cached `TableFile` handle.
- `cargo test table --lib`
- `cargo test block --all-targets`
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `git diff --check`

### Remaining Blockers

- None for this boundary slice.

### Recommended Next Action

- Introduce database-level storage object identifiers and a native-file backend
  adapter behind table reads, still without changing public async API shape.

## 2026-05-31: Native-File Storage Read Adapter

### Observation

- `BlockReadSource` gave `BlockManager` a named byte-read dependency, but
  `table.rs` still directly described the native-file source shape.
- Persistent table block reads have a concrete database object identity: a
  table object at a native path, optionally backed by a cached file handle.
- Other storage objects are not yet routed through this boundary, so adding all
  object kinds now would be premature.

### Interpretation

- The next useful storage slice is a native-file read adapter keyed by a table
  storage object id.
- This keeps checked-block validation independent from native files while
  preserving current synchronous behavior.
- Async storage traits and public async API migration should remain separate
  later phases.

### Verification

- Added `src/storage.rs` with `StorageObjectKind`, `StorageObjectId`,
  `NativeFileReadHandle`, and `NativeFileReadSource`.
- Routed persistent table checked-block reads through `NativeFileReadSource`.
- Kept cached table file handle reuse by implementing `NativeFileReadHandle`
  for `TableFile`.
- Scoped `StorageObjectKind` to `Table` in this slice because no other object
  kind is routed through the adapter yet.
- `cargo test table --lib`
- `cargo test block --all-targets`
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `git diff --check`
- Forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- None for the native-file table read adapter.

### Recommended Next Action

- Extend the storage adapter boundary to table open/metadata reads, or define
  the first async storage trait shape while keeping public async API migration
  as a separate phase.

## 2026-05-31: Table Open Storage Boundary

### Observation

- After persistent table checked-block reads moved behind
  `NativeFileReadSource`, `read_table` still directly opened native files,
  read the table header, checked file length, sought to the footer, and loaded
  startup metadata from a local `File`.
- Those direct reads were the main remaining native-file dependency in the
  persistent table read path.

### Interpretation

- Table opening should use the same storage object id and native-file adapter
  as later block reads, otherwise future backend migration would still need to
  split table startup reads from lazy block reads.
- It is still too early to move table writes, manifest publish, WAL append,
  blob reads, cleanup, or public async APIs in the same slice.

### Verification

- Added `NativeFileObject` in `src/storage.rs` to own an opened storage object,
  report object length, and serve locked random reads.
- Updated `NativeFileReadHandle` so cached opened objects serve reads through
  their own storage object state while fallback reads still use
  `StorageObjectId`.
- Updated `read_table` so open, header, file length, footer, properties,
  top-level index, pinned filters, and pinned index metadata all read through
  `NativeFileReadSource`.
- Updated lazy range tombstone, index partition, and data block reads to build
  the same storage read source instead of direct native-file helpers.
- Replaced the table-local cached file wrapper with `NativeFileObject`.
- `cargo test table --lib`
- `cargo test block --all-targets`
- `cargo test persistent --all-targets`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --check`
- `git diff --check`
- Forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- None for the persistent table read boundary.
- Table writes, manifest, WAL, blob files, cleanup, async traits, and public
  async API migration remain later slices.

### Recommended Next Action

- Define the first async storage trait shape next, keeping public async API
  migration and table-write routing as separate phases.

## 2026-05-31: Async Storage Read Trait Shape

### Observation

- The persistent table read path now uses a storage object and native-file read
  adapter, but the adapter was still only a synchronous byte-read boundary.
- The async-first protocol requires storage operations to be database-level
  async operations, while CPU-only table and block validation should stay
  synchronous.
- Public async API migration, table writes, manifest publish, WAL, blob files,
  cleanup, lease handling, and runtime selection are larger slices than the
  first read trait shape.

### Interpretation

- The smallest useful next step is an internal async read trait shape for
  storage object open, object length, and random reads.
- Native-file storage can implement that shape with immediately-completing
  futures and expose a blocking compatibility adapter for the current
  synchronous table path.
- Write, manifest, durability, lease, cleanup, and runtime capabilities should
  remain separate so this slice does not silently change storage semantics.

### Verification

- Added `StorageReadBackend`, `StorageReadObject`, blocking compatibility
  traits, `StorageReadCapabilities`, and `NativeFileBackend` in
  `src/storage.rs`.
- Implemented native-file async read futures without adding an async runtime
  dependency.
- Made native-file blocking compatibility poll only immediately-ready futures;
  a pending future reports unsupported runtime behavior instead of blocking on
  an unknown executor.
- Routed table read opening through `NativeFileBackend`'s blocking adapter while
  preserving existing table read behavior through `NativeFileReadSource`.
- Added a focused storage test for async open, object length, and random read
  shape.
- `cargo test storage --lib`
- `cargo test table --lib`
- `cargo test block --all-targets`
- `cargo test persistent --all-targets`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --check`
- `git diff --check`
- Forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- None for the async storage read trait shape.
- Public async API, memory backend routing, table writes, manifest publish, WAL,
  blob files, cleanup, durability capability errors, writer lease handling, and
  runtime selection remain later slices.

### Recommended Next Action

- Add explicit capability/error types for write/publish operations, or route
  memory mode through the same async read contract.

## 2026-05-31: Storage Capability And Error Types

### Observation

- The async storage read trait shape existed, but backend guarantees were still
  represented by a narrow read-only capability struct.
- The async-first protocol requires honest capability reporting before storage
  operations such as writes, manifest publish, strict durability, writer lease,
  background work, and runtime support are routed through a backend.
- The public error enum did not yet have explicit `UnsupportedBackend` or
  `UnsupportedDurability` variants, even though the protocol names those
  failure modes.

### Interpretation

- Adding the capability vocabulary and typed unsupported errors is the right
  next slice before routing write or manifest operations, because unsupported
  guarantees must fail explicitly instead of becoming ad hoc option errors.
- The current native-file read backend should only claim persistent random read
  in this slice; write, publish, durability, lease, and cleanup capabilities
  should be named but not claimed until those operations are routed.

### Verification

- Added `StorageCapability` and `StorageCapabilities` in `src/storage.rs`.
- Added capability helpers for backend features and durability modes.
- Added `UnsupportedBackend` and `UnsupportedDurability` variants in
  `src/error.rs`.
- Added `DurabilityMode::as_str` for error display.
- Routed the table read path's random-read requirement through
  `StorageCapabilities::require`.
- Added focused storage tests for unsupported backend capability and unsupported
  durability errors.
- `cargo test storage --lib`
- `cargo test table --lib`
- `cargo test block --all-targets`
- `cargo test persistent --all-targets`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --check`
- `git diff --check`
- Forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- None for the capability/error type slice.
- Public async API, memory backend routing, table writes, manifest publish, WAL,
  blob files, cleanup, writer lease handling, and runtime selection remain
  later slices.

### Recommended Next Action

- Route memory mode through the same async read contract, or add write/publish
  trait methods behind the capability checks.

## 2026-05-31: Memory Storage Read Backend

### Observation

- Native-file storage already implemented the async read trait shape, but
  memory byte objects did not.
- The async-first storage protocol names the memory backend as volatile,
  immediately completable, and the baseline for WASM logical correctness tests.
- Existing production in-memory DB behavior does not currently create SSTable
  storage objects, so this slice should prove the read contract without
  changing flush, compaction, WAL, manifest, blob, or public API behavior.

### Interpretation

- A volatile memory read backend is the smallest useful follow-up after typed
  capabilities: it proves the same storage read boundary supports non-file byte
  objects.
- Table decode tests are the right first consumer because they can read encoded
  table bytes through the same checked-block source path without changing
  production in-memory mode.

### Verification

- Added `MemoryStorageBackend` and `MemoryStorageObject` in `src/storage.rs`.
- Memory storage reports volatile random-read capability and rejects persistent
  capability checks.
- Added `StorageReadSource` for already-opened storage read objects.
- Persistent `read_table` uses `StorageReadSource` for the opened native-file
  object.
- Test table decoding now opens encoded table bytes through
  `MemoryStorageBackend` and reads header, footer, properties, indexes, data
  blocks, range tombstones, and filters through the storage read source.
- `cargo test storage --lib`
- `cargo test table --lib`
- `cargo test block --all-targets`
- `cargo test persistent --all-targets`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --check`
- `git diff --check`
- Forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, `README.md`, `CHANGELOG.md`, and `Cargo.toml`

### Remaining Blockers

- None for the memory read backend slice.
- Public async API, production in-memory table-object routing, write/publish
  routing, WAL, manifest, blob files, cleanup, writer lease handling, and
  runtime selection remain later slices.

### Recommended Next Action

- Add write/publish trait methods behind capability checks, starting with a
  small native-file table write or manifest publish adapter slice.
