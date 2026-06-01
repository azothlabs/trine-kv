# Roadmap

## Goal

Keep long-term direction visible at phase granularity while letting short-term
implementation follow evidence.

## Planning Rule

Roadmap entries describe phase direction, entry conditions, acceptance gates,
and major out-of-scope boundaries. Detailed implementation tasks belong only in
`.phrase/current.md`.

## Phases

### Phase 1: Freeze V1 Database Spec

**Status**: Accepted

**Goal**: Define Trine KV v1 as a complete embedded LSM MVCC database before
implementation.

**Entry Condition**: `.phrase/decision.md` exists.

**Acceptance Gate**:

- ADR records the LSM MVCC engine decision.
- Protocol spec covers API, MVCC, WAL, SSTable, manifest, compaction, recovery,
  transactions, in-memory mode, tests, and benchmarks.
- User accepts the spec as the implementation source of truth.

### Phase 2: Scaffold Rust Crate

**Status**: Complete

**Goal**: Create the Rust crate and module skeleton that matches the accepted
spec.

**Entry Condition**: Phase 1 accepted.

**Acceptance Gate**:

- `Cargo.toml` follows local Rust guidance.
- Module skeleton matches the spec boundaries.
- `cargo fmt --check`, `cargo clippy`, and empty scaffold tests pass.

### Phase 3: Build V1 Engine By Spec

**Status**: Complete

**Goal**: Implement the complete v1 engine in slices without changing the
accepted contracts silently.

**Entry Condition**: Phase 2 complete.

**Acceptance Gate**:

- The v1 acceptance gate in `.phrase/protocol/trine-kv-v1-spec.md` passes.

### Phase 4: Write Usage Documentation

**Status**: Complete

**Goal**: Give users a runnable path from opening a database to using the core
v1 API safely.

**Entry Condition**: Phase 3 complete.

**Acceptance Gate**:

- README explains what Trine KV is, how to run verification, and where to start.
- Usage docs cover in-memory and persistent open, buckets, reads/writes,
  batches, snapshots, transactions, range/prefix scans, durability, maintenance,
  stats, and recovery boundaries.
- At least one example program compiles and runs with `cargo run --example`.

### Phase 5: Polish Public API

**Status**: Complete

**Goal**: Reduce first-use friction in the v1 public API without changing the
storage contract.

**Entry Condition**: Phase 4 complete.

**Acceptance Gate**:

- Common open and write-option paths need less caller-side struct boilerplate.
- Existing v1 tests and examples keep passing.
- Usage docs stay aligned with the polished API.

### Phase 6: Production Hardening

**Status**: Complete

**Goal**: Audit and harden operational behavior after API polish lands.

**Entry Condition**: Phase 5 complete.

**Acceptance Gate**:

- Operational failure-mode audit records concrete risks and verification.
- Hardening changes are backed by focused tests before the phase closes.

### Phase 7: Release Packaging

**Status**: Complete

**Goal**: Prepare the v1 crate for a clean first package using Semantic
Versioning.

**Entry Condition**: Phase 6 complete.

**Acceptance Gate**:

- Cargo package metadata is ready for a `0.1.0` SemVer release candidate.
- Package contents exclude local workflow files and include user-facing docs,
  examples, tests, benches, changelog, and license files.
- Release checklist documents versioning and verification.
- `cargo package --list`, `cargo package`, `cargo fmt --check`,
  `cargo clippy`, `cargo test`, `cargo run --example quickstart`, and
  `git diff --check` pass.

### Phase 8: Integration Examples

**Status**: Complete

**Goal**: Add runnable examples that show Trine KV embedded behind realistic
application boundaries.

**Entry Condition**: Phase 7 complete.

**Acceptance Gate**:

- Integration examples are runnable with `cargo run --example`.
- README or usage docs point users to the examples.
- Examples use public APIs without changing the v1 storage contract.

### Phase 9: CI And Publishing Workflow

**Status**: Complete

**Goal**: Automate release verification and provide a guarded manual crates.io
publishing workflow.

**Entry Condition**: Phase 8 complete.

**Acceptance Gate**:

- CI workflow runs formatting, clippy, tests, examples, package content guard,
  and package verification.
- Publishing workflow is manual, checks the requested SemVer version, runs the
  full verification gate, defaults to dry-run behavior, and publishes only when
  explicitly requested.
- Release docs explain the CI and publishing workflow.

### Phase 10: Targeted Pre-Publish Hardening

**Status**: Complete

**Goal**: Reduce one concrete publish-blocking durability risk before the first
crate publish.

**Entry Condition**: Phase 9 complete and user requests targeted hardening
before publishing.

**Acceptance Gate**:

- The selected risk is classified and fixed without changing public API or v1
  storage formats.
- Focused regression coverage exists for the hardening mechanism.
- The full local release gate still passes.

### Phase 11: Windows Directory Sync Hardening

**Status**: Complete

**Goal**: Extend parent-directory sync after atomic file publish to Windows
before crate publish.

**Entry Condition**: Phase 10 complete and user asks how non-Unix targets are
handled.

**Acceptance Gate**:

- Windows uses a directory handle path for parent-directory sync after rename.
- Unix behavior remains unchanged.
- Other targets are documented as best-effort.
- The full local release gate still passes.

### Phase 12: Benchmark-Backed Performance Tuning

**Status**: Complete

**Goal**: Improve one measured v1 benchmark hotspot before CI push without
changing public API or storage formats.

**Entry Condition**: Phase 11 complete and user requests benchmark/performance
tuning.

**Acceptance Gate**:

- Current benchmark baseline is recorded.
- A hotspot is selected from benchmark evidence before implementation.
- The tuning change has before/after benchmark evidence and keeps the full
  release gate passing.

### Phase 13: Rust 1.85 CI Compatibility Fix

**Status**: Complete

**Goal**: Restore CI compatibility with the declared Rust 1.85 MSRV without
raising the crate's minimum supported compiler.

**Entry Condition**: Remote CI reports Rust 1.85 rejecting crate code that
newer local toolchains accepted.

**Acceptance Gate**:

- Code no longer uses unstable-in-1.85 `Vec` methods inside `const fn`.
- Runtime public API behavior and storage formats remain unchanged.
- Local verification passes for formatting, clippy, tests, examples, package
  checks, and dry-run publishing.

### Phase 14: Lazy Range Iterator

**Status**: Complete

**Goal**: Replace eager range/prefix result building with a lazy seek cursor
that merges memtable and SSTable records under MVCC visibility.

**Entry Condition**: User review identifies eager range iteration as an
incorrect engine shape for v1.

**Acceptance Gate**:

- Range and prefix scans create a cursor instead of prebuilding all visible
  `KeyValue` rows.
- The cursor merges memtable and SSTable user-key groups lazily and applies
  MVCC point/range-delete rules per returned row.
- Existing scan, snapshot, range-delete, table, and persistent tests pass.
- A focused test proves table blocks are not touched until `Iterator::next`.

### Phase 15: Point Read Hot Path

**Status**: Complete

**Goal**: Remove avoidable contention and allocation from point reads without
changing public API or v1 storage formats.

**Entry Condition**: User benchmark review identifies repeated snapshot pinning,
global block-cache locking, vector-based point lookup, and full memtable scans
as point-read bottlenecks.

**Acceptance Gate**:

- Snapshot-backed point reads reuse the caller's existing snapshot pin.
- Point reads seek memtable/table records for the requested user key and choose
  the newest visible record without building and sorting a full record vector.
- Block-cache hit tracking no longer depends on one global exclusive lock.
- Existing MVCC, range-delete, persistent, transaction, and benchmark gates pass.

### Phase 16: LSM Write Path And WAL Lifecycle

**Status**: Complete

**Goal**: Make the write path match the v1 LSM shape by adding immutable
memtables, size-triggered active memtable freeze, pressure-triggered flush, and
bounded WAL replay after flush.

**Entry Condition**: User audit identifies active-only memtables, manual-only
flush, and unbounded WAL replay as the next P1 production risks.

**Acceptance Gate**:

- Active memtables freeze into immutable memtables when the configured write
  buffer threshold is reached.
- Reads, transactions, range scans, and prefix scans include immutable
  memtables before SSTables.
- Flush consumes immutable memtables and manual flush first freezes current
  active memtables.
- Immutable memtable pressure is handled before accepting the next write, so
  storage errors do not leave a new write half-reported.
- Manifest WAL replay floor advances only after flushed SSTables are published,
  and the WAL is atomically rewritten so startup does not decode indefinitely
  old flushed batches.
- Existing MVCC, range-delete, persistent, transaction, recovery, and release
  gates pass.

### Phase 17: File-Backed SSTable Reader

**Status**: Complete

**Goal**: Replace startup-time full SSTable decoding with metadata-only table
open and on-demand verified data block reads.

**Entry Condition**: Phase 16 complete and user audit identifies full SSTable
loading as the highest P0 production-readiness risk.

**Acceptance Gate**:

- Persistent table open reads footer, properties, index, and filter metadata
  without decoding data blocks.
- Point and range reads load only candidate data blocks and verify checksum,
  codec, and block/index consistency at read time.
- `BucketOptions::block_bytes` controls data block sizing.
- Block cache stores real decoded data blocks and reports misses/hits
  around actual block reads.
- Startup validates formal blob files using table/manifest metadata, including
  compaction outputs that keep older blob references.
- In-memory mode and persistent mode tests continue to pass.

### Phase 18: Real Bloom Filters

**Status**: Complete

**Goal**: Replace exact-set table filters with compact Bloom bitsets for
point-key and prefix filtering.

**Entry Condition**: Phase 17 complete and evidence shows exact-set filters are
the next read-path memory-cost mismatch.

**Acceptance Gate**:

- Point-key and prefix filters store Bloom bitsets, not complete key/prefix
  sets.
- `bits_per_key` and `bits_per_prefix` control bit counts and hash counts.
- Table-level and block-level filters still guard point and prefix read paths.
- False positives are allowed, but false negatives are rejected by table/block
  validation.
- In-memory and persistent mode tests continue to pass.

### Phase 19: Leveled Compaction And Range Tombstone Queries

**Status**: Complete

**Goal**: Make compaction use level pressure and target-sized outputs, and make
range tombstone reads use ordered query structures.

**Entry Condition**: Phase 18 complete and evidence identifies compaction output
sizing plus tombstone lookup as the next production-readiness risks.

**Acceptance Gate**:

- Range tombstones are stored in ordered query structures for memtables and
  SSTables.
- Point reads and transaction conflict checks query only tombstones whose bounds
  can cover the requested key or range.
- Scan setup includes only tombstones overlapping the iterator selector.
- L0 compaction groups overlapping L0 inputs with overlapping L1 inputs.
- L1+ compaction uses level-size pressure and moves selected inputs down one
  level with overlapping next-level inputs.
- Compaction outputs split by `target_table_bytes` at user-key boundaries.
- Existing in-memory, persistent, MVCC, range-delete, blob, and table tests keep
  passing.

### Phase 20: Iterator Merge And Background Maintenance

**Status**: Complete

**Goal**: Harden lazy scan source selection and make persistent background
maintenance honor `background_worker_count`.

**Entry Condition**: Phase 19 complete and evidence identifies linear iterator
source selection plus foreground-only flush/compaction scheduling as the next
risks.

**Acceptance Gate**:

- Lazy range and prefix iterators choose source groups through a heap keyed by
  user key and scan direction.
- Forward and reverse scans preserve MVCC visibility and range-delete behavior.
- Persistent databases start background maintenance workers when
  `background_worker_count > 0`, while `0` keeps maintenance explicit.
- Background maintenance can flush immutable memtables and compact L0 pressure.
- Background errors surface through later writes, `flush()`, or
  `compact_range()`.
- In-memory mode does not start background worker threads.
- Full local Rust verification passes.

### Phase 21: Internal LSM Core Boundary

**Status**: Complete

**Goal**: Separate one-bucket LSM tree rules from database-wide coordination
without changing public API behavior or storage formats.

**Entry Condition**: Phase 20 complete and user identifies DB/LSM coupling as
the next maintainability and correctness risk.

**Acceptance Gate**:

- The LSM core boundary spec is written and linked from the v1 protocol.
- `Db` remains responsible for WAL, manifest publish, process lock, recovery,
  background worker lifecycle, snapshots, transactions, and cross-bucket
  atomicity.
- `LsmTree` owns active and immutable memtables, table layout, tree-local reads,
  range tombstones, flush planning, compaction planning, and MVCC retention for
  one bucket as the extraction progresses.
- In-memory mode continues to use the same LSM core.
- Public API and storage formats remain unchanged.
- Full local Rust verification passes after each extraction slice.

### Phase 22: Versioned LSM Level Layout

**Status**: Complete

**Goal**: Replace the flat locked table list with a versioned level layout so
readers hold a stable tree version and flush/compaction publish new versions
atomically.

**Entry Condition**: Phase 21 complete and user review identifies the missing
Tree Version boundary as the next core LSM risk.

**Acceptance Gate**:

- LSM boundary spec records version and level-layout invariants.
- `LsmVersion` and `LevelState` model L0 overlap and L1+ non-overlap.
- `LsmTree` exposes read-safe version handles instead of requiring long table
  list lock use.
- Flush and compaction build and validate new versions before install.
- Recovery and in-memory setup build the same version structure.
- Old table/blob file cleanup respects old version handles held by lazy readers
  and snapshots.
- Existing public API and storage formats remain unchanged.
- Full local Rust verification passes.

### Phase 23: Memtable And Flush Scheduling Hardening

**Status**: Complete

**Goal**: Harden memtable accounting, bucket-local freeze behavior, and
immutable queue pressure before deeper table and compaction optimizations.

**Entry Condition**: Phase 22 complete and user review identifies P3 as the
next LSM tree improvement after versioned level layout.

**Acceptance Gate**:

- Memtable byte accounting no longer needs whole-map scans on normal writes.
- Freeze/flush pressure is tree-local and does not move unrelated buckets.
- Immutable memtable queue pressure and write backpressure are tested.
- In-memory mode follows the same logical LSM path.
- Existing public API and storage formats remain unchanged.
- Full local Rust verification passes.

### Phase 24: SSTable Read Path Detail Hardening

**Status**: Complete

**Goal**: Tighten table read-path details after version and memtable scheduling
are stable.

**Entry Condition**: Phase 23 complete and user review identifies P4 as the
next LSM tree improvement.

**Acceptance Gate**:

- Table point lookup has a per-block fast path that avoids unnecessary scans
  inside large data blocks.
- Block cache keys distinguish data, index/filter, range-tombstone, and future
  blob-related block classes.
- Cache hit behavior promotes recently used blocks instead of simple FIFO-only
  replacement.
- Any fd-cache or metadata pinning change is backed by focused corruption and
  lazy-read tests.
- Existing public API and storage formats remain unchanged unless protocol docs
  are updated first.
- Full local Rust verification passes.

### Phase 25: Filter Strategy Observability

**Status**: Complete

**Goal**: Make table and block filter behavior observable and harden prefix
filter skip paths before broader compaction or blob-GC work.

**Entry Condition**: Phase 24 complete and user review identifies P5 as the
next LSM tree improvement.

**Acceptance Gate**:

- Filter stats distinguish table/block filter hits and misses for point and
  prefix reads.
- Prefix filter tests prove nonmatching prefixes skip data-block reads when the
  extractor matches.
- False positives are counted only after a filter-allowed candidate yields no
  matching user key.
- Existing public API and storage formats remain unchanged unless protocol docs
  are updated first.
- Full local Rust verification passes.

### Phase 26: Compaction Picker Hardening

**Status**: Complete

**Goal**: Improve compaction input selection and move behavior without changing
storage format or MVCC retention rules.

**Entry Condition**: Phase 25 complete and user review identifies P6 as the
next LSM tree improvement.

**Acceptance Gate**:

- Compaction picker uses level score and L0 pressure without broadening work
  beyond the needed key range.
- L0 compaction keeps overlap closure behavior and lower-level overlap inputs.
- L1+ compaction can avoid full-level rewrites when a narrower range is enough.
- Trivial move is supported when an input table has no lower-level overlap.
- Output table splitting continues to respect target table bytes.
- Existing public API and storage formats remain unchanged unless protocol docs
  are updated first.
- Full local Rust verification passes.

### Phase 27: MVCC And Deletion Semantics Hardening

**Status**: Complete

**Goal**: Strengthen compaction retention and delete coverage rules before
read-path and blob-GC work continues.

**Entry Condition**: Phase 26 complete and the remaining P7/P8/P9/P10 LSM
hardening items are still open.

**Acceptance Gate**:

- Compaction keeps all versions newer than the oldest active snapshot and the
  newest version visible at or before that snapshot.
- Point deletes and range deletes are removed only when active snapshots and
  lower-level data make removal safe.
- Range tombstone coverage rules have dedicated randomized coverage tests
  against a simple reference model.
- Future single-delete support remains possible without changing current delete
  behavior.
- Existing public API and storage formats remain unchanged unless protocol docs
  are updated first.
- Full local Rust verification passes.

### Phase 28: Level-Aware Read Path Optimization

**Status**: Complete

**Goal**: Make point and scan table selection use the level layout more
directly, keeping read cost close to the number of relevant sources.

**Entry Condition**: Phase 27 complete and P8 read-path level optimization is
still open.

**Acceptance Gate**:

- Point reads check memtables, immutable memtables, overlapping L0 tables, and
  at most one candidate table per non-overlapping level.
- Range and prefix scans avoid selecting unrelated non-overlapping tables.
- L0 behavior remains overlap-safe.
- Range tombstones remain lazy and table/level scoped.
- Existing public API and storage formats remain unchanged unless protocol docs
  are updated first.
- Full local Rust verification passes.

### Phase 29: Blob GC Hardening

**Status**: Complete

**Goal**: Close the remaining value-separation lifecycle gaps around stale blob
bytes, compaction cleanup, and recovery consistency.

**Entry Condition**: Phase 28 complete and P9 blob-GC hardening is still open.

**Acceptance Gate**:

- Stats expose live and stale blob bytes.
- Compaction keeps live blob references and removes stale blob files only when
  snapshots and version handles no longer need them.
- Recovery verifies manifest/table/blob consistency for referenced blob files.
- Blob cleanup remains tied to compaction and version-file lifetime rules.
- Existing public API and storage formats remain unchanged unless protocol docs
  are updated first.
- Full local Rust verification passes.

### Phase 30: Verification Expansion

**Status**: Complete

**Goal**: Close the remaining validation gap with a deterministic randomized
model test across MVCC, deletes, scans, snapshots, and reopen.

**Entry Condition**: Phase 29 complete and P10 verification expansion is still
open.

**Acceptance Gate**:

- Random operation testing compares Trine against a simple MVCC reference
  model.
- Existing crash/reopen, corruption, long scan, and benchmark gates remain in
  the verification list.
- Full local Rust verification passes.

### Phase 31: Default Bucket API Polish

**Status**: Complete

**Goal**: Make the common public API operate directly on a built-in default
bucket and rename optional named namespaces to buckets.

**Entry Condition**: Phase 30 complete and user requests the default-bucket API
shape before release.

**Acceptance Gate**:

- `Db::put/get/range/prefix` operate on the default bucket without an explicit
  bucket open.
- `Db::bucket` and `Db::bucket_with_options` support optional named
  buckets.
- `BucketOptions` replaces the public options type for bucket configuration.
- The default bucket exists in memory and persistent modes after open.
- Protocol, usage docs, examples, tests, and benches use bucket terminology.
- Full local Rust verification passes.

### Phase 32: Titan-Like Large-Value Storage Spec

**Status**: Complete

**Goal**: Define the durable storage contract for Titan-like large-value
separation before implementation.

**Entry Condition**: Phase 31 complete and user requests a Titan-like
large-value subsystem with spec-first implementation order.

**Acceptance Gate**:

- Protocol records that small values stay inline and large values separate
  during flush/compaction only.
- `BlobIndex`, `BlobRecord`, `BlobFile`, manifest metadata, read path, GC,
  recovery, stats, tests, and implementation order are specified.
- V1 protocol links to the new large-value storage contract.
- External Titan references are design references only, not code or format
  dependencies.

### Phase 33: Bucket API Contract Hardening

**Status**: Complete

**Goal**: Tighten the default/named bucket API contract before value separation
changes introduce more storage metadata.

**Entry Condition**: Phase 32 spec is complete and user asks to handle bucket
API concerns before key-value separation.

**Acceptance Gate**:

- Direct `Db` helpers and default `WriteBatch`/`Transaction` methods operate on
  the built-in default bucket.
- `Db::bucket` is the common get-or-create entry for named buckets.
- `Db::bucket_with_options` is the explicit entry for fixed non-default bucket
  options.
- Named bucket methods are explicitly suffixed with `_bucket`.
- `"default"` is reserved and rejected by `bucket` and
  `bucket_with_options`.
- Default bucket options are configured through `DbOptions`.
- Protocol, usage docs, examples, benches, and tests use the tightened API.
- Focused bucket API tests and full Rust verification pass.

### Phase 34: Titan-Like Blob Format Foundation

**Status**: Complete

**Goal**: Stabilize the new `BlobIndex` and `BlobFile` encode/decode format
with focused tests before changing flush behavior.

**Entry Condition**: Phase 33 complete and the large-value storage protocol is
accepted as the implementation source of truth.

**Acceptance Gate**:

- `ValueRef::BlobIndex` carries encoded length, decoded length, value checksum,
  record checksum, and compression id.
- Blob file encode/decode validates header, ordered records, properties,
  footer, and checksums.
- Corruption tests cover missing/corrupt header, footer, record checksum, value
  checksum, and unsupported compression id.
- Existing small-value behavior remains unchanged.

### Phase 35: Titan-Like Blob Flush And Recovery Integration

**Status**: Complete

**Goal**: Use the new `BlobFile` format in real persistent table output and
validate referenced blob files during recovery.

**Entry Condition**: Phase 34 complete and user asks to finish the remaining
spec integration work.

**Acceptance Gate**:

- Flush and compaction table output store large inline values as `BlobIndex`
  records backed by the new `BlobFile` format.
- Small values remain inline and in-memory mode does not create disk blob files.
- Table and manifest metadata carry per-blob-file referenced bytes, record
  count, and key span.
- Persistent open validates every manifest-referenced blob file and fails
  closed on corrupt blob data.
- `DbStats` exposes blob read count and bytes.
- Full local Rust verification passes.

### Phase 36: Snapshot-Safe Blob GC

**Status**: Complete

**Goal**: Finish the first Titan-like large-value lifecycle by making stale
blob files recoverable, measurable, and safe to reclaim.

**Entry Condition**: Phase 35 complete and user asks to finish the remaining
large-value work.

**Acceptance Gate**:

- Compaction records obsolete blob files as manifest pending deletions instead
  of deleting them directly.
- Blob GC rewrites still-live records from partially stale blob files into new
  blob files without creating user-visible MVCC versions.
- Old blob files remain readable while an active snapshot or range iterator can
  still reach old table handles.
- Writable recovery tolerates manifest-pending obsolete blob files and resumes
  physical cleanup.
- Cleanup refuses to delete a pending blob file that is still referenced by a
  manifest-live table.
- `DbOptions` exposes blob GC threshold/ratio controls and `DbStats` exposes GC
  counters.
- Full local Rust verification passes.

### Phase 37: Large-Value Benchmark And Direct Blob Read

**Status**: Complete

**Goal**: Add benchmark coverage for the large-value path and remove the
measured whole-blob decode from point reads.

**Entry Condition**: Phase 36 complete and blob GC throughput has no dedicated
benchmark baseline.

**Acceptance Gate**:

- Benchmark harness reports large-value point read, range scan, and GC rewrite
  rows.
- Evidence records pre/post benchmark numbers for the selected tuning change.
- `BlobIndex` point reads seek to the indexed blob record and verify only that
  record.
- Full local Rust verification passes.

### Phase 38: Blob Maintenance And Value-Lazy Iteration

**Status**: Complete

**Goal**: Finish the first post-GC large-value maintenance pass with optional
Level Merge, value-lazy reads, GC rewrite path tightening, and broader recovery
fault injection.

**Entry Condition**: Phase 37 complete and user asks to finish Titan Level
Merge, value-lazy iterator, blob GC throughput optimization, and systematic
crash/recovery fault injection.

**Acceptance Gate**:

- Level Merge has a compaction-time rewrite path for retained large values.
- Value-lazy range/prefix APIs avoid blob reads until callers request values.
- GC candidate selection uses blob properties metadata and live-record copying
  uses indexed blob reads.
- Recovery fault matrix covers representative temp publish, missing file,
  corrupt file, and unreferenced formal file cases.
- Protocol/docs and benchmark notes describe the implemented behavior.
- Full local Rust verification passes.

### Phase 39: Automatic Blob Maintenance Policy

**Status**: Complete

**Goal**: Close the Phase 38 policy gaps by making blob Level Merge automatic
by default and batching blob GC candidates.

**Entry Condition**: Phase 38 complete and user clarifies that Level Merge
should use an automatic strategy and GC should handle multiple candidates in
one maintenance pass.

**Acceptance Gate**:

- `BucketOptions` exposes `BlobLevelMergePolicy` with `Auto` as the default.
- Manifest v7 persists the policy, while v5/v6 bucket options decode into the
  new policy without losing compatibility.
- Auto Level Merge rewrites retained blob values when compaction output would
  otherwise keep scattered blob references or leave stale input blob refs
  behind.
- `Disabled` and `Always` remain available for benchmarks and explicit tuning.
- Blob GC batches all candidates that pass the discard threshold into one
  rewrite plan and one manifest publish.
- Protocol, usage docs, README, benchmark notes, tests, and evidence describe
  the implemented behavior.
- Full local Rust verification passes.

### Phase 40: Table Read-Path Index Hardening

**Status**: Complete

**Goal**: Remove fake search-policy surface area and make large persistent
tables open with only the small top-level table index resident.

**Entry Condition**: Phase 39 complete and user requests block hash lookup,
real search-policy behavior, and partitioned index/filter loading before
release.

**Acceptance Gate**:

- Data blocks encode and decode a checked point-lookup hash index.
- Point lookup inside a decoded data block uses the hash index and compares
  keys only for hash collisions.
- Retired search-policy manifest tags remain readable by mapping to `Auto`.
- Benchmark rows advertise only implemented linear, binary, and auto policies.
- Persistent table open reads footer, properties, and top-level index metadata;
  partition index/filter blocks load lazily.
- Filter misses can skip data blocks through lazily loaded partition filters.
- Full local Rust verification passes.

### Phase 41: Background Maintenance Scheduling And Backpressure

**Status**: Complete

**Goal**: Make persistent flush/compaction maintenance run by default, keep
writes out of heavy maintenance work, and add clear pressure behavior when the
LSM falls behind.

**Entry Condition**: Phase 40 complete and user identifies maintenance
scheduling, backpressure, writer-lock scope, compaction picker locality,
concurrent compaction boundaries, and long-running compaction validation as the
next release risks.

**Acceptance Gate**:

- Persistent default options start background maintenance workers unless the
  user explicitly sets `background_worker_count` to `0`.
- Background maintenance has separate flush and compaction requests, progress
  notification, in-flight state, and error propagation.
- Writes wait or help maintenance when immutable memtables or L0 table pressure
  exceeds configured limits.
- Table building and compaction merge work run outside the writer coordinator;
  the writer coordinator is used for commit sequencing and short publish
  cutovers.
- Compaction picker selects local key spans and avoids broad rewrites when a
  narrower safe span exists.
- Concurrent compactions cannot overlap in the same bucket key range, while
  non-overlapping compactions may proceed.
- Tests cover level non-overlap, MVCC retention, range-delete preservation,
  default worker behavior, and backpressure.

### Phase 42: Persistent Read-Path Resource Policy

**Status**: Complete

**Goal**: Reduce persistent read-path overhead by caching table file handles,
pinning hot L0/L1 index/filter metadata, and adding a high-priority block-cache
policy for metadata.

**Entry Condition**: Phase 41 complete and user identifies descriptor/file
handle churn, per-level index/filter pinning, block-cache priority, and
benchmark-gated key encoding as the next release risks.

**Acceptance Gate**:

- Persistent block reads reuse the table's cached file handle without cloning or
  reopening it per block.
- L0/L1 tables pin table filters and index partitions, while deeper levels keep
  partition metadata lazy.
- Lazy index partitions use the global block cache when available.
- Block-cache eviction protects high-priority metadata entries from
  low-priority data churn.
- Shared-prefix key benchmark evidence exists before any key-encoding change.
- Full local Rust verification passes.

### Phase 43: Public Maintenance Barrier Semantics

**Status**: Complete

**Goal**: Make public `flush()` and `compact_range()` preserve their foreground
API contracts when background maintenance already owns the relevant guard.

**Entry Condition**: Phase 42 complete and user identifies that public
maintenance APIs can return success after non-blocking helper conflicts.

**Acceptance Gate**:

- Public `flush()` captures the call boundary and returns only after writes
  committed before that boundary are out of active and immutable memtables.
- Background workers and write-pressure handling keep non-blocking best-effort
  helpers.
- Public `compact_range()` waits and retries when overlapping compaction
  reservations are active instead of silently succeeding.
- Focused tests cover flush guard contention, default background flush publish,
  and compaction reservation contention.
- Full local Rust verification passes.

### Phase 44: Lock-Free Foreground Write Path Spec

**Status**: Complete

**Goal**: Define the production-grade multi-writer write-path contract before
changing commit, WAL, delta, visibility, or recovery code.

**Entry Condition**: User chooses the stronger production direction: foreground
reads and blind writes should avoid a global writer lock, while background I/O
and maintenance can remain single-owner.

**Acceptance Gate**:

- Protocol defines the exact boundary between foreground no-global-lock paths
  and background owner workers.
- Protocol covers `PreparedCommit`, key-sharded immutable deltas, WAL shards,
  commit slot states, visible sequence advancement, recovery, backpressure,
  actor/worker boundaries, stats, and tests.
- Existing v1 protocol links to the new write-path protocol as the source of
  truth for the next implementation phase.
- Current phase file records scope, out-of-scope, verification, and blockers.
  No Rust behavior changes are made in this spec phase.

### Phase 45: Async-First Portable Storage And WASM Spec

**Status**: Complete

**Goal**: Make the v1 spec async-first at the public API and storage boundary,
with portable backend capabilities and WASM readiness defined before
implementation.

**Entry Condition**: User identifies synchronous `Db::open` and native-file
assumptions as the wrong long-term architecture for cross-platform storage.

**Acceptance Gate**:

- Protocol defines the primary async API, blocking adapter role, storage
  backend capabilities, manifest publish abstraction, durability mapping,
  cancellation rules, background work, backend families, recovery, stats, tests,
  and implementation staging.
- V1 protocol links to the async-first storage protocol and updates public API,
  storage mode, durability, cursor, error, test, benchmark, and acceptance-gate
  language.
- Decision framework records async-first storage and WASM readiness as durable
  boundaries.
- Current phase and evidence record scope, out-of-scope, verification, and
  remaining blockers.
- No Rust behavior changes are made in this spec phase.

### Phase 46: Block Manager Extraction

**Status**: Complete

**Goal**: Centralize table block content lifecycle before async storage
implementation.

**Entry Condition**: Phase 45 complete and user identifies block content
lifecycle as the better first implementation boundary before async storage.

**Acceptance Gate**:

- Checked block encoding, decoding, compression, checksum verification, and
  content read helpers move behind a focused internal Block Manager module.
- SSTable format, public API, MVCC, manifest, blob, compaction, transaction,
  and cache semantics remain unchanged.
- Existing block cache behavior and stats remain intact.
- Focused table/persistent tests, formatting, and diff checks pass.

### Phase 47: Block Read Source Boundary

**Status**: Complete

**Goal**: Make checked-block reads depend on a named read-source boundary before
the first storage backend implementation slice.

**Entry Condition**: Phase 46 complete and user asks to continue toward async
storage migration.

**Acceptance Gate**:

- `BlockManager` reads checked blocks through a named synchronous read-source
  boundary instead of ad hoc closures.
- The native-file table read path remains the only concrete persistent read
  source in this slice.
- SSTable format, public API, cache semantics, MVCC, manifest, blob,
  compaction, and transaction behavior remain unchanged.
- Focused table/persistent tests, formatting, clippy, and diff checks pass.

### Phase 48: Native-File Storage Read Adapter

**Status**: Complete

**Goal**: Put persistent table block reads behind database-level storage object
ids and a native-file read adapter.

**Entry Condition**: Phase 47 complete and user asks to implement the first
concrete storage backend slice.

**Acceptance Gate**:

- Internal storage object kind/id types exist for database storage objects.
- Persistent table checked-block reads use a native-file read adapter keyed by
  a storage object id.
- SSTable format, public API, cache semantics, MVCC, manifest, blob,
  compaction, transaction, and cleanup behavior remain unchanged.
- Focused table/persistent tests, formatting, clippy, and diff checks pass.

### Phase 49: Table Open Storage Boundary

**Status**: Complete

**Goal**: Move persistent table open and startup metadata reads behind the
native-file storage adapter.

**Entry Condition**: Phase 48 complete and user asks to finish the storage
backend boundary before async storage work.

**Acceptance Gate**:

- Persistent table open, file length, header, footer, properties, top-level
  index, pinned filters, and pinned index metadata reads use a native-file
  storage object and adapter keyed by a storage object id.
- Persistent table checked-block reads continue to use the same adapter.
- SSTable format, public API, cache semantics, MVCC, manifest, blob,
  compaction, transaction, and cleanup behavior remain unchanged.
- Focused table/persistent tests, formatting, clippy, and diff checks pass.

### Phase 50: Async Storage Read Trait Shape

**Status**: Complete

**Goal**: Define the first internal async storage read trait shape without
changing public APIs or storage formats.

**Entry Condition**: Phase 49 complete and user asks to continue after the
table read storage boundary lands.

**Acceptance Gate**:

- Internal async read backend/object traits exist for storage object open,
  object length, and random reads without choosing a concrete async runtime.
- Native-file storage implements the async trait shape and a blocking adapter
  for the current synchronous table read path.
- Persistent table read behavior, SSTable format, block cache semantics, MVCC,
  manifest, WAL, blob, compaction, transaction, and public API behavior remain
  unchanged.
- Focused table/persistent tests, formatting, clippy, and diff checks pass.

### Phase 51: Storage Capability And Error Types

**Status**: Complete

**Goal**: Add explicit storage capability checks and typed unsupported errors
before routing write or manifest operations through the backend.

**Entry Condition**: Phase 50 complete and user asks to continue storage
backend migration.

**Acceptance Gate**:

- Internal storage capability types name current read guarantees and later
  write, publish, durability, lease, cleanup, background, and runtime
  guarantees.
- Unsupported backend capability and unsupported durability errors are explicit
  typed variants.
- Current table random-read requirement uses the capability helper.
- Public API behavior, SSTable format, MVCC, manifest, WAL, blob, compaction,
  transaction, and cleanup behavior remain unchanged.
- Focused table/persistent tests, formatting, clippy, and diff checks pass.

### Phase 52: Memory Storage Read Backend

**Status**: Complete

**Goal**: Route memory storage objects through the same internal async read
contract as native-file table reads.

**Entry Condition**: Phase 51 complete and user asks to continue storage
backend migration.

**Acceptance Gate**:

- Volatile memory storage backend implements the same read backend/object traits
  as native-file storage.
- Memory backend reports volatile random-read capability and no persistence,
  write, publish, lease, cleanup, or durability guarantees.
- Table-byte decode coverage reads through the memory storage object and checked
  block source path.
- Public API behavior, SSTable format, MVCC, manifest, WAL, blob, compaction,
  transaction, and production in-memory DB behavior remain unchanged.
- Focused storage/table tests, formatting, clippy, and diff checks pass.

### Phase 53: Native-File Manifest Publish Backend

**Status**: Complete

**Goal**: Route manifest publish through the native-file storage backend
operation before broader write-path or public async migration.

**Entry Condition**: Phase 52 complete and user asks to continue following the
async storage protocol.

**Acceptance Gate**:

- Native-file backend reports atomic manifest publish and strict sync
  capabilities honestly.
- Native-file backend exposes a manifest publish operation that preserves the
  current manifest byte format and atomic publish behavior.
- `ManifestStore` publishes through the backend operation and still keeps
  in-memory state unchanged if publish fails.
- Public API behavior, SSTable format, MVCC, WAL, blob, compaction,
  transaction, manifest recovery, and table read/write behavior remain
  unchanged.
- Focused storage/manifest/persistent tests, formatting, clippy, and diff
  checks pass.

### Phase 54: Native-File Manifest Read Backend

**Status**: Complete

**Goal**: Route current-manifest reads through the native-file storage backend
operation.

**Entry Condition**: Phase 53 complete and user asks to continue following the
async storage protocol.

**Acceptance Gate**:

- Native-file backend exposes a current-manifest read operation that returns
  `None` for a missing manifest and bytes for an existing manifest.
- `ManifestStore::open_or_create` and `read_manifest` use the backend read
  operation while preserving manifest decode and create-if-missing behavior.
- Public API behavior, SSTable format, MVCC, WAL, blob, compaction,
  transaction, table read/write behavior, and manifest byte format remain
  unchanged.
- Focused storage/manifest/persistent tests, formatting, clippy, and diff
  checks pass.

### Phase 55: Native-File Object Listing Backend

**Status**: Complete

**Goal**: Route table file id discovery through the storage backend object
listing operation.

**Entry Condition**: Phase 54 complete and the async storage protocol plan is
the active implementation guide.

**Acceptance Gate**:

- Roadmap and current phase map completed storage-boundary work to the async
  storage protocol staging.
- Native-file backend exposes an object listing operation for database storage
  object kinds.
- Native-file backend reports object listing capability before table discovery
  uses that operation.
- Table file id listing uses the backend listing operation while preserving
  current filename validation and filtering behavior.
- Public API behavior, SSTable format, MVCC, manifest, WAL, blob, compaction,
  transaction, table read/write behavior, cleanup behavior, and storage format
  remain unchanged.
- Focused storage/table/persistent tests, formatting, clippy, and diff checks
  pass.

### Phase 56: Native-File Table Object Write Backend

**Status**: Complete

**Goal**: Route table output-file creation through the storage backend object
write operation.

**Entry Condition**: Phase 55 complete and table object discovery already routes
through the backend boundary.

**Acceptance Gate**:

- Native-file backend reports object write capability before table output writes
  use that operation.
- Native-file backend exposes a complete-object write operation for table
  objects.
- Table output writes use the backend operation while preserving table bytes,
  temporary-file naming, file sync, final rename, and reopen behavior.
- Parent-directory sync batching remains owned by existing flush/compaction
  callers and still occurs before manifest publish.
- Public API behavior, SSTable format, MVCC, manifest, WAL, blob, compaction,
  transaction, cleanup behavior, and storage format remain unchanged.
- Focused storage/table/persistent tests, formatting, clippy, and diff checks
  pass.

### Phase 57: Native-File Blob Object Write Backend

**Status**: Complete

**Goal**: Route blob file creation through the storage backend object write
operation.

**Entry Condition**: Phase 56 complete and table output writes already use the
backend object write boundary.

**Acceptance Gate**:

- Storage object kinds include blob objects.
- Native-file backend can write blob objects through the generic object write
  operation while still rejecting manifest objects.
- `write_blob_file` uses the backend write operation while preserving blob
  bytes, temporary-file naming, file sync, final rename, and returned indexes.
- Parent-directory sync batching remains owned by existing flush/compaction
  callers and still occurs before manifest publish.
- Public API behavior, SSTable format, MVCC, manifest, WAL, compaction,
  transaction, cleanup behavior, and storage format remain unchanged.
- Focused storage/blob/table/persistent tests, formatting, clippy, and diff
  checks pass.

### Phase 58: Native-File Object Delete Backend

**Status**: Complete

**Goal**: Route table and blob cleanup deletion through the storage backend
object delete operation.

**Entry Condition**: Phase 57 complete and table/blob object creation already
routes through backend-owned object write paths.

**Acceptance Gate**:

- Native-file backend reports object delete capability.
- Native-file backend exposes an idempotent object delete operation for table
  and blob objects.
- Generic object delete still rejects manifest objects so manifest publish
  remains the only manifest update path.
- Pending obsolete table cleanup, pending obsolete blob cleanup, and failed
  flush/compaction output cleanup use the backend delete operation while
  preserving snapshot and manifest safety checks.
- Public API behavior, SSTable/blob formats, MVCC, manifest, WAL, compaction,
  transaction, and storage format remain unchanged.
- Focused storage/persistent tests, formatting, clippy, and diff checks pass.

### Phase 59: Native-File Blob Object Read Backend

**Status**: Complete

**Goal**: Route blob file reads through the storage backend random-read
operation.

**Entry Condition**: Phase 58 complete and table/blob creation plus cleanup
deletion already route through backend-owned object operations.

**Acceptance Gate**:

- Blob full-file reads use the storage backend read object while preserving
  full validation behavior.
- Blob properties reads use the storage backend read object while preserving
  properties-only execution shape.
- Indexed blob record reads use the storage backend read object while
  preserving target-record-only execution shape.
- Blob format checks, checksums, corrupt/missing blob errors, blob GC,
  recovery, compaction, public API behavior, MVCC, WAL, manifest, and storage
  formats remain unchanged.
- Focused blob/persistent tests, formatting, clippy, and diff checks pass.

### Phase 60: Native-File Blob Object Listing Backend

**Status**: Complete

**Goal**: Route blob object listing through the storage backend object listing
operation.

**Entry Condition**: Phase 59 complete and blob object bytes already route
through backend read/write/delete operations.

**Acceptance Gate**:

- Blob file discovery uses the storage backend object listing operation.
- Blob file-id parsing remains in the blob module.
- Directory skipping, non-blob extension filtering, non-blob prefix filtering,
  uppercase extension handling, and malformed blob filename corruption behavior
  remain unchanged.
- Recovery, stats, blob GC, public API behavior, MVCC, WAL, manifest,
  compaction, and storage formats remain unchanged.
- Focused blob/recovery/persistent tests, formatting, clippy, and diff checks
  pass.

### Phase 61: Native-File WAL Append Backend

**Status**: Complete

**Goal**: Route WAL append and WAL persist through the storage backend append
operation.

**Entry Condition**: Phase 60 complete and table/blob object lifecycle
operations already route through backend operations.

**Acceptance Gate**:

- Native-file backend reports append capability.
- Native-file backend exposes a WAL append object that can append bytes and
  persist by requested durability mode.
- Non-WAL objects are rejected by the append path.
- `WalWriter` uses the backend append object while preserving WAL frame bytes,
  replay, torn-tail handling, checksum failure behavior, and commit visibility
  ordering.
- WAL rewrite-after-flush, manifest publish, table/blob formats, MVCC,
  compaction, recovery policy, and public API behavior remain unchanged.
- Focused storage/WAL/persistent tests, formatting, clippy, and diff checks
  pass.

### Phase 62: Native-File Writer Lease Backend

**Status**: Complete

**Goal**: Route persistent writer lease acquisition and release through the
storage backend writer-lease operation.

**Entry Condition**: Phase 61 complete and the persistent commit append path
already routes through storage backend operations.

**Acceptance Gate**:

- Native-file backend reports writer lease capability.
- Native-file backend exposes a writer-lease operation that preserves the
  existing `LOCK` marker behavior.
- Existing lease markers fail closed until an operator removes them.
- Lease release removes only a marker still owned by the releasing handle.
- Persistent writable open uses the backend writer-lease operation.
- Read-only open still avoids writer lease acquisition.
- Recovery, WAL, manifest, table/blob formats, MVCC, compaction, and public API
  behavior remain unchanged.
- Focused storage/writer-lock/persistent tests, formatting, clippy, and diff
  checks pass.

### Phase 63: Native-File Directory Sync Backend

**Status**: Complete

**Goal**: Route native-file directory metadata sync after atomic renames through
the storage backend.

**Entry Condition**: Phase 62 complete and the persistent writable-open,
commit-append, table/blob object, and cleanup paths already route through
backend operations.

**Acceptance Gate**:

- Native-file backend reports directory-sync capability.
- Native-file backend exposes a directory-sync operation for rename publish
  barriers.
- WAL rewrite, recovery report publish, flush output publish barriers,
  compaction output publish barriers, and blob-GC output publish barriers use
  the backend directory-sync operation.
- Table/blob output batching remains one directory sync after one or more
  renames and before manifest publish.
- Public API behavior, WAL, manifest, table/blob formats, MVCC, compaction,
  recovery policy, and storage format remain unchanged.
- Focused storage/recovery/WAL/persistent tests, formatting, clippy, and diff
  checks pass.

### Phase 64: Native-File WAL Rewrite Backend

**Status**: Complete

**Goal**: Route WAL rewrite-after-flush through the storage backend while
preserving the existing WAL rewrite temporary file protocol.

**Entry Condition**: Phase 63 complete and both WAL append plus directory sync
already route through backend operations.

**Acceptance Gate**:

- Native-file backend reports atomic WAL rewrite capability.
- Native-file backend exposes a WAL rewrite operation with an explicit
  temporary WAL object.
- WAL rewrite keeps using `trine.wal.tmp` so recovery still recognizes safe
  rewrite leftovers.
- `rewrite_batches_after` uses the backend operation while preserving WAL frame
  bytes, replay filtering, checksum behavior, and writer reopen behavior.
- Public API behavior, WAL format, manifest, table/blob formats, MVCC,
  compaction, recovery policy, and cleanup semantics remain unchanged.
- Focused storage/WAL/recovery/persistent tests, formatting, clippy, and diff
  checks pass.

### Phase 65: Native-File Recovery Report Write Backend

**Status**: Complete

**Goal**: Route recovery report publish through storage backend object write
and directory-sync operations.

**Entry Condition**: Phase 64 complete and object writes plus directory sync
already route through backend operations.

**Acceptance Gate**:

- Storage object kinds include recovery report objects.
- Recovery report publish uses backend object write while preserving
  `RECOVERY_REPORT.tmp`.
- Recovery report publish uses backend directory sync after the rename.
- Manifest publish remains reserved for manifest objects.
- Public API behavior, recovery report format, safe temporary file policy, WAL,
  manifest, table/blob formats, MVCC, compaction, and cleanup semantics remain
  unchanged.
- Focused storage/recovery/persistent tests, formatting, clippy, and diff
  checks pass.

### Phase 66: WAL Replay Optional Object Read Backend

**Status**: Complete

**Goal**: Route WAL replay reads through a backend operation that treats a
missing WAL as a normal empty replay.

**Entry Condition**: Phase 65 complete and WAL append/rewrite plus recovery
report publish already route through backend operations.

**Acceptance Gate**:

- Native-file and in-memory backends report optional object-read capability.
- Native-file optional object read returns bytes for existing objects and
  `None` for missing objects.
- In-memory optional object read returns bytes for existing objects and `None`
  for missing objects.
- `read_batches_after` uses the backend operation while preserving WAL replay,
  torn-tail, checksum, and replay-floor behavior.
- Public API behavior, WAL format, recovery policy, manifest, table/blob
  formats, MVCC, compaction, and cleanup semantics remain unchanged.
- Focused storage/WAL/recovery/persistent tests, formatting, clippy, and diff
  checks pass.

### Phase 67: Native-File Recovery Report Read Backend

**Status**: Complete

**Goal**: Route public recovery report reads through backend optional object
read.

**Entry Condition**: Phase 66 complete and optional object read is available for
native-file storage.

**Acceptance Gate**:

- `read_recovery_report` reads through the storage backend.
- Missing recovery reports still return a `NotFound` I/O error.
- Recovery report text format and decode behavior remain unchanged.
- Public API behavior, recovery repair policy, WAL, manifest, table/blob
  formats, MVCC, compaction, and cleanup semantics remain unchanged.
- Focused recovery/persistent tests, formatting, clippy, and diff checks pass.

### Phase 68: Native-File Directory Create Backend

**Status**: Complete

**Goal**: Route persistent database directory creation through storage backend
operations.

**Entry Condition**: Phase 67 complete and native-file directory ids are already
used for backend directory sync.

**Acceptance Gate**:

- Native-file backend reports directory-create capability.
- Native-file backend exposes a directory-create operation.
- Persistent create-if-missing uses backend directory creation.
- Read-only missing database path still fails without creating directories.
- Public API behavior, recovery policy, WAL, manifest, table/blob formats,
  MVCC, compaction, stats, and cleanup semantics remain unchanged.
- Focused storage/persistent/recovery tests, formatting, clippy, and diff
  checks pass.

### Phase 69: Stats Object Length Backend Reads

**Status**: Complete

**Goal**: Route persistent stats byte accounting through storage backend
object-open and object-length operations.

**Entry Condition**: Phase 68 complete and native-file random reads are already
available as storage backend operations.

**Acceptance Gate**:

- Table byte stats use backend object length reads.
- Obsolete blob byte stats use backend object length reads.
- Stats keep the previous fail-open behavior for missing or unreadable files.
- Public API behavior, recovery policy, WAL, manifest, table/blob formats,
  MVCC, compaction, and cleanup semantics remain unchanged.
- Focused stats/persistent tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 70: Recovery Directory-File Listing Backend

**Status**: Complete

**Goal**: Route recovery safe temporary file scanning/deletion and referenced
blob existence checks through storage backend operations.

**Entry Condition**: Phase 69 complete and object delete plus random-read
backend operations are already available.

**Acceptance Gate**:

- Native-file backend reports directory-file listing capability.
- Native-file backend exposes directory-file listing for regular files.
- Recovery safe temporary file scanning uses backend directory listing.
- Recovery safe temporary file repair deletion uses backend object deletion.
- Referenced blob existence checks use backend object open.
- Recovery fail-closed, repair-safe-temporary, and unreferenced formal file
  policies remain unchanged.
- Focused storage/recovery tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 71: Public Async Compatibility API

**Status**: Complete

**Goal**: Add the first public async compatibility surface for database and
bucket open/read/write helpers without breaking the existing blocking API.

**Entry Condition**: Phase 70 complete and internal storage operations already
have async-shaped futures plus blocking adapters.

**Acceptance Gate**:

- `Db` exposes async compatibility methods for open, point read/write/delete,
  batch write, persist, flush, compaction, and close.
- `Bucket` exposes async compatibility methods for point read/write/delete and
  basic range/prefix iterator construction.
- The compatibility surface does not choose a concrete runtime and does not
  claim native-file storage is non-blocking.
- A focused memory-mode async smoke test passes without an external runtime
  crate.
- Existing blocking API behavior remains unchanged.
- Focused async API tests, formatting, clippy, full tests, and diff checks
  pass.

### Phase 72: Async Cursor Compatibility Advancement

**Status**: Complete

**Goal**: Add async compatibility advancement for range/prefix cursors and lazy
values without removing existing blocking iterator behavior.

**Entry Condition**: Phase 71 complete and async range/prefix construction
methods already exist for `Db` and `Bucket`.

**Acceptance Gate**:

- `Iter` exposes async advancement returning `Result<Option<KeyValue>>`.
- `LazyIter` exposes async advancement returning `Result<Option<LazyKeyValue>>`.
- `LazyValue` and `LazyKeyValue` expose async compatibility read/conversion
  methods.
- Existing `Iterator` implementations remain unchanged.
- A focused memory-mode async smoke test consumes normal and lazy iterators
  through async cursor methods without an external runtime crate.
- Focused async API tests, formatting, clippy, full tests, and diff checks
  pass.

### Phase 73: Commit Tracker State Machine

**Status**: Complete

**Goal**: Put explicit accepted-write terminal states behind the current writer
coordinator before deeper async cancellation and multi-writer work.

**Entry Condition**: Phase 72 complete and public async compatibility methods
exist for point writes, batches, and cursor advancement.

**Acceptance Gate**:

- Commit slots have explicit `Open`, `Visible`, and `Skipped` states.
- Successful writes reserve a slot, append WAL, publish deltas, and mark the
  slot visible.
- Accepted failures before delta publication mark the slot skipped.
- The public read boundary advances through the commit tracker.
- WAL replay resets the tracker to the recovered durable boundary.
- The existing writer coordinator, WAL/table/blob/manifest formats, MVCC read
  behavior, and public API shapes remain unchanged.
- Focused commit-tracker tests, formatting, clippy, full tests, and diff checks
  pass.

### Phase 74: Async Transaction Compatibility And Write Cancellation Tests

**Status**: Complete

**Goal**: Add the missing async transaction compatibility surface and lock down
the current async write cancellation behavior before introducing an owned
runtime execution boundary.

**Entry Condition**: Phase 73 complete and commit acceptance/terminal state is
represented by the commit tracker.

**Acceptance Gate**:

- `Transaction` exposes async compatibility methods for point reads, range
  reads, and commit.
- Async API tests cover transaction async reads and commit.
- Dropping an unpolled async write future has no side effect.
- Polling an async write future reaches a visible terminal commit.
- The current no-runtime compatibility model, writer coordinator, commit
  tracker, MVCC, WAL/table/blob/manifest formats, compaction, and recovery
  behavior remain unchanged.
- Focused async tests, formatting, clippy, full tests, and diff checks pass.

### Phase 75: Runtime Boundary For Background Execution

**Status**: Complete

**Goal**: Introduce the minimal runtime boundary needed before accepted writes
can move to owned async execution.

**Entry Condition**: Phase 74 complete and current async write cancellation
behavior is covered by tests.

**Acceptance Gate**:

- Runtime options and capabilities are public and default to native-thread
  behavior.
- Background maintenance worker spawning goes through the runtime boundary.
- Persistent writable open rejects background workers when the selected runtime
  has no background-thread capability.
- Existing default background worker behavior remains unchanged.
- Writer coordinator, commit tracker, WAL/table/blob/manifest formats, MVCC,
  compaction, recovery, cleanup, and public API behavior remain unchanged.
- Focused runtime tests, formatting, clippy, full tests, and diff checks pass.

### Phase 76: Runtime Cancellation And Task Join Primitives

**Status**: Complete

**Goal**: Add the runtime cancellation and task-join primitives needed before
accepted writes can move to owned task execution.

**Entry Condition**: Phase 75 complete and background worker spawning already
routes through the runtime boundary.

**Acceptance Gate**:

- Runtime exposes cancellation token and task-join capabilities.
- Cancellation token clones share state and are test-covered.
- Native-thread background tasks can observe cancellation and join in tests.
- Database background worker shutdown cancels the runtime token.
- Existing default background worker behavior remains unchanged.
- Writer coordinator, commit tracker, WAL/table/blob/manifest formats, MVCC,
  compaction, recovery, cleanup, and public API behavior remain unchanged.
- Focused runtime/background tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 77: Owned Write Request And Completion Shape

**Status**: Complete

**Goal**: Introduce the owned write request and completion waiter needed before
accepted writes can move to runtime-owned task execution.

**Entry Condition**: Phase 76 complete and runtime cancellation/task-join
primitives exist.

**Acceptance Gate**:

- Batch writes and transaction commits build an owned write request.
- Current write execution completes through an internal accepted-write waiter.
- The waiter delivers successful and failed commit results without cloning
  commit errors.
- Existing async cancellation tests continue to pass.
- Writer coordinator, commit tracker, WAL/table/blob/manifest formats, MVCC,
  compaction, recovery, cleanup, and public API behavior remain unchanged.
- Focused write/async tests, formatting, clippy, full tests, and diff checks
  pass.

### Phase 78: Runtime-Owned Write Execution

**Status**: Complete

**Goal**: Move accepted async writes behind the runtime boundary while
preserving cancellation-before-poll and terminal-after-acceptance behavior.

**Entry Condition**: Phase 77 complete and owned write request/completion types
exist.

**Acceptance Gate**:

- Async batch, default-bucket, named-bucket, and transaction writes create an
  accepted write and hand execution to the runtime after the first poll.
- Dropping an unpolled async write future has no side effect.
- Dropping a polled accepted write future does not cancel the internal commit in
  native-thread runtime mode.
- Inline runtime mode still completes async writes without background-thread
  capability.
- Blocking write and transaction commit behavior remains unchanged.
- Writer coordinator, commit tracker, WAL/table/blob/manifest formats, MVCC,
  compaction, recovery, cleanup, and public API behavior remain unchanged.
- Focused async/write/runtime tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 79: Writer-Local Accepted State And Publish Barrier

**Status**: Complete

**Goal**: Make accepted write state and the durable publish boundary explicit
before changing the writer coordinator shape.

**Entry Condition**: Phase 78 complete and accepted async writes run under
Trine-owned runtime execution after first poll.

**Acceptance Gate**:

- `DbInner` has a named publish barrier instead of an anonymous writer mutex.
- Write acceptance/preflight returns an explicit writer-local state before
  entering the publish barrier.
- Publish-time routing, transaction validation, sequence assignment, WAL
  append, memtable delta publication, visibility marking, and post-commit
  freeze remain serialized by the named publish barrier.
- Blocking and async write behavior remains unchanged.
- Commit tracker, WAL/table/blob/manifest formats, MVCC, compaction, recovery,
  cleanup, and public API behavior remain unchanged.
- Focused write/concurrency tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 80: Bounded Runtime Blocking Scheduler

**Status**: Complete

**Goal**: Add a bounded native blocking task scheduler so runtime-owned async
work does not create an unbounded thread per accepted write.

**Entry Condition**: Phase 79 complete and async accepted writes already run
behind Trine's runtime boundary after first poll.

**Acceptance Gate**:

- Native runtime mode owns a bounded blocking task pool.
- Blocking adapter submissions return a recoverable error when the task queue is
  full or shutting down.
- Accepted async writes use the bounded blocking adapter instead of spawning one
  thread per write.
- Long-lived background maintenance workers remain on dedicated background
  threads.
- Inline runtime async writes still complete without background threads.
- Public async API, blocking API, publish barrier, commit tracker,
  WAL/table/blob/manifest formats, MVCC, compaction, recovery, cleanup, and
  storage behavior remain unchanged.
- Focused runtime/async tests, formatting, clippy, full tests, and diff checks
  pass.

### Phase 81: Owned Async Storage Read Completion

**Status**: Complete

**Goal**: Define an owned async storage read completion boundary so storage
reads can cross runtime and portable backend boundaries without borrowing the
caller's output buffer.

**Entry Condition**: Phase 80 complete and runtime-owned blocking work has a
bounded scheduler.

**Acceptance Gate**:

- Storage read objects expose an owned read-buffer completion API.
- Memory and native-file storage objects implement the owned read-buffer API.
- Blocking storage read objects expose a blocking adapter for owned read
  completions.
- Existing borrowed blocking read paths remain unchanged for current table/blob
  decode code.
- Public async API, blocking API, publish barrier, commit tracker,
  WAL/table/blob/manifest formats, MVCC, compaction, recovery, cleanup, and
  storage behavior remain unchanged.
- Focused storage tests, formatting, clippy, full tests, and diff checks pass.

### Phase 82: Native-File Runtime-Owned Storage Reads

**Status**: Complete

**Goal**: Route native-file owned storage reads through the bounded runtime
blocking adapter when a runtime-enabled backend is used.

**Entry Condition**: Phase 81 complete and owned storage read completions exist.

**Acceptance Gate**:

- Runtime exposes a result-bearing bounded blocking future.
- Native-file backends can be constructed with a runtime boundary.
- Runtime-enabled native-file whole-object reads and owned read-buffer reads
  execute through the bounded blocking adapter.
- Inline/no-runtime storage reads remain immediately pollable.
- Existing borrowed blocking read paths remain unchanged for current table/blob
  decode code.
- Public async API, blocking API, publish barrier, commit tracker,
  WAL/table/blob/manifest formats, MVCC, compaction, recovery, cleanup, and
  storage behavior remain unchanged.
- Focused runtime/storage tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 83: Native-File Runtime-Owned Storage Mutations

**Status**: Complete

**Goal**: Route native-file owned storage writes, append operations, manifest
publish, and object listing through the bounded runtime blocking adapter when a
runtime-enabled backend is used.

**Entry Condition**: Phase 82 complete and native-file read operations can use
runtime-owned blocking work.

**Acceptance Gate**:

- Runtime-enabled native-file object writes/deletes execute through the bounded
  blocking adapter.
- Runtime-enabled native-file WAL rewrite, manifest read/publish, directory
  operations, and object listing execute through the bounded blocking adapter.
- Runtime-enabled native-file append-object open, append, and persist execute
  through the bounded blocking adapter.
- Blocking storage adapters remain direct synchronous paths.
- Inline/no-runtime storage operations remain immediately pollable.
- Public async API, blocking API, publish barrier, commit tracker,
  WAL/table/blob/manifest formats, MVCC, compaction, recovery, cleanup, and
  storage behavior remain unchanged.
- Focused storage tests, formatting, clippy, full tests, and diff checks pass.

### Phase 84: Persistent DB Runtime-Enabled Native Storage

**Status**: Complete

**Goal**: Attach persistent database construction and DB-owned storage helpers
to a runtime-enabled native-file backend while keeping existing blocking decode
paths explicit.

**Entry Condition**: Phase 83 complete and native-file owned operations can
execute through a runtime-enabled backend.

**Acceptance Gate**:

- Persistent `DbInner` owns a native-file backend constructed with the database
  runtime.
- Persistent manifest store creation and publish operations use the DB-owned
  native backend.
- Persistent WAL read/rewrite and append construction use the DB-owned native
  backend.
- DB-owned directory create/sync and cleanup deletes use the DB-owned native
  backend.
- Standalone table/blob/recovery helpers and borrowed blocking decode paths
  remain unchanged.
- Public async API, blocking API, publish barrier, commit tracker,
  WAL/table/blob/manifest formats, MVCC, compaction, recovery, cleanup, and
  storage behavior remain unchanged.
- Focused DB/storage tests, formatting, clippy, full tests, and diff checks
  pass.

### Phase 85: DB-Owned Table/Blob Native Storage Helpers

**Status**: Complete

**Goal**: Route persistent database table/blob helper calls through the
DB-owned native-file backend while preserving standalone helper behavior and
current decode semantics.

**Entry Condition**: Phase 84 complete and persistent `DbInner` owns a
runtime-enabled native-file backend.

**Acceptance Gate**:

- Table module exposes crate-internal backend-taking helpers for list, write,
  and read paths used by `Db`.
- Blob module exposes crate-internal backend-taking helpers for list, write,
  large-value rewrite, inline rewrite, metadata, and indexed value-read paths
  used by `Db`.
- Persistent `Db` flush, compaction, blob GC, open-time table load, stats, and
  blob candidate reads use the DB-owned native backend.
- Standalone table/blob wrappers still construct no-runtime native backends.
- Recovery scanning and borrowed block decode semantics remain unchanged.
- Public async API, blocking API, publish barrier, commit tracker,
  WAL/table/blob/manifest formats, MVCC, compaction, recovery, cleanup, and
  storage behavior remain unchanged.
- Focused DB/table/blob tests, formatting, clippy, full tests, and diff checks
  pass.

### Phase 86: Recovery Native Storage Backend Boundary

**Status**: Complete

**Goal**: Route persistent database recovery startup checks through the
DB-owned native-file backend while preserving standalone recovery helper
behavior and fail-closed semantics.

**Entry Condition**: Phase 85 complete and persistent table/blob helpers accept
explicit native storage backends.

**Acceptance Gate**:

- Recovery module exposes crate-internal backend-taking helpers for process
  lock acquisition, safe temporary file repair, referenced blob validation, and
  unreferenced formal file scanning.
- Blob module exposes a backend-taking full-file validation helper for recovery.
- Persistent `Db` open-time recovery checks use the DB-owned native backend.
- Standalone recovery wrappers still construct no-runtime native backends.
- Recovery report format, fail-closed behavior, storage formats, borrowed block
  decode semantics, public async API, blocking API, publish barrier, commit
  tracker, WAL/table/blob/manifest formats, MVCC, compaction, and cleanup
  behavior remain unchanged.
- Focused recovery tests, formatting, clippy, full tests, and diff checks pass.

### Phase 87: Owned Block-Read Seam For Decode

**Status**: Complete

**Goal**: Make table/blob block decode read through an owned, `Arc`-backed
completion (`StorageReadBuffer`) instead of a borrowed `&mut [u8]`, decoupling
read completion from decode as the precondition for a later async decode phase,
without changing scheduling for synchronous decode callers.

**Entry Condition**: Phase 86 complete and the owned storage read completion
(`read_exact_at_owned` / `StorageReadBuffer`) and bounded blocking adapter
already exist from Phases 81–86.

**Acceptance Gate**:

- `BlockReadSource` exposes `read_exact_at_owned` returning a
  `StorageReadBuffer`, with a borrowed fallback default for generic sources.
- `BlockManager::read_checked_from_source` and `read_checked_at_source_offset`
  read owned completions before decode.
- `StorageReadSource` and `NativeFileReadSource` override the owned read to use
  the storage object's owned blocking read; synchronous decode callers stay
  decoupled from the runtime blocking queue.
- Borrowed `read_exact_at` remains available for non-block (header/footer)
  reads.
- Storage formats, MVCC, recovery contract, public async/blocking API, publish
  barrier, commit tracker, and compaction behavior remain unchanged.
- Focused block tests, formatting, clippy, full tests, and diff checks pass.

### Phase 88: Measured Block-Decode Runtime Reads

**Status**: Complete

**Goal**: Measure table block-decode read cost under Trine runtime modes before
changing cursor advancement or decode scheduling.

**Entry Condition**: Phase 87 complete and table/blob block decode reads route
through the owned `StorageReadBuffer` seam while synchronous callers remain off
the runtime blocking queue.

**Acceptance Gate**:

- The v1 benchmark emits native-thread and inline runtime rows for
  cache-disabled persistent table point reads.
- The new benchmark rows assert that table data-block reads and disabled-cache
  misses occurred, so the timing row is tied to real decode work.
- Existing public async/blocking API, storage formats, MVCC, recovery contract,
  publish barrier, commit tracker, compaction behavior, and decode scheduling
  remain unchanged.
- Benchmark output, focused tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 89: Async Cursor Advancement Path

**Status**: Complete

**Goal**: Route async range and prefix iterator advancement through an internal
awaitable scan/source/table cursor path while preserving synchronous iterator
behavior.

**Entry Condition**: Phase 88 complete and benchmark evidence says the next
useful read-path slice is cursor advancement shape rather than routing current
synchronous block decode through the runtime queue.

**Acceptance Gate**:

- Async `next_async` for range and prefix lazy scans advances through internal
  async scan/source/table cursor methods.
- Synchronous `Iterator::next` remains unchanged.
- Persistent async range and prefix coverage proves async advancement works
  after records are flushed into table files.
- Storage formats, MVCC, recovery contract, public async/blocking API, publish
  barrier, commit tracker, compaction behavior, and synchronous decode
  scheduling remain unchanged.
- Focused async/table tests, formatting, clippy, full tests, and diff checks
  pass.

### Phase 90: Async Table Block-Load Completion

**Status**: Complete

**Goal**: Make async table cursor data-block loads await owned storage read
completion while preserving the synchronous iterator path.

**Entry Condition**: Phase 89 complete and async cursor advancement reaches a
dedicated table block-load hook.

**Acceptance Gate**:

- Data-block cache misses can be loaded through an async loader without holding
  block-cache locks across await.
- Async table cursor block loading uses `StorageReadObject::read_exact_at_owned`
  when a cached native table file is available.
- Synchronous table block loading and synchronous iterators remain unchanged.
- Storage formats, MVCC, recovery contract, public async/blocking API, publish
  barrier, commit tracker, and compaction behavior remain unchanged.
- Focused async/table/cache tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 91: Async Table Metadata Reads

**Status**: Complete

**Goal**: Route async table cursor metadata reads for block decisions and index
partition misses through awaited owned storage read completions.

**Entry Condition**: Phase 90 complete and async data-block body loads already
await owned read completion.

**Acceptance Gate**:

- Data-block metadata lookup has an async path for table cursors.
- Index partition cache misses can be loaded through an async cache loader.
- Async index partition reads use `StorageReadObject::read_exact_at_owned`
  when a cached native table file is available.
- Synchronous metadata reads and synchronous iterators remain unchanged.
- Storage formats, MVCC, recovery contract, public async/blocking API, publish
  barrier, commit tracker, and compaction behavior remain unchanged.
- Focused async/table/cache tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 92: Writer-Local Prepared Deltas

**Status**: Complete

**Goal**: Build immutable writer-local prepared commit data before entering the
publish barrier, without changing commit visibility, WAL format, or memtable
publication behavior.

**Entry Condition**: Phase 91 complete and the foreground write-path protocol
identifies immutable prepared commits and shard delta types as the next
post-tracker write-path slice.

**Acceptance Gate**:

- Accepted non-empty writes prepare bucket delta data before entering the
  publish barrier and remain invisible until publication.
- Prepared deltas preserve WAL operation order, batch indexes, touched bucket
  states, coarse key bounds, and estimated bytes.
- Publish-time transaction validation, sequence assignment, WAL append,
  memtable publication, visible marking, and post-commit freeze remain
  serialized by the named publish barrier.
- Public async/blocking API, storage formats, MVCC, recovery contract, commit
  tracker, compaction behavior, and WAL/table/blob/manifest formats remain
  unchanged.
- Focused commit/write/async tests, formatting, clippy, full tests, and diff
  checks pass.

### Phase 93: In-Memory Key-Sharded Delta Heads

**Status**: Complete

**Goal**: Publish in-memory writes into bucket-local key-sharded delta heads
and make read paths include those deltas while preserving existing public
behavior.

**Entry Condition**: Phase 92 complete and writer-local prepared commit data is
available before publish-time memtable mutation.

**Acceptance Gate**:

- In-memory writes publish immutable delta data into bucket-local key shards.
- Point reads, range/prefix scans, and transaction conflict checks include
  in-memory delta heads under existing MVCC and range tombstone rules.
- The current active-memtable publication path remains as a compatibility
  mirror for freeze/stats behavior in this phase.
- Public async/blocking API, storage formats, MVCC, recovery contract, commit
  tracker, compaction behavior, and WAL/table/blob/manifest formats remain
  unchanged.
- Focused commit/MVCC/iteration/transaction/async tests, formatting, clippy,
  full tests, and diff checks pass.

### Phase 94: Delta Epoch Merge Accounting

**Status**: Complete

**Goal**: Add bounded in-memory delta epoch sealing, merge, and accounting so
delta heads have a safe path toward replacing the active-memtable mirror.

**Entry Condition**: Phase 93 complete and evidence shows delta heads are
read-integrated but still unbounded without epoch merge behavior.

**Acceptance Gate**:

- Delta shards track open epoch bytes and chain length.
- A shard seals and merges an over-budget epoch into one immutable delta while
  keeping old snapshots safe through `Arc` ownership.
- Point reads, range/prefix scans, and transaction conflict checks keep seeing
  records and range tombstones across merged delta epochs.
- Active-memtable publication remains as the current compatibility mirror.
- Public async/blocking API, storage formats, MVCC, recovery contract, commit
  tracker, compaction behavior, and WAL/table/blob/manifest formats remain
  unchanged.
- Focused delta/MVCC/iteration/transaction/async tests, formatting, clippy,
  full tests, and diff checks pass.

### Phase 95: In-Memory Delta-Backed Writes

**Status**: Complete

**Goal**: Stop mirroring in-memory writes into the active memtable by making
delta heads carry in-memory write accounting and read visibility.

**Entry Condition**: Phase 94 complete and delta shards have epoch accounting
plus merge behavior.

**Acceptance Gate**:

- In-memory commits publish through delta heads without also mutating the active
  memtable.
- In-memory stats count delta bytes so recent writes remain visible in
  `DbStats::memtable_bytes`.
- Point reads, range/prefix scans, snapshots, and transaction conflict checks
  keep working without the active-memtable mirror.
- Persistent write behavior, WAL/table/blob/manifest formats, recovery,
  compaction, and public API shape remain unchanged.
- Focused in-memory/transaction/async/persistent-write-buffer tests,
  formatting, clippy, full tests, and diff checks pass.

### Phase 96: Delta Read Cost Measurement

**Status**: Complete

**Goal**: Measure the bounded read-path cost introduced by delta-backed
in-memory writes before selecting the next async/write-path implementation
slice.

**Entry Condition**: Phase 95 complete and in-memory writes no longer replay
through the active memtable.

**Acceptance Gate**:

- The v1 benchmark emits active-memtable and delta-backed rows for point and
  bounded range reads.
- The delta-backed rows assert they are reading recent in-memory write data
  without immutable memtables or table files.
- The active-memtable comparison rows assert they avoid freeze/flush and table
  reads.
- Benchmark output is recorded as evidence with a clear next recommendation.
- Public async/blocking API, storage formats, MVCC, recovery contract, commit
  tracker, compaction behavior, and WAL/table/blob/manifest formats remain
  unchanged.
- Focused benchmark build/run, formatting, clippy, full tests, diff checks, and
  forbidden-term scan pass.

### Phase 97: Delta Read Chain Budget

**Status**: Complete

**Goal**: Reduce the default in-memory delta read-chain cost exposed by Phase
96 without changing public API, storage formats, or persistent write behavior.

**Entry Condition**: Phase 96 benchmark evidence shows merged-delta reads are
bounded, while default in-memory rows remain slower because open epochs can
keep multiple deltas per shard.

**Acceptance Gate**:

- Default in-memory point and bounded range benchmark rows improve or remain
  acceptable relative to the Phase 96 measurement.
- Write-path benchmark rows do not regress enough to invalidate the change.
- Delta epoch merge tests still prove snapshot-safe point and range tombstone
  visibility.
- Public async/blocking API, storage formats, MVCC, recovery contract, commit
  tracker, compaction behavior, and persistent WAL/table/blob/manifest formats
  remain unchanged.
- Focused delta/in-memory/benchmark verification, formatting, clippy, full
  tests, diff checks, and forbidden-term scan pass.

### Phase 98: Persistent WAL Front-Door Staging

**Status**: Complete

**Goal**: Put persistent WAL append behind a named front-door boundary and
stage recovery/cancellation tests before later WAL shard work.

**Entry Condition**: Phase 97 complete and the async/write-path protocol says
WAL front-door work must wait for recovery and cancellation tests.

**Acceptance Gate**:

- Persistent commits append through a named WAL front-door boundary while still
  using one WAL lane and the existing WAL file format.
- The front-door boundary can accept a whole commit record, rewrite after a
  replay floor, and continue accepting appends.
- Recovery tests prove a WAL-accepted record can replay even when no in-memory
  publish happened before open.
- Persistent async cancellation tests prove unpolled writes leave no WAL record
  and polled accepted writes survive caller future drop plus reopen.
- Public async/blocking API, storage formats, MVCC, recovery contract,
  compaction behavior, and manifest/table/blob formats remain unchanged.
- Focused WAL/recovery/async tests, formatting, clippy, full tests, diff
  checks, and forbidden-term scan pass.

### Phase 99: Persistent WAL Preaccept

**Status**: Complete

**Goal**: Separate persistent blind-write WAL acceptance from the publish
barrier while keeping visibility, transaction validation, and recovery
semantics unchanged.

**Entry Condition**: Phase 98 complete and persistent commits route through the
named single-lane WAL front door.

**Acceptance Gate**:

- Persistent non-transaction writes can reserve a commit slot and accept the
  whole WAL record before entering the publish barrier.
- The accepted WAL record remains invisible to readers until writer-local state
  is published and the commit slot becomes visible.
- Transaction writes continue accepting WAL only after read-set validation.
- In-memory writes continue using the deferred no-WAL path.
- One WAL lane, WAL frame format, recovery contract, public async/blocking API,
  storage formats, MVCC, compaction behavior, and manifest/table/blob formats
  remain unchanged.
- Focused commit/WAL/recovery/async tests, formatting, clippy, full tests, diff
  checks, and forbidden-term scan pass.

### Phase 100: Visible-Sequence Completion

**Status**: Complete

**Goal**: Move normal commit slot visibility completion out of the publish
barrier while preserving data-publication ordering and reader-visible sequence
rules.

**Entry Condition**: Phase 99 complete and persistent blind writes can accept
WAL before waiting for publication.

**Acceptance Gate**:

- Writer-local publish returns the commit slot that must become visible.
- The public write path leaves the publish barrier before completing the commit
  slot in the normal success path.
- Completing a later slot before an earlier slot does not advance public
  visibility past the earlier open slot.
- Transaction validation, memtable publication, and persistent freeze remain
  serialized by the publish barrier.
- One WAL lane, WAL frame format, recovery contract, public async/blocking API,
  storage formats, MVCC, compaction behavior, and manifest/table/blob formats
  remain unchanged.
- Focused commit/WAL/recovery/async tests, formatting, clippy, full tests, diff
  checks, and forbidden-term scan pass.

### Phase 101: WAL Recovery Merge Boundary

**Status**: Complete

**Goal**: Stage the WAL recovery sequence-merge boundary for future WAL shard
replay while keeping current persistent databases on one WAL stream.

**Entry Condition**: Phase 100 complete and visible-sequence completion no
longer depends on the normal publish-barrier path.

**Acceptance Gate**:

- WAL batch-stream merge orders batches from multiple sources by commit
  sequence.
- WAL batch-stream merge rejects duplicate commit sequences across sources.
- WAL batch-stream merge rejects non-increasing sequences inside one source.
- Current persistent open still reads one WAL stream and replays through the
  merge boundary.
- One WAL lane, WAL file name, WAL frame format, recovery behavior, public
  async/blocking API, storage formats, MVCC, compaction behavior, and
  manifest/table/blob formats remain unchanged.
- Focused WAL/recovery tests, formatting, clippy, full tests, diff checks, and
  forbidden-term scan pass.

### Phase 102: WAL Shard Routing And Recovery

**Status**: Complete

**Goal**: Finish the WAL shard recovery and write-routing tail by making
persistent WAL front doors use multiple shard files with deterministic recovery
merge.

**Entry Condition**: Phase 101 complete and WAL recovery already has a tested
sequence merge helper.

**Acceptance Gate**:

- Legacy lane 0 remains `trine.wal`.
- Additional WAL lanes use stable shard file names.
- Persistent open discovers existing WAL shard files and replays all valid
  streams through sequence merge.
- Persistent writes route whole commit records across WAL shards.
- Malformed WAL shard file names fail closed during recovery.
- Flush WAL rewrite applies across opened or existing WAL shards after the
  replay floor advances.
- WAL shard rewrite temporary files keep shard-specific names and follow the
  safe temporary recovery policy.
- `DbStats` exposes commit tracker and WAL shard counters needed to diagnose
  the new path.
- Public async/blocking API, WAL frame format, storage formats, MVCC,
  transactions, compaction behavior, and manifest/table/blob formats remain
  unchanged.
- Focused WAL/recovery/async/commit tests, formatting, clippy, full tests, diff
  checks, and forbidden-term scan pass.

### Phase 103: Async Write-Path Tail Closure

**Status**: Complete

**Goal**: Close the remaining async/write-path tails that do not require a
platform-specific native async file-I/O backend.

**Entry Condition**: Phase 102 complete and WAL shard routing/recovery already
tested.

**Acceptance Gate**:

- WAL shard append/persist/rewrite commands run through bounded front-door lane
  workers.
- Transaction writes reserve sequence under the publish barrier but append WAL
  outside that barrier after validation succeeds.
- Memtable publication and post-commit freeze use a narrower memtable publish
  lock instead of the global publish barrier.
- Public flush freezes active memtables without allowing newer commit records
  into an older flush boundary.
- Table-open header/footer metadata reads use owned read buffers.
- Public async/blocking API, WAL frame format, storage formats, MVCC,
  compaction behavior, and manifest/table/blob formats remain unchanged.
- Focused WAL/commit/flush/table/persistent/async tests, formatting, clippy,
  full tests, diff checks, and forbidden-term scan pass.

**Major Out Of Scope**:

- Platform-native async file I/O. That remains a separate backend phase because
  it needs explicit platform support beyond the portable bounded blocking
  adapter.

### Phase 104: Async Storage Backend Honesty

**Status**: Complete

**Goal**: Close the remaining async storage boundary by making native-file
blocking-adapter behavior explicit and observable instead of implying true
platform async file I/O.

**Entry Condition**: Phase 103 complete and the only remaining async blocker is
the native-file backend's use of the portable blocking adapter.

**Acceptance Gate**:

- Storage capabilities distinguish `BlockingAdapter` from `PlatformAsyncIo`.
- Native-file backend reports `BlockingAdapter` only when a runtime blocking
  adapter is active.
- Native-file backend does not report `PlatformAsyncIo` without a real platform
  async file driver.
- `DbStats` exposes native-file adapter usage and task counters.
- The async storage protocol records the distinction.
- Focused storage/db tests, formatting, clippy, full tests, diff checks, and
  forbidden-term scan pass.

**Major Out Of Scope**:

- Adding an OS async file driver or new runtime dependency.

### Phase 105: IO Completion And Driver Boundary

**Status**: Complete

**Goal**: Introduce Trine's internal `io` completion and driver boundary before
adding platform-specific file I/O drivers.

**Entry Condition**: Phase 104 complete and backend capability reporting can
distinguish blocking adapters from platform async I/O.

**Acceptance Gate**:

- `src/io.rs` owns completion state, driver kind, driver submission, and I/O
  object traits.
- Native-file read, append, and persist paths submit through `io` completions
  on both inline and blocking-adapter drivers.
- Existing native-file capabilities and stats remain stable.
- No public API, storage format, WAL, MVCC, manifest, table, or compaction
  behavior changes.
- Focused storage tests, formatting, clippy, full tests, diff checks, and
  forbidden-term scan pass.

**Major Out Of Scope**:

- Adding Linux io_uring, Windows IOCP, kqueue, or another platform driver.

### Phase 106: Feature-Gated Platform I/O Driver

**Status**: Complete

**Goal**: Add an opt-in native platform I/O path below Trine's `io` completion
boundary.

**Entry Condition**: Phase 105 complete and native-file read/append/persist
paths submit through `io` completions.

**Acceptance Gate**:

- Cargo exposes a `platform-io` feature that pulls in an MSRV-compatible
  platform I/O dependency.
- `RuntimeOptions::platform_io()` selects a native-file platform I/O driver
  only when the feature is enabled.
- Native-file length, owned random reads, append, and persist can complete
  through the platform driver.
- Native-file capabilities and stats distinguish platform I/O tasks from
  blocking-adapter tasks.
- Default runtime behavior remains unchanged.
- Formatting, clippy, full tests, diff checks, and forbidden-term scan pass.

**Major Out Of Scope**:

- Making platform I/O the default runtime.
- Moving manifest publish, directory operations, object listing, writer lease,
  recovery scanning, and all remaining metadata operations to platform I/O.

### Phase 107: Platform I/O Storage Operation Coverage

**Status**: Complete

**Goal**: Move native-file storage operations that have platform support below
the opt-in `io` platform driver.

**Entry Condition**: Phase 106 complete and the remaining native-file operation
tails are known.

**Acceptance Gate**:

- Platform I/O builds route object read/write/delete, manifest read/publish,
  WAL rewrite, append-object opening, directory create/sync, and writer lease
  acquisition through `PlatformIoDriver`.
- Platform task stats cover the newly routed operations and bounded
  blocking-adapter stats stay separate.
- Default native-thread runtime, inline runtime, blocking APIs, public API,
  storage formats, WAL, MVCC, manifest, table, and compaction behavior remain
  unchanged.
- Formatting, clippy, full tests, diff checks, and forbidden-term scan pass.

**Major Out Of Scope**:

- Directory and object listing until the platform driver exposes directory
  enumeration.
- Making platform I/O the default runtime.
- Changing lease-drop cleanup.

### Phase 108: Platform Listing And Lease Cleanup Closure

**Status**: Complete

**Goal**: Close the listing and lease-drop tail without overstating platform
driver capabilities.

**Entry Condition**: Phase 107 complete and directory/object listing plus
lease-drop cleanup are the remaining native-file platform I/O tails.

**Acceptance Gate**:

- Platform I/O builds route async and blocking directory/object listing through
  `PlatformIoDriver`.
- Listing work is counted as platform blocking fallback, not true platform
  async I/O and not Trine bounded blocking-adapter work.
- Writer lease drop cleanup uses the platform driver after platform I/O
  acquisition.
- Recovery/open paths using blocking listing can use the same platform driver
  fallback when platform I/O is selected.
- Formatting, clippy, full tests, diff checks, and forbidden-term scan pass.

**Major Out Of Scope**:

- Claiming directory enumeration is true platform async I/O before the selected
  driver exposes a real operation.
- Making platform I/O the default runtime.

### Phase 109: IO Boundary Correction

**Status**: Complete

**Goal**: Correct the platform I/O architecture so Trine's `io` boundary is the
design subject and the selected native backend is only an implementation detail.

**Entry Condition**: Phase 108 complete, process guardrail added, and the
backend boundary receipt required before further backend work.

**Acceptance Gate**:

- `src/io.rs` expresses Trine-owned completion, driver info, driver submission,
  and operation routing without backend dependency references.
- Backend-specific native platform implementation lives below the `io` boundary
  in a feature-gated implementation module.
- Storage, stats, docs, current phase, roadmap, and protocol pass backend-name
  leakage checks outside dependency-selection evidence.
- Phase record contains the backend boundary receipt.
- Formatting, clippy, full tests, diff checks, forbidden-term scan, and
  backend-name leakage scan pass.

**Major Out Of Scope**:

- Replacing the selected backend dependency.
- Adding target-specific Linux, macOS/BSD, or Windows native backend
  implementations.
- Making platform I/O the default runtime.

### Phase 110: Native Backend Capability Matrix

**Status**: Complete

**Goal**: Record the native platform backend capability matrix at Trine's `io`
operation boundary before adding more platform behavior.

**Entry Condition**: Phase 109 complete and backend boundary receipt written.

**Acceptance Gate**:

- Linux, Windows, Unix fallback, and unsupported fallback target families have
  explicit operation classes.
- The matrix distinguishes true platform async, backend fallback, and blocking
  fallback.
- Directory enumeration is not classified as true platform async.

**Major Out Of Scope**:

- Replacing the selected backend dependency.
- Making platform I/O the default runtime.

### Phase 111: IO Backend Switch Layer

**Status**: Complete

**Goal**: Keep target-specific platform backend selection below Trine's `io`
boundary.

**Entry Condition**: Phase 110 matrix accepted.

**Acceptance Gate**:

- `src/io.rs` exposes Trine-owned driver metadata and operation classes.
- Target-specific backend modules live below the platform backend
  implementation boundary.
- Storage, stats, docs, protocol, and roadmap do not name backend dependency
  crates as the architecture subject.

**Major Out Of Scope**:

- Changing public API, storage format, WAL, MVCC, table, manifest, compaction,
  transaction, or recovery semantics.

### Phase 112: Linux Native Async Backend

**Status**: Complete

**Goal**: Enable Linux native async backend support through the `platform-io`
feature and classify supported Linux file operations honestly.

**Entry Condition**: Phase 111 switch layer complete.

**Acceptance Gate**:

- `platform-io` enables the selected backend's Linux native async feature.
- Linux regular-file operations covered by the backend matrix are classified as
  true platform async.
- Directory enumeration remains blocking fallback.

**Major Out Of Scope**:

- Hand-written Linux OS bindings.
- Making platform I/O the default runtime.

### Phase 113: Windows Backend Classification

**Status**: Complete

**Goal**: Classify Windows platform backend coverage without overstating
end-to-end IOCP coverage for Trine composite storage operations.

**Entry Condition**: Phase 111 switch layer complete.

**Acceptance Gate**:

- Windows read/write primitives are recorded as IOCP-capable evidence.
- Current Windows Trine composite storage operations are classified as backend
  fallback unless every step in the operation has a native async path.
- Windows metadata/open/sync/rename/directory/listing gaps are classified as
  backend fallback or blocking fallback.
- Stats can report fallback work separately from true platform async work.

**Major Out Of Scope**:

- Hand-written Windows OS bindings.

### Phase 114: macOS/BSD Backend Decision

**Status**: Complete

**Goal**: Record the macOS/BSD and other non-Linux Unix fallback decision.

**Entry Condition**: Phase 111 switch layer complete.

**Acceptance Gate**:

- Non-Linux Unix regular-file work is not claimed as true native async in this
  phase.
- Fallback-classified platform-driver work remains observable in stats.
- ADR/protocol wording captures the decision.

**Major Out Of Scope**:

- Claiming kqueue or polling makes ordinary file reads and writes true native
  async.
- Adding hand-written macOS or BSD backend code.

### Phase 115: Directory Enumeration Closure

**Status**: Complete

**Goal**: Close directory enumeration honestly as an explicit platform-driver
blocking fallback.

**Entry Condition**: Phase 110 matrix identifies listing as unsupported for
true platform async.

**Acceptance Gate**:

- Directory and object listing tasks are counted as platform blocking fallback.
- Blocking fallback is separate from true platform async work and separate from
  Trine's bounded blocking adapter.
- Focused platform storage tests assert fallback accounting.

**Major Out Of Scope**:

- Claiming directory enumeration is true platform async before a backend
  exposes a native async enumeration operation.

### Phase 116: Async Storage Final Gate

**Status**: Complete

**Goal**: Verify the async storage platform I/O closure across formatting,
linting, tests, naming, and protocol evidence.

**Entry Condition**: Phases 110 through 115 complete.

**Acceptance Gate**:

- Formatting, clippy, focused platform tests, full tests, diff checks,
  forbidden-term scan, project-name scan, and backend-name leakage scan pass.
- Current phase, roadmap, evidence, protocol, and ADR documents match the
  implemented boundary.
- Commit records why the change exists, what was verified, and what risks
  remain.

**Major Out Of Scope**:

- New platform behavior beyond the accepted matrix.

### Phase 117: True Async Capability Hardening

**Status**: Complete

**Goal**: Close the requested true-async gaps for directory enumeration,
Windows composite storage operations, and macOS/BSD/other Unix file work by
preventing false `PlatformAsyncIo` capability reporting.

**Entry Condition**: Phase 116 complete and the remaining gaps are known.

**Acceptance Gate**:

- `PlatformAsyncIo` is advertised only when the current target has at least one
  true Trine-level platform async storage operation.
- Non-Linux targets with the `platform-io` feature use the bounded blocking
  adapter instead of starting a platform driver whose current Trine operations
  are all fallback-classified.
- Directory enumeration remains explicit fallback and is not counted as true
  platform async work.
- Windows and non-Linux Unix matrix tests assert fallback classification for
  current Trine composite storage operations.
- Protocol, ADR, current phase, and usage docs record the capability rule.

**Major Out Of Scope**:

- Hand-written OS bindings or backend replacement for Linux directory
  enumeration, Windows end-to-end composite operations, dispatch I/O, POSIX AIO,
  kqueue, or other target-specific mechanisms.

### Phase 118: Async Host Boundary And Observability Closure

**Status**: Complete

**Goal**: Close the remaining async tail by making host persistent storage
selection explicit, exposing storage/runtime async observability, and recording
cooperative maintenance yields.

**Entry Condition**: Phase 117 complete and remaining async items are
WASI/browser persistence, observability, and cooperative maintenance.

**Acceptance Gate**:

- WASI and browser persistent modes are explicit public options and fail with
  `UnsupportedBackend` until real host adapters exist.
- `DbStats` reports blocking-adapter queue depth, task lifecycle counts, total
  adapter runtime, and per-storage-operation request/latency metrics.
- Cooperative maintenance yields and bounded-wait expirations are observable.
- Existing runtime/storage/backend capability behavior remains unchanged.
- Final verification gate passes.

**Major Out Of Scope**:

- Implementing real WASI or browser persistence.
- Resumable compaction work budgets.
- New OS bindings or backend replacement.

### Phase 119: WASI Persistent Backend

**Status**: Complete

**Goal**: Implement WASI persistent open against a host-preopened filesystem
path while keeping unsupported host capabilities explicit.

**Entry Condition**: Phase 118 complete and the next focused phase is real
WASI persistence.

**Acceptance Gate**:

- `DbOptions::wasi_persistent(path)` selects a path-carrying WASI host backend.
- On WASI targets, persistent open routes through the existing persistent
  engine against the host-preopened filesystem path.
- On non-WASI targets, the same option returns `UnsupportedBackend`.
- Strict sync durability returns `UnsupportedDurability` for WASI until host
  guarantees are proven.
- Browser persistence remains unsupported.
- Native and WASI target verification pass.

**Major Out Of Scope**:

- Browser persistence.
- WASI background workers.
- WASI strict sync durability guarantees.
- Resumable compaction work budgets.

### Recommended Next Action

### Phase 120: Resumable Maintenance Work Budgets

**Status**: Complete

**Goal**: Let hosts advance flush and compaction in bounded atomic maintenance
units that can yield and resume by replanning from current database state.

**Entry Condition**: Phase 119 complete and cooperative maintenance remains a
browser/WASM readiness blocker.

**Acceptance Gate**:

- Public `MaintenanceBudget` and `MaintenanceOutcome` types exist.
- `run_maintenance_with_budget` and `compact_range_with_budget` report progress,
  busy reservations, and budget exhaustion.
- Budgeted maintenance preserves existing `flush()` and `compact_range()`
  barrier behavior.
- Budget exhaustion is observable in stats.
- Focused tests prove budget exhaustion and resume-by-replanning behavior.
- Final verification gate passes.

**Major Out Of Scope**:

- Browser persistent storage.
- Async-only persistent engine conversion.
- Splitting one compaction publish across multiple manifests.

### Recommended Next Action

- Start a browser persistence phase by removing blocking persistent storage
  calls from the engine path before wiring IndexedDB/OPFS.
