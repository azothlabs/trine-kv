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
- Usage docs cover in-memory and persistent open, keyspaces, reads/writes,
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
- `KeyspaceOptions::block_bytes` controls data block sizing.
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

**Goal**: Separate one-keyspace LSM tree rules from database-wide coordination
without changing public API behavior or storage formats.

**Entry Condition**: Phase 20 complete and user identifies DB/LSM coupling as
the next maintainability and correctness risk.

**Acceptance Gate**:

- The LSM core boundary spec is written and linked from the v1 protocol.
- `Db` remains responsible for WAL, manifest publish, process lock, recovery,
  background worker lifecycle, snapshots, transactions, and cross-keyspace
  atomicity.
- `LsmTree` owns active and immutable memtables, table layout, tree-local reads,
  range tombstones, flush planning, compaction planning, and MVCC retention for
  one keyspace as the extraction progresses.
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

**Goal**: Harden memtable accounting, keyspace-local freeze behavior, and
immutable queue pressure before deeper table and compaction optimizations.

**Entry Condition**: Phase 22 complete and user review identifies P3 as the
next LSM tree improvement after versioned level layout.

**Acceptance Gate**:

- Memtable byte accounting no longer needs whole-map scans on normal writes.
- Freeze/flush pressure is tree-local and does not move unrelated keyspaces.
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
