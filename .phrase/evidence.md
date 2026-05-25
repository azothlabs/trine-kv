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
- A compact zlib-style codec is still useful for workloads that value space over
  CPU.

### Interpretation

- Prefix extractor and prefix filter support must be part of v1 table format,
  keyspace options, tests, and metrics.
- Trine should default to a fast block codec implemented with `lz4_flex`, while
  also supporting a compact zlib/DEFLATE codec implemented with `flate2`.
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
