# LSM Core Boundary Specification

Date: 2026-05-26
Status: Accepted for phased internal refactoring

## 1. Purpose

This specification defines the internal boundary between the database layer and
the LSM core.

The goal is to make one LSM tree a clear data structure with its own read,
flush, compaction, tombstone, and MVCC rules. The database layer should
coordinate database-wide concerns such as public handles, WAL, manifest publish,
process locking, recovery, background worker lifecycle, and cross-keyspace
atomicity.

This is an internal refactoring contract. It must not change:

- public API behavior;
- on-disk file formats;
- WAL format;
- manifest format;
- SSTable format;
- MVCC visibility rules;
- keyspace semantics;
- in-memory mode behavior.

## 2. Problem Statement

The current implementation has grown the LSM tree inside the database module.
That makes `db.rs` responsible for unrelated layers at the same time:

- public database API;
- keyspace registration;
- WAL and manifest coordination;
- active and immutable memtables;
- table lists and level decisions;
- point reads;
- range and prefix scans;
- range tombstone lookup;
- transaction conflict checks;
- flush input selection;
- compaction planning, merge, retention, and output splitting;
- background maintenance dispatch.

That shape makes future changes risky because core tree rules can be duplicated
in point read, scan, transaction validation, and compaction paths.

The LSM core boundary exists to make one rule live in one place.

## 3. Core Rule

The database layer owns a set of named trees.

Each LSM tree owns the data-structure rules for exactly one keyspace:

```text
Db
  -> keyspace map
    -> LsmTree
```

The database layer may choose when a write, flush, or compaction runs. The LSM
core decides how one tree reads, freezes, flushes, scans, and compacts its own
data.

## 4. Database Layer Responsibilities

The database layer remains responsible for:

- public `Db`, `Keyspace`, `Snapshot`, and `Transaction` handles;
- storage mode selection;
- process lock acquisition and release;
- durable WAL append, flush, sync, replay, and rewrite;
- manifest loading and atomic publish;
- keyspace name and option registration;
- cross-keyspace write batch sequencing;
- assigning commit sequences;
- snapshot tracker ownership;
- oldest active snapshot sequence discovery;
- background worker startup, wakeup, shutdown, and error surfacing;
- recovery policy and repair reports;
- filesystem path allocation for persistent table, blob, WAL, and manifest
  files;
- user-facing stats aggregation;
- public error shape.

The database layer must not decide:

- which internal key version is visible for a point read;
- which range tombstone hides a point record;
- how a scan groups records for one user key;
- which point versions compaction keeps for one tree;
- which range tombstones compaction keeps or clips for one tree;
- how table and memtable cursors are merged for one tree.

## 5. LSM Core Responsibilities

One `LsmTree` owns:

- `KeyspaceOptions`;
- active memtable;
- immutable memtable queue;
- table readers for the current tree version;
- level layout for the tree;
- table metadata needed by reads and compaction;
- range tombstone query structures;
- point read visibility;
- range scan and prefix scan visibility;
- transaction conflict checks for this tree;
- freeze decisions driven by configured write-buffer thresholds;
- flush input selection for this tree;
- compaction input planning for this tree;
- compaction merge and MVCC retention for this tree;
- table output splitting by `target_table_bytes`;
- tree-local stats.

The LSM core may use shared services supplied by the database layer:

- block cache handle;
- table file allocator;
- blob file allocator;
- clock or sequence inputs;
- storage paths;
- manifest publish callback or returned tree edits.

The LSM core must not:

- append WAL records;
- publish manifest edits directly;
- acquire or release process locks;
- spawn background worker threads;
- assign global commit sequences;
- make cross-keyspace atomicity decisions;
- expose public crate API types unless they are already part of the v1 API.

## 6. Target Module Shape

The phased target is:

```text
src/lsm/
  mod.rs
  tree.rs
  version.rs
  read.rs
  scan.rs
  write.rs
  flush.rs
  compact.rs
  tombstone.rs
```

Initial extraction may keep existing modules such as `table`, `iterator`,
`filter`, `search`, `memtable`, and `range_tombstone` at crate level. Moving a
file under `lsm/` is optional unless it improves ownership.

The important boundary is not directory layout. The important boundary is that
`Db` calls tree operations instead of reimplementing tree rules.

## 7. Core Types

The target public-to-crate boundary should include types shaped like these:

```rust
pub(crate) struct LsmTree;
pub(crate) struct LsmReadContext;
pub(crate) struct LsmRetentionContext;
pub(crate) struct LsmWriteBatch;
pub(crate) struct FlushPlan;
pub(crate) struct FlushOutput;
pub(crate) struct CompactionPlan;
pub(crate) struct CompactionOutput;
pub(crate) struct TreeVersion;
pub(crate) enum TreeEdit;
```

The exact names may evolve during implementation, but the ownership must remain
the same:

- `LsmReadContext` carries `read_sequence` and any read-scoped guards needed by
  the tree.
- `LsmRetentionContext` carries `oldest_snapshot_sequence` and compaction scope
  facts.
- `TreeVersion` represents the current table layout for one tree.
- `TreeEdit` describes table and blob changes that the database layer can
  publish atomically through the manifest.

## 8. MVCC Belongs In LSM Core

MVCC visibility is a tree rule.

The LSM core must own:

- internal key ordering;
- newest visible version selection;
- point tombstone handling;
- range tombstone handling;
- scan grouping by user key;
- compaction retention by `oldest_snapshot_sequence`;
- point tombstone cleanup after older records are gone;
- range tombstone cleanup after covered older records are gone.

The database layer supplies:

- `read_sequence` for reads;
- `oldest_snapshot_sequence` for compaction;
- commit sequence for writes.

The database layer must not inspect internal key sequences to decide read
visibility after the extraction is complete.

## 9. Read Path Contract

The LSM core exposes tree-local read operations:

```rust
LsmTree::get(read_context, user_key) -> Result<Option<Value>>
LsmTree::range(read_context, selector) -> Result<Iter>
LsmTree::prefix(read_context, prefix) -> Result<Iter>
```

Contract:

- point read checks active memtable, immutable memtables, and tables in the
  correct freshness order;
- point read asks only candidate range tombstones that can cover the key;
- point read returns the newest visible live value or missing;
- range and prefix scans merge source cursors lazily;
- range and prefix scans return each user key at most once;
- scans preserve forward and reverse ordering;
- prefix filters only skip candidate table or block reads;
- block reads remain on demand and verified before decoded records affect a
  result.

`Db` and `Keyspace` may convert the returned value representation to public
owned values. They must not redo MVCC visibility.

## 10. Write Path Contract

The database layer assigns one commit sequence after WAL append succeeds. It
then passes per-tree operations into each affected `LsmTree`.

The LSM core:

- applies puts and point deletes to the active memtable;
- applies range deletes to the tree tombstone structure;
- preserves batch-local duplicate-key order through internal key rules;
- decides whether the active memtable should freeze after the batch;
- reports whether maintenance should be requested.

The database layer:

- keeps cross-keyspace batch commit serialized;
- keeps WAL and manifest ordering correct;
- surfaces write errors through public APIs.

## 11. Freeze And Flush Contract

Freezing is a tree-local data-structure operation.

Flush output publish is a database-layer durability operation.

Flow:

```text
Db writer coordinator
  -> LsmTree::freeze_if_needed()
  -> LsmTree::prepare_flush()
  -> write SSTable files or memory tables
  -> Db publishes manifest edit
  -> LsmTree::install_flush()
```

Rules:

- active memtable and active range tombstones freeze as one unit;
- immutable memtables flush in sequence order;
- a failed table write does not change the live tree version;
- a failed manifest publish does not install flush output;
- WAL replay floor advances only after publish succeeds;
- in-memory mode follows the same logical tree transition without filesystem
  durability.

## 12. Compaction Contract

Compaction selection and merge are tree-local rules.

The database layer may decide when to ask for compaction. The LSM core chooses
the actual tree inputs using configured pressure and key range rules.

Flow:

```text
Db asks tree for compaction
  -> LsmTree::plan_compaction(range, retention_context)
  -> LsmTree::build_compaction_outputs(plan)
  -> Db publishes manifest edit
  -> LsmTree::install_compaction()
```

Rules:

- L0 compaction groups overlapping L0 tables and overlapping L1 tables;
- L1 and deeper compaction uses level-size pressure;
- deeper levels remain non-overlapping within one tree;
- merge reads table cursors by user key;
- output SSTables split at user-key boundaries by `target_table_bytes`;
- compaction keeps versions needed by active snapshots;
- compaction keeps point and range tombstones needed to hide older data;
- range tombstones may be clipped only when the compaction scope proves older
  covered data outside the output span is gone;
- partial compaction retains original range tombstone bounds when needed.

## 13. Transaction Conflict Contract

Transaction validation asks each touched `LsmTree` whether a key or range was
modified after the transaction read sequence.

The LSM core owns:

- point-key modified-after checks;
- range modified-after checks;
- range tombstone conflict checks;
- table and memtable source ordering for conflict checks.

The database layer owns:

- transaction lifecycle;
- read-set and write-set storage;
- cross-keyspace validation order;
- final commit sequencing.

## 14. Caching Boundary

Caches are shared services, not correctness dependencies.

The database layer may own cache budgets and create cache handles. Each
`LsmTree` receives the cache handles needed by its readers.

Rules:

- block cache entries are keyed by table id and block offset or equivalent
  stable block identity;
- table cache entries are keyed by table id and storage generation;
- prefix and point Bloom filters remain tied to table metadata;
- clearing a cache cannot change read results;
- value guards may keep cached blocks alive while public values are read.

## 15. Concurrency And Lock Ordering

`LsmTree` must be `Send + Sync` through its owned state and shared references.

Reads:

- do not acquire the database writer coordinator;
- may take tree read locks briefly to clone `Arc` handles;
- must not hold tree metadata locks while doing slow table block I/O when a
  cloned handle is enough.

Writes and maintenance:

- database writer coordinator serializes commits and install steps;
- tree metadata updates happen under tree-local locks;
- table building may run outside tree metadata locks when it uses frozen input
  handles;
- background workers live in the database layer and call tree operations.

Required lock order:

```text
database writer coordinator
  -> tree metadata locks
    -> table reader internal locks
      -> cache locks
```

Code comments must document any function that intentionally relies on this
order.

## 16. Stats Boundary

`LsmTree` reports tree-local stats:

- memtable bytes;
- immutable memtable count;
- per-level table count and bytes;
- L0 table count;
- tombstone counts;
- compaction input and output bytes for this tree;
- table and block read counters where practical.

The database layer aggregates:

- keyspace count;
- active snapshot count;
- WAL bytes;
- recovery stats;
- cross-tree totals;
- background maintenance error state.

## 17. Phased Migration Plan

The extraction must be incremental and testable.

### Step 1: Spec And Boundary Guards

- add this protocol spec;
- update current phase and roadmap;
- keep public API and storage format unchanged.

### Step 2: Create LSM Module And Move Tree State

- introduce `src/lsm/`;
- move `KeyspaceState`, immutable memtable state, and tree-local stats behind
  `LsmTree`;
- keep behavior unchanged;
- keep `Db` as the owner of the keyspace map.

### Step 3: Move Point Read Into LSM Core

- move point candidate collection and MVCC visible-version selection into
  `LsmTree`;
- `Db::get_at` becomes lookup of the tree plus a call into core;
- add tests that fail if DB-level point read bypasses tree visibility helpers.

### Step 4: Move Range And Prefix Scans Into LSM Core

- move scan setup, source creation, range tombstone collection, and prefix
  filter selection into `LsmTree`;
- preserve lazy heap merge behavior;
- keep public iterator API unchanged.

### Step 5: Move Freeze And Flush Planning Into LSM Core

- tree prepares flush inputs and build results;
- database layer writes and publishes durable edits;
- tree installs results only after publish succeeds.

### Step 6: Move Compaction Planning And Merge Into LSM Core

- move compaction input selection, merge, retention, and output splitting into
  `LsmTree`;
- database layer still publishes manifest edits and cleans obsolete files after
  snapshot safety.

### Step 7: Move Transaction Conflict Checks Into LSM Core

- transaction validation calls tree-local modified-after APIs;
- DB layer keeps transaction lifecycle and cross-keyspace ordering.

## 18. Acceptance Gate

The LSM core separation is complete when:

- `Db` owns database-wide coordination but no longer implements tree MVCC read
  visibility;
- `Db` no longer directly scans memtables or tables for point reads;
- `Db` no longer builds range or prefix scan sources directly;
- `Db` no longer owns compaction retention helpers for point or range
  tombstones;
- `LsmTree` owns active and immutable memtables for one keyspace;
- `LsmTree` owns tree table layout and level version logic;
- `LsmTree` owns point read, range scan, prefix scan, and conflict checks;
- `LsmTree` owns flush planning and compaction planning;
- manifest publish and WAL replay remain database-layer responsibilities;
- in-memory mode still uses the same LSM core;
- public API tests pass unchanged;
- persistent recovery tests pass unchanged;
- full local Rust verification passes.

## 19. Refactoring Safety Rules

- Do not change storage formats during the extraction.
- Do not change public API during the extraction.
- Do not move WAL or manifest publish into the LSM core.
- Do not create a second in-memory-only engine.
- Do not duplicate MVCC visibility helpers in DB code.
- Prefer moving one behavior at a time behind `LsmTree` over moving files first.
- Every step must keep tests passing before the next step starts.
- Add comments where lock order, install-after-publish, or MVCC retention is
  non-obvious.

