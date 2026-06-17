# Branching & Time Travel

Status: **Slices 1–3 implemented** in `src/branch.rs` (no feature gate):
ephemeral instant clones + time travel (1); durable, writable named branches
whose divergent writes persist in their own buckets (2); and branch-aware
retention — a durable branch pins its fork with a checkpoint so the parent keeps
its fork history across restarts and aggressive GC, with `delete_branch`
releasing the pin (3); and branch-of-branch — the git-style DAG, where a branch
reads through its whole ancestor chain (4). `delete_branch` drops a branch's
divergent data buckets (via the new `Db::drop_bucket` / `drop_bucket_sync`,
which reclaim storage on every backend — in-memory, native, WASI, object store,
and browser), and `range` is a lazy merging iterator. This document specifies
copy-on-write
**branches** (Neon-/git-style: an O(1) fork off a version point, history shared
with the parent, divergent writes isolated) and read-only **time travel** (`AS
OF` a past version) for trine-kv.

These are **generic, domain-agnostic storage capabilities**, like the MVCC
`ReadVersion` they build on — they keep trine-kv a general-purpose KV (a more
capable one), and deliberately carry **no** application concepts (no SQL, no sync
CSN, no replica/changelog). trinedb projects its own meanings onto a branch (a
sync authority lineage, a SQL database, a dev/test clone); the KV only knows
"a versioned keyspace that can fork."

## Hard constraint: zero impact on the non-branch hot path

The single overriding requirement: **using branches must not slow down a
database that does not use them.** The default (root) lineage's read and write
code paths stay byte-for-byte as they are today. This is enforced two ways:

1. **Architecture (below):** a branch is a *separate* layer-set linked to its
   parent by `(ancestor, fork_sequence)`. The root lineage has no ancestor, so
   its per-key read loop and its write path are unchanged — branching adds no
   per-operation dispatch to root. Any fall-through to a parent is assembled
   **once when a branch read snapshot is opened**, not per key, and never runs
   for root.
2. **Gate:** `benches/v1_bench.rs` (criterion) is run before and after every
   branching change; a regression on the non-branch path blocks the change. New
   branch state hangs off `Option<…>`/separate maps that are empty/`None` for a
   database that never branches.

## Background: what trine-kv already gives us

trine-kv is an MVCC LSM. The pieces branching reuses already exist and are
public/stable:

- **`ReadVersion(u64)`** — a public, persistable cursor; the docs already frame
  it as "a public cursor, not a promise about internal commit machinery."
- **`CommitInfo { sequence }.read_version()`** — every commit returns the version
  it landed at (a versionstamp).
- **`Db::snapshot_at(ReadVersion) -> Snapshot`** — a repeatable read pinned to a
  past version, validated against the retained-history floor.
- **`Db::latest_read_version()` / `oldest_retained_read_version()`** — the head
  and the retention floor (the floor that a branch's fork point must pin).
- **`create_checkpoint_sync(name) -> ReadVersion`** — a named, read-only version
  anchor: the read-only precursor of a branch.

Internally: `DbInner.buckets: RwLock<BTreeMap<String, Arc<LsmTree>>>` maps a
bucket to its `LsmTree` (memtables + `current_version: Arc<LsmVersion>` layer
map); a read captures memtable + table sources and filters by `read_sequence`.

## The branch model (Neon timelines, applied to an MVCC LSM)

A **branch** (timeline) is:

```
Branch { id, ancestor: Option<(BranchId, Sequence /*fork*/)>, layer_set }
```

- **Root** is the branch with `ancestor = None`; its `layer_set` is exactly
  today's per-bucket `LsmTree`s. Nothing about root changes.
- A **child** forks at `fork_sequence` (a `ReadVersion`). It **shares all
  history ≤ fork** with its ancestor (no copy — O(1) create) and writes its own
  divergent versions into its **own** layer-set.
- A read on branch `B` at version `V` resolves a key by searching, in order: `B`'s
  own layer-set (versions in `(fork, V]`), then — assembled once at snapshot
  open — the ancestor's layer-set capped at `min(V, fork)`, recursing up the
  ancestor chain to root. For root the chain is length 1 = the current read.

Why separate layer-sets and **not** a `branch` dimension inside the internal key:
keying every record by `(user_key, branch, sequence)` would widen every key and
add a dimension to the comparator/seek on the **shared** hot path — taxing root.
Separate layer-sets keep root's keys and read loop untouched; only branch reads
pay the fall-through, and only at snapshot-assembly time.

The global `Sequence` counter stays **single and monotonic** across all branches,
so `ReadVersion` remains a globally meaningful coordinate and a fork point is just
a point on that timeline. Isolation comes from separate layer-sets, not from
separate sequence spaces.

## Slicing (each slice independently shippable and gated)

### Slice 1 — read-only instant clone + `AS OF` (done)

The first slice delivers **instant clones** and **time travel** by *composing
existing primitives*, so the engine core is untouched and the perf constraint is
satisfied trivially:

- `AS OF`: read through `snapshot_at(version)` — already supported; this slice
  just surfaces it as a first-class read handle and documents the retained-window
  limit (`oldest_retained_read_version`).
- **Instant clone**: a `Branch` opened at `fork = some ReadVersion` keeps that
  parent snapshot pinned and adds an **in-memory overlay** (a `Memtable`) for
  branch-local writes. A read is `overlay.get(k)` falling through to
  `parent.get_at(fork_snapshot, k)`; a scan merges the overlay over the parent
  snapshot. Branch-local writes are **ephemeral** (overlay only; not yet
  persisted, compacted, or recovered).
- Cost model: clone create is O(1) (pin a snapshot + allocate an empty overlay);
  the parent's retained floor is pinned at `fork` while the branch lives. **No
  change to root read/write code** — branching is a composition layer over
  `snapshot_at`.

This proves the fall-through + the zero-root-overhead claim and ships a real,
useful feature (dev/test clones, point-in-time reads) before the hard durable
machinery.

### Slice 2 — durable, writable branches (done)

A branch's divergent writes need their own persisted, separately-compacted
layer-set. Rather than grow the manifest with a new branch/WAL schema (a large,
recovery-sensitive engine change), slice 2 reuses what the KV already does per
**bucket** — every named bucket is its own `LsmTree` with its own WAL, flush,
compaction, and recovery. So a durable branch stores its writes in its **own
reserved buckets**, one per user bucket it diverges in
(`\u{1}trine-branch\u{1}<branch>\u{1}<user_bucket>`), and its metadata in a
reserved registry bucket (branch name → `(fork ReadVersion, written buckets)`).
This is the same composition philosophy as slice 1: **no manifest/WAL/flush/
recovery/compaction change, no hot-path change** — durability, compaction, and
recovery come for free, and because a branch's data lives in its own buckets it
never enters the parent's trees (no compaction/read-amp coupling — perf stays
isolated). A branch delete value is tombstone-tagged so it hides a parent key
without a parent write; reads check the branch bucket, decode present/tombstone,
else fall through to the pinned parent snapshot. `Db::create_branch` /
`open_branch` / `list_branches` manage them.

Known limit (closed by slice 3): a durable branch pins its fork only while a
handle is open. Across a restart with no configured retention, the parent's fork
history may have been GC'd, so the fall-through read fails until the fork is
re-pinned — today the caller must keep enough retention
(`with_keep_last_read_versions`) to cover live forks. Deferred to later slices:
`delete_branch` (needs a bucket-drop primitive) and a branch of a branch (the
registry already carries the fork; the read path would walk the ancestor chain).

### Slice 3 — branch-aware retention (done)

The parent must keep the history a branch reads through (everything at or below
its fork), or a durable branch breaks across a restart or under aggressive GC.
Again this is achieved by composition rather than a new GC subsystem: the
retained-history floor already drops to the **oldest checkpoint**
(`oldest_retained_sequence = min(active snapshots, configured window, oldest
checkpoint)`), and checkpoints are durable manifest metadata. So a durable
branch **pins its fork with a checkpoint** at create time; the parent's GC then
cannot reclaim that history while the branch lives, even across restarts and
with `keep_last_read_versions = 1`. `delete_branch` deletes the checkpoint,
releasing the pin so the parent can GC again. The one engine addition is a small
generic primitive, `Db::create_checkpoint_at_sync(name, version)` — checkpoint a
specific retained past version, not just the latest (the underlying manifest
already accepted an arbitrary sequence) — which is useful on its own (anchor any
PITR point) and keeps branching a composition layer with no hot-path change.

This realizes the "min over the live set" retain-set (Neon's `retain_lsn`)
without a bespoke GC pass: each live branch contributes one checkpoint, and the
floor is already their minimum. Per-branch compaction is automatic because each
branch's data is its own buckets (slice 2).

### Slice 4 — nesting (done) & lifecycle completion (partial)

**Branch of a branch (done):** the registry entry records a `parent` (None = the
root lineage), and `Db::create_branch_from(name, parent)` forks an existing
branch at the current version. `open_branch` assembles a **read chain** — the
branch (read at its own latest), then each ancestor branch read frozen at the
version its child forked it, then the root snapshot at the base fork. A `get`
walks the chain leaf-first (first present value or tombstone wins); a `range`
merges the chain root-first so the leaf wins. Each level's fork is pinned by its
own checkpoint, so the whole chain stays readable; `delete_branch` refuses while
a branch still has children (a child reads through the parent's pinned history).

**Bucket-drop (done, all backends):** `Db::drop_bucket_sync` (in-memory, native,
and WASI — WASI is the same native file machinery over a preopened path) and the
async `Db::drop_bucket` (adds object store and browser).

- **Native / WASI**: remove the bucket from the registry and the manifest, retire
  its SSTable files and mark its blob files for deletion. File deletion is
  **refcount-guarded** (`Arc::strong_count == 1`), so a reader still holding the
  bucket's tables keeps working and the files are freed only once no reference
  remains; a `flush_sync` first advances the WAL replay floor so recovery never
  replays into a dropped bucket.
- **Object store**: remove the bucket from the manifest via a CAS publish, making
  its table and blob objects unreferenced, then reclaim them with the existing
  snapshot-safe **orphan GC** (`cleanup_object_store_orphans_async`).
- **Browser**: publish the removal to the IndexedDB-backed manifest (marking its
  blobs), then retire its table and blob files through the browser async cleanup.

Verified by run tests on native, in-memory, and object-store (`InMemoryObjectStore`);
the WASI and browser paths are compile-checked on `wasm32-wasip1` and
`wasm32-unknown-unknown` (no browser/WASI runtime in this repo's test env).
`delete_branch` uses the sync drop to remove a deleted branch's data buckets
outright (it falls back to clearing only where a sync drop is unavailable, e.g.
the object-store backend, which needs the async drop).

**`range` is lazy (done):** a [`BranchRange`] k-way-merges the chain's sorted
scans on the fly instead of materializing a map.

**Remaining:** merge/reset semantics stay at the
application layer (trinedb decides merge policy; the KV exposes the version
primitives).

## trinedb projection (out of scope here, for context)

- A trinedb **branch** = a trine-kv branch; a SQL session opens against a branch.
- The **sync authority lineage / CSN** is **per-branch**: each branch is its own
  write-order authority, so "fork a dev branch off main `AS OF` v, sync it
  independently, then discard or promote" falls out naturally.
- `head`, `AS OF`, and a branch fork point are all the **same `ReadVersion`
  coordinate** viewed three ways — trinedb holds one coordinate system over the
  KV.
