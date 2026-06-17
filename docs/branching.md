# Branching & Time Travel

Status: **Slice 1 implemented** (read-only instant clones + time travel + an
in-memory write overlay, behind no feature gate, in `src/branch.rs`); slices 2–4
designed. This document specifies copy-on-write
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

### Slice 2 — durable, writable branches

Give a branch its own persisted layer-set: a branch-scoped WAL stream, flush, and
manifest records (`ManifestState` grows a branch registry: `BranchId →
(ancestor, fork_sequence)` plus per-branch table lists). Recovery reconstructs
every branch from the manifest + WALs. Root's WAL/flush/manifest entries are a
branch-registry of one and behave as today.

### Slice 3 — branch-aware retention & compaction

The retained-history floor becomes the **min over the live set**: `min(PITR
window, every child branch's fork_sequence)` — a parent cannot GC history any
child still forks from (Neon's `retain_lsn` set). Compaction runs per layer-set;
shared ancestor layers compact under the pinned floor.

### Slice 4 — branch lifecycle

Branch delete (drop a layer-set, release its fork pin so the parent floor can
advance) and the semantics of merge/reset (likely left to the application layer;
the KV exposes the version primitives, trinedb decides merge policy).

## trinedb projection (out of scope here, for context)

- A trinedb **branch** = a trine-kv branch; a SQL session opens against a branch.
- The **sync authority lineage / CSN** is **per-branch**: each branch is its own
  write-order authority, so "fork a dev branch off main `AS OF` v, sync it
  independently, then discard or promote" falls out naturally.
- `head`, `AS OF`, and a branch fork point are all the **same `ReadVersion`
  coordinate** viewed three ways — trinedb holds one coordinate system over the
  KV.
