# Storage Substrate Seam

Status: **Design (probe-driven).** This note records what an architecture probe
of the open / commit / flush paths found, and redefines how the object-storage
backend (see `object-storage-backend.md`) should plug in. It supersedes the
naive "thread one backend enum through every helper" approach, which the probe
showed to be both large *and* the wrong abstraction.

## Why the naive approach is wrong

The fine-grained `Storage*Backend` traits are good, but the **layers above them**
(WAL, recovery/bootstrap, the open/commit orchestration) are written
concretely against `NativeFileBackend` *and* encode **filesystem semantics that
object storage cannot provide**:

| Filesystem assumption | Used for | Object storage reality |
|---|---|---|
| Atomic `rename` | manifest publish (temp+rename), WAL rewrite | no rename; PUT new key + **conditional-write CAS** |
| Appendable file + cheap fsync | WAL grows via append; ~ms durability | objects immutable, **no append**; PUT ~tens of ms; segment / group-commit |
| Directory listing + per-file ops | recovery: scan dir, delete orphan temp, replay WAL files | prefix listing; **orphan-object GC** |
| Local lock file (flock) | single-writer process lock | **lease object + TTL + fencing token** |

Genericizing those helpers to `<B>` would *compile* for an object store but be
**semantically wrong** (it would still assume rename, append, flock). So the
seam must be drawn where semantics actually diverge — not at every function.

## What the probe found: divergence is narrow

Tracing `open_persistent_with_options`, the commit path (`db/commit.rs`), and
the memtable-flush path, the operations split cleanly into three bands:

**Band 1 — backend-agnostic LSM core (fully portable, no change):** memtable,
SSTable/block format, compaction, MVCC, the manifest *state* logic, iterators,
`wal::merge_batch_streams_by_sequence` (pure merge-by-sequence). These operate
on bytes + manifest state and do not care where bytes live.

**Band 2 — byte-level IO (portable; needs only the fine-grained traits):**
`table.rs` / `blob.rs` read/write of SSTable and blob bytes, block reads,
manifest byte read. These are ~30 functions currently typed `&NativeFileBackend`
but are semantically just "put/get bytes of object X". The primitives they need
are *already* trait methods (`read_object_bytes`, `write_object`, `open_read`,
`list_objects`, …). They can take `&StorageBackend` (the dispatch enum already
committed) unchanged in meaning.

**Band 3 — durability substrate (genuinely divergent; needs two impls):** the
probe localized *all* the real divergence to **two-and-a-half** places:

1. **WAL lifecycle (hard divergence).** `WalFrontDoor` is "one append-growing
   file per shard" and holds a `NativeFileBackend`; object storage has no
   `Append` capability. The object-store substrate must either segment the WAL
   into objects or drop the WAL entirely and rely on frequent memtable→SSTable
   flush + manifest CAS (the object-store-LSM "WAL-less" lineage). Either way the
   whole WAL write/replay path forks.
2. **Manifest publish atomicity + conflict (hard divergence).** Filesystem:
   write temp + `rename`. Object store: conditional-PUT CAS. Critically, the
   current `publish_manifest` trait returns `Result<()>` — **no conflict
   signal** — but the object store's optimistic concurrency needs publish to
   report "lost the CAS → retry". The publish operation needs a conflict-aware
   result.
3. **Bootstrap temp-file semantics (half divergence).** `repair_safe_temporary_files`
   assumes write-temp-then-rename orphans; `list`/`delete` primitives are
   trait-expressible, but "what is a temp / orphan" needs per-substrate logic.

Everything else is Band 1/2 (portable). **The divergence does not sprawl across
the ~50 helpers — it concentrates in WAL + manifest-publish-conflict +
bootstrap.** That is what makes a clean seam possible.

## The two-seam architecture

```
┌──────────────────────────────────────────────────────────┐
│ Band 1: LSM core (backend-agnostic, unchanged)             │
│   memtable · SSTable/block · compaction · MVCC ·           │
│   manifest state · iterators · WAL merge-by-sequence       │
└───────────────┬───────────────────────┬────────────────────┘
   Band 2: byte │ IO                     │ Band 3: durability substrate
┌───────────────▼──────────┐   ┌─────────▼──────────────────────────┐
│ trait-level byte IO        │   │ trait DurabilitySubstrate           │
│ via StorageBackend enum    │   │   bootstrap: create/list/lease/     │
│ (committed): SSTable/blob/  │   │              recover                 │
│ block/manifest-bytes        │   │   wal:  open/append/replay/truncate │
│ read & write                │   │   manifest: publish(state)          │
│                             │   │            -> Published | Conflict   │
│ NativeFileBackend           │   │   ├─ FilesystemSubstrate (rename,    │
│ ObjectStoreBackend (2b)     │   │   │   append-file, dir-scan, flock)  │
└─────────────────────────────┘   │   └─ ObjectStoreSubstrate (CAS,     │
                                   │       segment/WAL-less, GC, lease)   │
                                   └──────────────────────────────────────┘
```

- **Lower seam (Band 2)** = the already-committed `StorageBackend` enum. Flip the
  byte-level `table.rs`/`blob.rs` helpers to accept `&StorageBackend` (or make
  them generic over the byte traits). Mechanical, no semantic risk; tests that
  pass `&NativeFileBackend` keep working if we go generic, or wrap in
  `StorageBackend::Native(..)` if we go enum.
- **Upper seam (Band 3)** = a new, **narrow** `DurabilitySubstrate` trait whose
  methods are exactly the divergent operations the open/commit/flush
  orchestration calls. Two implementations; the LSM core and `DbInner` talk to
  the trait, not to `WalFrontDoor`/`ManifestStore`/`NativeFileBackend` directly.

### Proposed `DurabilitySubstrate` surface (sketch)

```rust
/// Backend-specific durability + bootstrap. The LSM core is written against
/// this; one impl per storage substrate. Byte-level SSTable/blob IO is NOT
/// here — that stays on the fine-grained StorageBackend traits (Band 2).
trait DurabilitySubstrate {
    // --- bootstrap / recovery (open path) ---
    fn open(location: &Location, opts: &OpenOptions) -> Result<Self>;
    fn acquire_writer_lease(&self) -> Result<WriterLease>;   // flock | lease+fence
    fn recover(&self) -> Result<RecoveredState>;             // dir-scan+temp-gc | list+orphan-gc
    fn load_manifest(&self) -> Result<ManifestState>;

    // --- WAL (commit path) ---
    fn append_wal(&self, batch: &WalBatch, durability: DurabilityMode) -> Result<()>;
    fn replay_wal(&self, after: Sequence) -> Result<Vec<WalBatch>>;
    fn truncate_wal(&self, up_to: Sequence) -> Result<()>;
    // (ObjectStoreSubstrate may implement these as segment objects, or as a
    //  no-op WAL-less strategy where flush+manifest-CAS is the durability point.)

    // --- manifest publish (flush/compaction path) — the atomic point ---
    fn publish_manifest(&self, next: &ManifestState) -> Result<PublishOutcome>;
    // PublishOutcome::{ Published, Conflict { current } }  <-- conflict-aware,
    // unlike today's publish_manifest() -> Result<()>.

    // --- table/blob object lifecycle that isn't pure byte IO ---
    fn remove_objects(&self, ids: &[ObjectId]) -> Result<()>;   // delete | tombstone+GC
    fn sync_after_publish(&self) -> Result<()>;                 // fsync dir | no-op
}
```

(Exact shape to be refined when implementing; this captures the operation set
the probe found, with the conflict-aware manifest publish as the key change.)

### Mapped surface (from tracing the actual fields)

The concrete method surface of the three filesystem-shaped `DbInner` fields,
found by tracing `db.rs` + `db/commit.rs` — this is what the substrate must
cover, and it is **narrower than it looks** because the manifest *state* logic is
backend-agnostic:

- **`wal: Option<WalFrontDoor>`** — `accept_commit(..)` (the commit append),
  `rewrite_after_replay_floor(..)` (truncate at checkpoint), `persist(..)`
  (durability flush), `stats(..)`, presence (`is_some`). → substrate WAL ops, or
  "no WAL" for the object store.
- **`manifest: Option<Mutex<ManifestStore>>`** — read: `state/tables/buckets/pending_blob_deletions`;
  mutate+publish: `create_bucket`, `prepare_create_bucket_publish`,
  `prepare_add_tables_publish`, `prepare_replace_tables_batch_publish`,
  `prepare_clear_pending_blob_deletions_publish`, `install_prepared_publish`.
  **Crucially, all of these funnel through one chokepoint —
  `publish_next_state` / `publish_next_state_async` → `publish_manifest_with_backend`
  → the `publish_manifest` trait method.** So the manifest state machine stays as
  is; only that chokepoint is backend-specific, and it is *already* trait-routed.
  The one change needed there: make it **conflict-aware** (return
  `Published | Conflict` instead of `Result<()>`; filesystem always `Published`).
- **`process_lock`** — `take()`, `lock()`. → substrate writer-lease.

**Implication for sequencing:** because the manifest publish already funnels
through one trait-routed chokepoint, the cleanest *first* invasive move is to
make that chokepoint (`publish_next_state` + `publish_manifest`) conflict-aware,
then introduce the substrate around WAL + bootstrap + lease. But note every such
move threads the commit/flush hot path — there is no tiny self-contained
sub-step. 2b is one deliberate surgery, guarded by the 293 lib + integration
tests (the filesystem path must stay byte-identical).

## db.rs surgery scope (the real cost)

`DbInner` directly holds `manifest: Option<Mutex<ManifestStore>>`,
`wal: Option<WalFrontDoor>`, `process_lock`, and `native_storage`, and the
open/commit/flush methods call substrate functions concretely. Extracting Band 3
means moving those stateful, filesystem-shaped pieces **behind the substrate**:
`DbInner` would hold `substrate: Box<dyn DurabilitySubstrate>` (or an enum) plus
the byte-level `StorageBackend`. This is the genuine surgery.

Mitigants found by the probe: `wal.rs` / `recovery.rs` / `manifest.rs` are
*already* separate modules, and `ManifestStore` already carries an internal
backend enum — so the substrate is "define a trait over operations that already
have module boundaries, then have `FilesystemSubstrate` delegate to the existing
code", not a rewrite. The 293 lib tests + integration tests are the safety net:
the filesystem substrate must keep behavior byte-identical.

## Open design question: keep a WAL on object storage, or go WAL-less?

This is the one substrate-level decision worth settling early, because it shapes
the `ObjectStoreSubstrate`:

- **Segmented WAL**: each commit/group writes a `wal/<seq>` object; replay lists
  + reads them; truncate deletes below the checkpoint. Preserves low-ish commit
  latency via group commit; more objects + GC.
- **WAL-less (flush-based)**: no WAL; a commit is durable only once the memtable
  is flushed to an SSTable object and the manifest CAS publishes it. Simpler, far
  fewer objects, but commit latency = flush cadence (mitigate with small frequent
  flushes / group commit at the SSTable level). This is the common object-store
  LSM choice.

Recommendation: **WAL-less first** for the object-store substrate (simplest,
fewest objects, aligns with the manifest-CAS-as-commit-point model already in
`object-storage-backend.md`); revisit segmented WAL if commit latency demands it.

## Band 2 finding: the async byte path is already generic; the sync path is filesystem-coupled

A second probe (attempting to genericize the sync byte helpers) found that
Band 2 is **not** a uniform "flip table/blob to one backend type":

- **Async byte helpers are ALREADY generic** over the byte traits
  (`read_table_with_backend_async<B: StorageReadBackend>`,
  `write_table_with_backend_async<B>`, the `*_blob_*_async<B>` family, list).
  They read whole objects eagerly (`decode_table_bytes`, `Table.file = None`).
- **Sync byte helpers are filesystem-coupled via lazy reads.**
  `read_table_with_backend` (sync) stores `Table.file: Some(Arc<NativeFileObject>)`
  and serves blocks lazily through it; `LazyValue::Blob` likewise holds a
  concrete `NativeFileBackend`. Genericizing the sync read path would either
  regress that lazy optimization (not behavior-preserving) or make `Table`
  itself generic over the read-object type (viral). So the sync+lazy path is
  **inherently the filesystem path**, not portable Band 2.

Consequence: the **object-storage backend is async and rides the
already-generic async byte helpers — Band 2 is effectively already satisfied for
it.** The sync+lazy path stays `NativeFileBackend`-only. Therefore *which* byte
path (sync+lazy vs async+eager) to use is a **per-substrate choice**, which means
it belongs to Band 3, not a standalone Band-2 refactor.

## Re-sliced plan

The "slice 2a step 1" `StorageBackend` byte-dispatch enum (commit `4993d09`)
stands as the Band-2 dispatch type for where a single backend value is needed.

- **~~2a (genericize sync byte helpers)~~ — DROPPED.** The Band-2 finding above
  shows it is unnecessary (object store uses the already-generic async path) and
  wrong (sync+lazy is filesystem-specific). Folded into 2b.
- **2b (Band 3 seam, filesystem impl):** define `DurabilitySubstrate`; implement
  `FilesystemSubstrate` by delegating to current wal/recovery/manifest code (and
  its sync+lazy byte path); route `DbInner` + open/commit/flush through it.
  Behavior-identical; tests are the net. This is the db.rs surgery. Entry order:
  ① conflict-aware publish chokepoint → ② substrate around WAL+bootstrap+lease →
  ③ route `DbInner`+open/commit/flush.
  - **2b ① DONE** (`manifest.rs`): the publish chokepoint is now conflict-aware.
    New `pub(crate) enum PublishOutcome { Published, Conflict { current } }`;
    `publish_manifest_with_backend[_async]`, `publish_next_state[_async]`, and the
    wasm `publish_async` all return it; `ManifestStore` advances its in-memory
    `state` **only** on `Published` (a lost CAS leaves state untouched for rebase).
    The filesystem path (temp-write + atomic rename) can't lose a race → always
    `Published`, so every public `ManifestStore` method keeps its `Result<()>`
    signature via `?.published_or_err()` and `db.rs` is untouched — behavior
    byte-identical (346 tests green native + wasm `cargo check`). `published_or_err`
    is the placeholder 2c replaces with a rebase-and-retry loop; the `Conflict`
    variant is `#[allow(dead_code)]` until the object-store substrate constructs it.
  - **2b ② DONE** (`src/substrate.rs`, dead-code + smoke test). **Scope decision
    refining the trait sketch above:** tracing `DbInner` showed the manifest
    publish is *already* abstracted (`ManifestStore` + its backend enum + ① made
    it conflict-aware), and that **bootstrap/recovery is open-time orchestration
    that *constructs* the substrate, not a runtime op**. So the substrate covers
    only the two runtime-divergent things still bound concretely to
    `NativeFileBackend` with a parallel `browser_*` field: the **WAL lifecycle**
    (`wal: Option<WalFrontDoor>`) and the **writer lease**
    (`process_lock: Mutex<Option<ProcessLock>>`). Landed as
    `pub(crate) enum DurabilitySubstrate { Filesystem(FilesystemSubstrate) }`
    (enum dispatch, house style — not `dyn`), with methods `wal_is_present`,
    `accept_commit`, `persist_wal`, `wal_stats`, `rewrite_wal_after_replay_floor`,
    `release_writer_lease`. `FilesystemSubstrate` owns `Option<WalFrontDoor>` +
    `Mutex<Option<ProcessLock>>` and delegates to existing code. Two smoke tests
    drive it against a real temp-dir WAL+lease exactly as commit/flush/close will
    (and the no-WAL/read-only case). Unconsumed (`#![allow(dead_code)]`) until ③;
    behavior byte-identical (348 tests green native + wasm `cargo check`, clippy
    clean).
  - **2b ③ DONE** (`db.rs` + `db/commit.rs`): `DbInner` now holds
    `substrate: DurabilitySubstrate` in place of the `wal: Option<WalFrontDoor>`
    and `process_lock: Mutex<Option<ProcessLock>>` fields. All three construction
    sites build it (native/WASI persistent = `Filesystem(wal, process_lock)`;
    in-memory and the browser path = `Filesystem(None, None)`, inert — browser
    durability still rides the untouched `browser_*` fields). The commit / flush /
    close call sites route through the substrate: `accept_wal_front_door`,
    `can_preaccept_wal_front_door`, `has_wal_front_door` (WAL presence), the sync
    WAL persist, `add_wal_stats`, `rewrite_wal_after_replay_floor`, and
    `close_sync`'s lease release. `#![allow(dead_code)]` removed (substrate fully
    consumed; no dead-code warnings native or wasm). Behavior byte-identical: 348
    tests green native (persistent suite stable across repeated runs), wasm
    `cargo check` clean, clippy clean. **The 2b filesystem substrate is complete.**
    **Next: 2c** — `ObjectStoreSubstrate` as a second `DurabilitySubstrate` variant
    over `ObjectClient` (WAL-less, conflict-aware manifest CAS via `put_if` —
    `PublishOutcome::Conflict` finally gets constructed and `published_or_err`
    becomes a rebase-retry loop — lease+fencing, orphan-object GC); wire
    `HostStorageBackend::ObjectStore`; validate vs the in-memory `ObjectClient`
    fake + recovery/durability suites.
- **2c (object-store impl):** `ObjectStoreSubstrate` over `ObjectClient` (slice
  1): WAL-less durability, conflict-aware manifest CAS (`put_if`), lease+fencing,
  orphan-object GC; byte IO via the **async generic helpers** + `ObjectStoreBackend`.
  Wire `HostStorageBackend::ObjectStore`. Validate against the in-memory
  `ObjectClient` fake + the recovery/durability suites.

  **Forks resolved before slicing 2c:**
  1. *Sync vs async substrate.* Everything object-storage is async, but the
     `DurabilitySubstrate` methods are sync (the filesystem WAL accept is sync via
     lane channels). Resolution: the object store is **WAL-less**, so its
     substrate WAL methods are no-ops (`accept_commit`/`persist_wal` → `Ok(())`,
     `wal_stats` → `None`) — no async needed there. The genuinely-async work
     (write SSTable objects, CAS-publish the manifest) happens on the **existing
     async flush + async manifest-publish paths**, not through the sync substrate.
     So adding the object store does **not** require async substrate methods.
  2. *Manifest CAS integration.* The existing `publish_manifest` byte trait is
     keyed by `StorageObjectId` + bytes with no `ETag` — it cannot express
     conditional-PUT. Rather than contort it, object-storage manifest publishing
     gets its own small primitive (`ObjectManifestStore`) that tracks the object
     `ETag` and CAS-publishes via `put_if`, returning `PublishOutcome`. A later
     slice makes it a `ManifestStoreBackend::ObjectStore` variant.

  **2c sub-slices (dependency order):**
  - **2c-1 DONE** (`manifest.rs` + `object_store.rs`): the conflict-aware manifest
    CAS primitive. Added `ObjectClient::head` (S3-HEAD: metadata/`ETag` without
    bytes) + an `impl ObjectClient for Arc<C>` blanket (so the manifest store and
    byte backend can share one client). Added `ObjectManifestStore<C: ObjectClient>`:
    `open` (head+get → state+`ETag`), `state`, `try_publish` (encode →
    `put_if` If-None-Match to create / If-Match to advance; on `Stored` advance
    cached state+`ETag` and return `Published`; on `PreconditionFailed` refresh
    from the store and return `Conflict { current }`). **This is where slice 2b ①'s
    `PublishOutcome::Conflict` is finally constructed** (variant `#[allow(dead_code)]`
    dropped; only the `current` field stays annotated until the rebase-retry loop
    reads it). Dead-code + 3 unit tests vs `InMemoryObjectStore` incl. a real
    two-writer create-race → conflict → rebase → retry-succeeds. 351 tests green
    native + wasm `cargo check` + clippy clean.
  - **2c-2 DONE** (`object_store.rs`, `65385ce`): object-store byte backend —
    `ObjectStoreBackend` implementing the async `Storage*Backend` byte traits over
    `Arc<dyn ObjectClient>` (whole-object read via eager GET + in-memory random
    access, write=PUT, delete, prefix-list→direct-children-by-extension), mapping
    `StorageObjectId.path()`→object key. `StorageCapabilities::object_store()`
    added. Dead-code + 2 tests.
  - **2c-3 DONE** (`substrate.rs`, `d97aef8`): `DurabilitySubstrate::ObjectStore`
    variant — WAL-less (all WAL methods no-op; `wal_is_present`=false), with
    `ObjectWriterLease` modelled as a **fencing token** (acquire bumps a monotonic
    epoch via CAS, doesn't fail-if-held; a stale holder is fenced at
    manifest-publish time, wired in 2c-4). `release_writer_lease` is a no-op for
    the object store (reclaimed by next epoch / TTL). Dead-code + 3 tests.
  - **DECISION (2026-06-10): object-store DBs are async-only.** Confirmed with the
    user. Object storage is network I/O and the manifest CAS publish is inherently
    async (`put_if`); the sync `publish_next_state` can't `await`. So object-store
    open/commit/flush use the async API only (like the browser backend);
    filesystem/memory keep sync+async. This shapes all of 2c-4.
  - **2c-4 (the integration mega-step — NOT a single commit; ~4 sub-commits).**
    Probing it found real plumbing beyond the original one-liner.
    **Progress: 2c-4a DONE (`937fbf6`), 2c-4b DONE (`ce8b6d7`); 2c-4c remaining.**
    - **2c-4a — `ManifestStore` object-store backend + rebase-retry.** Add
      `ManifestStoreBackend::ObjectStore(ObjectManifestStore<Arc<dyn ObjectClient>>)`
      (delegates to the 2c-1 primitive — no duplicate state machine). Needs manual
      `Debug`/`Clone` for `ObjectManifestStore` over `dyn` (dyn isn't `Debug`). Add
      `open_object_store_async`; the `publish_next_state_async` `ObjectStore` arm
      delegates to `try_publish` and syncs `self.state` from the primitive. **The
      rebase-retry loop lives at the public-method level** (`Conflict` → refresh →
      re-run the mutation+validation → retry); since the sync API is gone for
      object storage, the mutating methods (`create_bucket`, `add_tables`,
      `replace_tables_batch_and_mark_blob_deletions`, `clear_pending_blob_deletions`)
      need **async variants** (only `create_bucket_async`/`add_tables_async` exist
      today; replace/clear are sync-only or wasm-prepared-only) — factor them
      through one `commit_edit_async(|state| ...)` retry helper.
    - **2c-4b DONE (`ce8b6d7`) — `StorageBackend` enum gains `ObjectStore`.** Added
      `StorageBackend::ObjectStore(ObjectStoreBackend)` + `BackendReadObject::ObjectStore`;
      byte ops delegate, all non-byte ops (append/wal-rewrite/lease/dir/manifest)
      + every blocking variant return `unsupported` (object-store DBs are WAL-less,
      async-only, lease via the substrate, manifest via `ObjectManifestStore`). The
      enum can now dispatch object-store byte IO; the `DbInner` *field reroute*
      (concrete `native_storage` → `StorageBackend`) is folded into 2c-4c since it
      only makes sense alongside the open path that constructs an object-store DB.
    - **2c-4c — `DbInner` reroute + object-store async open path + options
      (REMAINING; the largest remaining piece).** (1) Change
      `DbInner.native_storage: NativeFileBackend` → `storage: StorageBackend` and
      reroute the ~15 runtime `self.inner.native_storage` sites + 3 construction
      sites + `Drop`/stats (filesystem stays `StorageBackend::Native(..)`,
      byte-identical — a 2b③-scale surgery). (2) A distinct
      `open_object_store_async` (bootstrap: acquire `ObjectWriterLease`, read
      manifest via `ObjectManifestStore`, list table/blob objects → buckets, build
      `DbInner` with `substrate = ObjectStore`, byte backend = object store, no WAL)
      + `options::HostStorageBackend::ObjectStore` selection + open dispatch +
      integration tests vs `InMemoryObjectStore`.
  - **2c-5:** orphan-object GC (objects unreferenced by the published manifest).

## Why this is safe to pursue

The probe converted "unknown, possibly-everything refactor" into a bounded one:
divergence is WAL + manifest-publish-conflict + bootstrap; the rest is portable
or already trait-abstracted; the substrate concepts already have module
boundaries; and every step before the object-store impl is behavior-preserving
with 293 tests guarding it.
