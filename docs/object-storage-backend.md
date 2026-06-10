# Object-Storage Backend

Status: **Design (not implemented).** This document specifies a fourth storage
backend for trine-kv that persists the database on object storage (Amazon S3 and
S3-compatible / other providers) instead of a local filesystem. It is the
durability foundation for running trinedb as a self-hostable or managed cloud
service where data lives on cheap object storage.

This is a **data-plane** feature and stays open source: a self-hosting user must
be able to point their own server at their own bucket, exactly as they can point
the native-file backend at their own disk. Object storage is an **allowed
option, never a requirement** — the native-file backend remains the default and
is unchanged.

## Goals

- Persist the full database (SSTables, blobs, manifest, WAL) on an object store.
- Keep strong, crash-consistent semantics: a committed transaction stays
  committed; recovery reconstructs a consistent state from the bucket alone.
- Reuse the existing storage-backend seam (capabilities + `Storage*Backend`
  traits). No changes to the LSM core, MVCC, or the transaction API.
- Provider-agnostic: S3 first, but the object primitive is a small trait so
  S3-compatible stores (R2, MinIO, GCS XML, Azure Blob) drop in.
- Keep the embedded core dependency-free: the object-store backend and its cloud
  SDK live behind a cargo feature gate.

## Non-goals (handled elsewhere or later)

- **Client↔cloud sync / local-first replication.** Offline-first clients that
  sync into a server is a separate design (`docs/sync-protocol.md`, TODO). This
  backend is how the **server** durably stores authoritative data; it is
  orthogonal to how clients replicate into it.
- **Multi-writer conflict resolution / CRDTs.** This backend assumes a single
  logical writer at a time (see Concurrency). Multi-master merge is out of scope.
- **The wire protocol.** Transport between clients and the server is separate.

## The existing seam (what we build on)

trine-kv already abstracts storage behind fine-grained capability traits, and
already ships three backends (`MemoryStorageBackend`, `NativeFileBackend`,
`BrowserStorageBackend`). The object-store backend is a **fourth backend**, with
`BrowserStorageBackend` as the closest template (object-oriented, no real
filesystem). Relevant pieces in `src/storage.rs`:

- **Object identity:** `StorageObjectKind` ∈ {`Blob`, `Manifest`,
  `RecoveryReport`, `Table`, `Temporary`, `Wal`, `WriterLease`}; `StorageObjectId`
  carries the kind + a path-like name. The engine already thinks in *named
  objects*, which map 1:1 to object-store keys.
- **Capabilities:** `StorageCapability` already names everything we need —
  `ObjectRead`, `ObjectListing`, `ObjectWrite`, `ObjectDelete`, `RandomRead`,
  `Append`, `AtomicWalRewrite`, `AtomicManifestPublish`, `WriterLease`,
  `Persistent`, plus the sync/durability modes. The engine is capability-gated
  (`require`, `require_durability`), so a backend that lacks `Append` is already
  a supported shape (the browser backend exercises non-filesystem paths).
- **Per-concern backend traits** (each with an async + blocking variant):
  - `StorageReadBackend` / `StorageObjectReadBackend` — `open_read` (random read
    via offset/len) and `read_object_bytes` (whole-object read).
  - `StorageObjectWriteBackend::write_object` — write a whole immutable object.
  - `StorageAppendBackend` + `StorageAppendObject` — WAL append/persist.
  - `StorageWalRewriteBackend::rewrite_wal` — atomic whole-WAL replace via a
    temporary object + swap.
  - `StorageManifestReadBackend::read_current_manifest` /
    `StorageManifestPublishBackend::publish_manifest` — **the commit point.**
  - `StorageWriterLeaseBackend::acquire_writer_lease` — single-writer fencing.
  - `StorageDirectory{Create,List,Sync}Backend` — namespace/list/durability of a
    "directory" (a key prefix on an object store).
- **Selection:** `options::HostStorageBackend` (`Wasi`, `Browser`) is the public
  enum that chooses a non-native backend. We add an `ObjectStore` variant; the
  native-file and in-memory defaults are untouched.

The decisive consequence: the LSM already isolates **the only three operations
whose object-store semantics differ from a filesystem** behind their own traits
— `publish_manifest` (the commit), `acquire_writer_lease` (the writer fence),
and the WAL append/rewrite. Everything else (SSTables, blobs) is write-once and
maps directly to whole-object PUT + range GET. So this backend is "implement the
traits + solve those three," not an engine rewrite.

## The object primitive

A minimal, provider-agnostic trait. The S3 (and S3-compatible) implementation is
the first; the rest of the backend is written against this trait only.

```rust
/// Provider-agnostic object store. Keys are the StorageObjectId rendered to a
/// bucket-relative path. All methods are async; the blocking adapter wraps them.
trait ObjectClient: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;
    async fn get_range(&self, key: &str, off: u64, len: u64) -> Result<Bytes>;
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>; // key + len + etag

    /// Conditional put used for the manifest commit and the writer lease.
    /// `If-None-Match: *` for create-if-absent; `If-Match: <etag>` for
    /// compare-and-swap. Returns `Conflict` when the precondition fails.
    async fn put_if(&self, key: &str, bytes: Bytes, cond: Precondition) -> Result<PutResult>;
}

enum Precondition { IfNoneMatch, IfMatch(ETag) }
```

`put_if` is the load-bearing primitive. Modern S3 supports conditional writes
(`If-None-Match: *` since 2024; `If-Match`/CAS on PUT is the mechanism behind
the manifest commit). For stores without conditional PUT, the backend declares
reduced capabilities (no `AtomicManifestPublish`) and the database opens
read-only or refuses multi-process writers — see Open questions.

## Capability declaration

```
Persistent, RandomRead (via get_range), ObjectRead, ObjectListing,
ObjectWrite, ObjectDelete, AtomicWalRewrite (whole-object PUT is atomic),
AtomicManifestPublish (via put_if CAS), WriterLease (via put_if + TTL),
AsyncTasks
```

Notably **absent: `Append`.** S3 objects are immutable; there is no append. The
WAL is handled without it (below), exactly as the browser backend handles
non-filesystem persistence.

## Mapping each object kind

| Kind | Object-store mapping |
|------|----------------------|
| `Table` (SSTable), `Blob` | Write-once → `put` whole object. Reads → `get` or `get_range` (block reads). Compaction writes new keys and `delete`s obsolete ones. Perfect fit for immutable LSM output. |
| `Manifest` | The linearizable commit point. `read_current_manifest` → `get`; `publish_manifest` → `put_if` CAS (see Commit). |
| `RecoveryReport` | `put` / `get`, non-critical. |
| `Temporary` | Backing for `rewrite_wal`'s temp object; `put` then swap, or skip (whole-object PUT is already atomic). |
| `Wal` | No append on S3. See WAL. |
| `WriterLease` | A small object guarded by `put_if` + TTL/epoch fencing. See Concurrency. |

## The three hard problems

### 1. WAL without append

S3 objects are immutable, so the filesystem WAL (one growing file, `append_io` +
`fsync`) does not translate. Options:

- **(A) Segmented WAL — chosen.** Each flush/commit batch becomes a new,
  numbered WAL **segment object** (`wal/<seq>`). The "append" of N records
  becomes a `put` of one segment. Recovery lists `wal/*`, orders by seq, and
  replays. Checkpoint/truncation (today's `rewrite_wal`) becomes "delete
  segments below the checkpoint."
- **(B) Whole-WAL rewrite** (what the browser backend effectively does):
  read-modify-write the entire WAL object per commit. Simple but O(WAL size) per
  commit — fine for tiny WALs, bad on S3. Reject for the server case.

Segmenting drives the **commit-latency** story: one object PUT per commit is
~tens of ms on S3. Mitigate with **group commit** — coalesce concurrent
transactions' WAL records into a single segment PUT, amortizing latency across a
batch. This pairs naturally with the existing `DurabilityMode`.

### 2. The commit point = manifest CAS

A trine-kv commit advances the manifest to point at the new set of SSTables/WAL.
On object storage this becomes:

1. PUT the new SSTable/WAL objects (immutable, safe to write before commit).
2. `publish_manifest` = `put_if(manifest_key, new_manifest, IfMatch(prev_etag))`.
   - Success → the new version is the durable, linearizable truth.
   - `Conflict` (etag moved) → someone else advanced the manifest; surface as the
     existing conflict error and retry/rebase.

This maps **directly onto trinedb's optimistic MVCC**: trinedb already validates
conflicts at commit and returns a retryable busy error. "CAS the manifest" is the
same contract one layer down. No new semantics.

Orphan objects from a lost CAS race (SSTables written but not referenced) are
reclaimed by a GC pass that lists objects unreferenced by the current manifest
and deletes them after a safety interval.

### 3. Writer fencing across processes/nodes

trine-kv has a `WriterLease` capability (single writer owns the DB). On object
storage the lease is a small object acquired via `put_if(IfNoneMatch)` carrying
an owner id + expiry + monotonically increasing **epoch (fencing token)**. The
holder renews before expiry; a crashed holder's lease expires and another writer
can take it at a higher epoch. The manifest records the epoch so a stale,
fenced-out writer's `publish_manifest` CAS fails. This keeps the single-logical-
writer invariant the rest of the design assumes.

## Concurrency model on object storage

- **Single logical writer + many readers (recommended first target).** One
  writer node holds the lease and advances the manifest; any number of reader
  nodes open the manifest read-only and serve queries from cached objects. This
  is the cheap, consistent, scale-out-reads model (and matches scale-to-zero:
  drop all compute, data is safe in the bucket; cold start re-reads the
  manifest + working set).
- **Optimistic multi-writer (future).** Multiple writers contending on the
  manifest CAS is *possible* given the primitive, but raises write-amplification
  and rebase questions; deferred. Not needed for the cloud service's single-
  primary-per-database model.

## Caching and cold start

- Reuse the existing block cache (`src/cache.rs`): hot blocks stay local; cold
  reads are `get_range` against the object store. A local-disk block cache tier
  (optional) cuts repeat egress.
- **Cold start** = `read_current_manifest` + fetch the working-set objects. Keep
  the manifest small and the SSTable block index fetchable independently so a
  cold node is queryable after a few round trips, not a full download.
- Cost note: object storage bills per-request + egress. Favor larger blocks,
  group commit, and caching to keep request counts down.

## Deployment modes (same backend, different write-timing policy)

1. **Live backend.** The object store *is* the durable store; every commit's
   manifest CAS happens against it. Strong consistency, cheapest at rest,
   network-dependent for writes. This is the cloud server's normal mode.
2. **Async replica target.** A local-first node commits to local LSM first, and a
   background shipper mirrors immutable objects to the bucket and advances the
   remote manifest when connected (offline tolerated; reconnect drains the
   queue). This is the substrate for the local-first sync product; the backend
   implementation is identical, only *who advances the remote manifest, and when*
   differs. Detailed in the sync design (next doc).

A per-transaction / per-database **remote durability level** governs mode 2:
`local` (commit returns on local durability; remote is eventual) vs `synced`
(commit waits for remote manifest CAS). This extends the existing
`DurabilityMode` with a remote tier.

## Public surface (trinedb / trine-kv)

- `options::HostStorageBackend::ObjectStore { provider, bucket, prefix, creds, .. }`
  selects the backend. Native-file / in-memory remain the defaults.
- Feature gate, e.g. `object-store` (and `object-store-s3` for the AWS SDK), so
  the embedded core pulls in no cloud dependencies unless enabled.
- trinedb exposes it through its existing `DbOptions` path; the SQL/MVCC/
  transaction layers are untouched.

## Implementation slices

1. **`ObjectClient` trait + an in-process fake** (an `ObjectStore` over a local
   directory or in-memory map, with real `put_if`/etag semantics). Lets us build
   and test the whole backend — segmented WAL, manifest CAS, lease fencing —
   under object semantics with **zero cloud dependency**, deterministically.
2. **Object-store backend** implementing the `Storage*Backend` traits against
   `ObjectClient`, with capability declaration; wire into `HostStorageBackend`.
   Run trine-kv's existing recovery/durability test suites against the fake.
3. **S3 `ObjectClient`** (AWS SDK or `object_store` crate) behind the feature
   gate; integration test against MinIO / S3-compatible local server.
4. **Group commit + block-cache tuning**; cold-start and cost benchmarks.
5. (Later) replica-target mode + remote durability level — coordinated with the
   sync design.

## Open questions

- **Conditional-write support across providers.** S3, R2, GCS, Azure differ in
  CAS/precondition support and consistency. Define the minimum
  (`put_if` IfMatch/IfNoneMatch + read-after-write) and degrade gracefully
  (read-only / single-process) when absent.
- **WAL segment sizing & group-commit window** vs commit-latency target.
- **GC policy** for orphaned objects (safety interval, listing cost).
- **Lease TTL / clock assumptions** for fencing; tolerate clock skew via epochs,
  not wall-clock comparison alone.
- **Manifest growth**: keep it compact (version-edit log + periodic snapshot) so
  cold start and every commit PUT stay small.
