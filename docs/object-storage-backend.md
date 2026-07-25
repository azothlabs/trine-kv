# Object-Storage Backend

Status: **Implemented.** This document records the current object-storage
durability contract for trine-kv. The backend persists the database on Amazon
S3, S3-compatible providers, or any implementation of Trine's `ObjectClient`
contract instead of a local filesystem.

This is a **data-plane** feature and stays open source: a self-hosting user must
be able to point their own server at their own bucket, exactly as they can point
the native-file backend at their own disk. Object storage is an **allowed
option, never a requirement** — the native-file backend remains the default and
is unchanged.

## Goals

- Persist the full database (SSTables, blobs, manifest, WAL) on an object store.
- Keep strong, crash-consistent semantics: a committed transaction stays
  committed; recovery reconstructs a consistent state from the bucket alone.
- Reuse the storage-backend and durability-substrate seams. The LSM core, MVCC,
  and transaction API retain the same visibility semantics.
- Provider-agnostic: S3 first, but the object primitive is a small trait so
  S3-compatible stores (R2, MinIO, GCS XML, Azure Blob) drop in.
- Keep provider adapters optional. The provider-neutral in-memory client is in
  the core; the S3 adapter is behind the `s3` feature.

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
    `StorageManifestPublishBackend::publish_manifest` — atomic metadata publish.
  - `StorageWriterLeaseBackend::acquire_writer_lease` — single-writer fencing.
  - `StorageDirectory{Create,List,Sync}Backend` — namespace/list/durability of a
    "directory" (a key prefix on an object store).
- **Selection:** `DbOptions::object_store()` plus
  `Db::open_object_store[_at]` selects this backend. Native-file and in-memory
  defaults are untouched.

The decisive consequence is that the object-specific semantics remain
concentrated in manifest CAS, the writer/WAL-head lease, and immutable segmented
WAL publication. SSTables and blobs stay write-once and map directly to whole
object PUT plus bounded range GET.

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

Notably **absent: `Append`.** S3 objects are immutable. The object-store
durability substrate implements WAL acceptance by publishing immutable,
content-addressed segment objects and advancing the lease/WAL-head object with
a conditional PUT.

## Mapping each object kind

| Kind | Object-store mapping |
|------|----------------------|
| `Table` (SSTable), `Blob` | Write-once → `put` whole object. Reads → `get` or `get_range` (block reads). Compaction writes new keys and `delete`s obsolete ones. Perfect fit for immutable LSM output. |
| `Manifest` | Atomic table, bucket, checkpoint, and GC metadata. `read_current_manifest` → `get`; publish → conditional `put_if` CAS. It is not rewritten for every user commit. |
| `RecoveryReport` | `put` / `get`, non-critical. |
| `Temporary` | Backing for `rewrite_wal`'s temp object; `put` then swap, or skip (whole-object PUT is already atomic). |
| `Wal` | Immutable, content-addressed segment chain. The current head is stored in the writer-lease object. |
| `WriterLease` | A small CAS-guarded object containing epoch, random owner nonce, expiry, confirmed sequence, and WAL head. |

## The three hard problems

### 1. WAL without append

Each accepted group is encoded into an immutable segment containing its
predecessor key and consecutive commit frames. The object key includes a SHA-256
content identity; replay recomputes and verifies that identity before decoding.
The writer lane serializes sequence reservation with WAL handoff, so concurrent
callers cannot publish sequence holes or reorder the remote chain.

The lease object is the linearizable WAL-head commit point. A segment PUT alone
is only an orphan; the commit is confirmed after CAS advances the lease's
confirmed sequence and head to that exact segment. If a PUT/CAS response is
lost, Trine reads back the state and treats only an exact byte-for-byte intended
state as success. A readable different state is a conflict; an unreadable
outcome remains an error rather than being guessed successful.

`DurabilityMode::Buffered` accepts frames into process memory. Callers that need
remote durability must use `persist(DurabilityMode::Flush)` (or a stronger
supported mode) before relying on recovery. Object storage rejects filesystem
sync modes whose guarantees it cannot provide.

Replay and admission are bounded: a segment is at most 128 MiB, a group reserves
64 KiB of that limit for framing, a chain is at most 16,384 segments, and one
replay is at most 1 GiB. Exceeding a bound fails closed.

### 2. Manifest CAS

The manifest is the linearizable metadata point for SSTable sets, buckets,
checkpoints, and pending reclamation; ordinary user commits are durable through
the WAL-head CAS and do not rewrite the manifest. Table/blob objects are written
first, then an edit is published with conditional PUT.

CAS conflicts rebase only when the edit is still semantically applicable.
Already-applied exact edits are idempotent; table-ID collisions and partially
applied replacements are corruption. When a conditional PUT reports a transport
error, read-after-error accepts only the exact intended manifest bytes as
published. Orphan immutable objects are reclaimed only after current manifest
reachability is checked.

### 3. Writer fencing across processes/nodes

Lease format v3 binds every acquisition or takeover to both a monotonically
increasing epoch and a fresh random 128-bit owner nonce. Every renewal, WAL-head
CAS, rewrite, and release verifies both values. This closes the same-epoch ABA
hole: a stale handle is fenced even if it observes an epoch reused by legacy
state or a replacement owner. Legacy and v2 leases remain readable with an
all-zero owner only for compatibility; new writes publish v3.

## Concurrency model on object storage

- **Single logical writer + many readers.** One
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

## Deployment mode

The implemented mode uses the object store as the authoritative durable store.
An asynchronous local-to-remote replica target remains out of scope and must not
be inferred from `DurabilityMode::Buffered`.

## Public surface (trinedb / trine-kv)

- `DbOptions::object_store()` configures engine policy;
  `Db::open_object_store` and `Db::open_object_store_at` supply the client and
  optional database prefix.
- The provider-neutral client contract is always available; the S3 adapter is
  enabled by the `s3` feature.
- trinedb exposes it through its existing `DbOptions` path; the SQL/MVCC/
  transaction layers are untouched.

## Provider contract and remaining scope

The client must provide atomic create-if-absent and ETag-based compare-and-swap,
read-after-write visibility, bounded range reads, and observable deletion.
`verify_object_client_contract_on_open` can probe unsafe conditional-PUT
implementations. S3 listing is capped at 100,000 returned objects per operation;
larger namespaces fail with `RuntimeBusy` instead of allocating without bound.
The ETag used after PUT comes only from that PUT response—Trine never substitutes
a later HEAD result that could belong to another write.

Multi-primary writers, versioned-object reclamation, replica shipping, and
provider-specific retention/lock bypass remain out of scope.
