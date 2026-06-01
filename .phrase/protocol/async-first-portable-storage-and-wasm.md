# Async-First Portable Storage And WASM Protocol

Date: 2026-05-27

## 1. Purpose

This protocol defines Trine KV's async-first database API and portable storage
boundary.

The design target is:

```text
Trine's primary database API is async, and every persistent backend enters the
engine through a capability-based async storage contract.
```

Native files, volatile memory, WASI storage, and browser persistence are backend
implementations. They are not the engine model.

This protocol extends the v1 LSM MVCC spec. It governs API shape, storage
capabilities, durability mapping, runtime boundaries, cancellation rules, and
WASM readiness. It does not change SSTable, WAL, manifest, MVCC, compaction, or
transaction semantics unless this protocol explicitly says so.

## 2. Non-Goals

This protocol does not promise:

- identical durability strength on every platform;
- a required async runtime crate;
- a required native I/O mechanism;
- browser multi-tab writable access without a reliable writer lease;
- making CPU-only LSM algorithms async;
- using threads, memory mapping, direct I/O, process locks, or blocking
  filesystem calls in the engine core;
- changing the on-disk format solely because the API is async.

## 3. Vocabulary

- **Primary API**: the public `Db`, `Bucket`, `Snapshot`, `Transaction`, and
  cursor API that defines Trine's behavior.
- **Blocking adapter**: native convenience wrapper that calls the primary async
  API from synchronous Rust code.
- **Storage backend**: implementation of Trine's async storage contract.
- **Storage capability**: explicit backend promise such as volatile-only
  storage, atomic manifest publish, durable sync, writer lease, random reads,
  background task support, blocking adapter support, or platform async I/O.
- **Storage object**: backend-owned byte object used for WAL segments, table
  files, blob files, manifests, lock or lease records, reports, and temporary
  outputs.
- **Publish operation**: backend action that makes a manifest version current
  atomically according to that backend's capability contract.
- **Completion**: result of an async storage request, including returned buffers
  and verified error state.
- **Cooperative worker**: maintenance worker that advances within an explicit
  time or operation budget and yields instead of monopolizing the host thread.

## 4. Design Principles

### 4.1 Async-First API

Persistent database operations are async by default:

- open and recovery;
- point reads that may miss cache and read a block;
- range and prefix cursor advancement;
- writes that enter WAL and durability handling;
- flush, compaction, and close barriers;
- transaction reads and commit.

CPU-only work remains synchronous inside the engine:

- internal-key comparison;
- memtable lookup;
- block decode after bytes have arrived;
- filter probes;
- index search;
- MVCC visibility checks;
- merge-cursor selection.

Async is used at waiting boundaries, not as a blanket coding style.

### 4.2 Blocking Is An Adapter

The blocking API is a native convenience layer. It must not define engine
semantics.

Rules:

- blocking handles wrap the primary async engine;
- blocking adapters are unavailable or unsupported on targets that cannot block
  safely;
- blocking adapters must not introduce a second storage path;
- tests that define correctness should target the primary async API first.

### 4.3 Backend Capabilities Are Honest

Backends must report what they can actually guarantee. The engine must not
silently pretend that a weaker backend provides stronger persistence.

Required capability categories:

```text
Volatile
Persistent
RandomRead
Append
AtomicManifestPublish
WriterLease
Flush
StrictDataSync
StrictMetadataSync
BackgroundThreads
AsyncTasks
BlockingAdapter
PlatformAsyncIo
CooperativeTasks
```

Capability names may evolve, but the distinction must remain explicit.

### 4.4 Runtime Independence

The engine may depend on a small runtime boundary, but must not hard-code one
host runtime into the storage model.

The runtime boundary may provide:

- spawn async task;
- spawn blocking task when the target supports it;
- yield;
- sleep or timer;
- bounded channel or queue;
- cancellation token;
- shutdown join.

Unsupported runtime features must be visible during open or option validation.

### 4.4.1 `io` Boundary Before Backend Choice

Trine's `io` module and storage contract own the architecture. Platform crates
and OS APIs are backend implementations behind that boundary.

Rules:

- phase goals and acceptance gates name Trine operations, traits, capabilities,
  completions, stats, and recovery behavior before naming a backend crate;
- backend crate names may appear in Cargo metadata, implementation modules, and
  dependency-selection evidence, but not as the subject of user-facing docs or
  protocol wording;
- backend-specific limits must be represented as capabilities, fallbacks, or
  explicit blockers rather than hidden in implementation details;
- a platform I/O phase must include a backend boundary receipt before code
  changes: owned Trine operation names, selected backend, known unsupported
  operations, leak-check scope, and verification commands;
- review is not complete until docs and protocol are scanned for backend-name
  leakage outside dependency-selection evidence.

### 4.5 Storage Operations Are Database Operations

The storage contract describes what the database needs, not one platform's file
API.

Core operations:

```text
open_backend(options) -> backend
backend.capabilities() -> StorageCapabilities
acquire_writer_lease(scope) -> Lease
read_at(object_id, offset, len, buffer) -> Completion<buffer>
append(object_id, bytes) -> Completion<AppendResult>
write_temp_object(kind, bytes_or_stream) -> Completion<ObjectId>
sync_object(object_id, durability) -> Completion
publish_manifest(manifest_bytes, publish_options) -> Completion<ManifestId>
read_current_manifest() -> Completion<Option<ManifestBytes>>
list_objects(kind) -> Completion<Vec<ObjectId>>
delete_object(object_id) -> Completion
close() -> Completion
```

Native-file backends may implement these operations with files. Memory backends
may use in-process byte arrays. Browser and WASI backends may use their own host
storage APIs. The engine sees the same database-level operations.

### 4.6 Manifest Publish Is Not Rename

Manifest publish is a protocol action.

Rules:

- the engine calls `publish_manifest`;
- a backend chooses how to make the new manifest current;
- recovery reads the current manifest through the same backend contract;
- native-file implementations may use temporary files and atomic replacement;
- non-file implementations may use a transactional host storage update;
- if a backend cannot provide atomic manifest publish, persistent writable open
  must fail unless the user explicitly selects a weaker experimental mode.

### 4.7 WASM Is A First-Class Constraint

The engine core must be compatible with targets that cannot safely block and may
not have native threads or a process filesystem.

Rules:

- the primary API must compile without requiring a blocking filesystem;
- memory mode must be available on WASM-capable builds;
- persistent WASM backends are selected by capability, not assumed;
- browser persistence must use async APIs only;
- writable browser persistence requires a reliable writer lease;
- if no reliable writer lease exists, persistent writable open fails;
- background maintenance must support cooperative execution;
- compaction, flush, and cleanup must be resumable across small work budgets;
- no core protocol may rely on memory mapping, OS page size, process locks,
  direct I/O, or native endian behavior.

## 5. Public API Shape

The primary API is async:

```rust
let db = Db::open(options).await?;
db.put(key, value).await?;
let value = db.get(key).await?;

let mut cursor = db.range(range).await?;
while let Some(row) = cursor.next().await? {
    // use row
}
```

Expected conceptual surface:

```rust
Db::open(options).await -> Result<Db>
Db::memory(options) -> Result<Db>
Db::put(key, value).await -> Result<()>
Db::get(key).await -> Result<Option<Value>>
Db::range(range).await -> Result<Cursor>
Db::prefix(prefix).await -> Result<Cursor>
Db::write(batch, write_options).await -> Result<CommitInfo>
Db::persist(mode).await -> Result<()>
Db::flush().await -> Result<()>
Db::compact_range(range).await -> Result<()>
Db::close().await -> Result<()>
Db::snapshot() -> Snapshot
Db::transaction(options) -> Transaction
Db::stats() -> DbStats

Bucket::get(key).await -> Result<Option<Value>>
Bucket::put(key, value).await -> Result<()>
Bucket::delete(key).await -> Result<()>
Bucket::range(range).await -> Result<Cursor>
Bucket::prefix(prefix).await -> Result<Cursor>

Cursor::next().await -> Result<Option<KeyValue>>

Transaction::get(key).await -> Result<Option<Value>>
Transaction::put(key, value) -> Result<()>
Transaction::delete(key) -> Result<()>
Transaction::read_range(range).await -> Result<()>
Transaction::commit().await -> Result<CommitInfo>

BlockingDb::open(options) -> Result<BlockingDb>
BlockingDb::get(key) -> Result<Option<Value>>
BlockingDb::put(key, value) -> Result<()>
```

Rules:

- builders and in-memory staging methods may remain synchronous;
- snapshot creation may remain synchronous because it captures already published
  immutable state;
- cursor advancement is async because it can require backend reads;
- transaction mutation methods that only stage local writes may remain
  synchronous;
- transaction reads and commit are async;
- blocking handles are optional native adapters.

## 6. Durability Mapping

Write durability modes map through backend capabilities:

```text
Buffered   -> accepted by WAL path; no durable flush promise
Flush      -> backend has pushed bytes to its flush boundary
SyncData   -> backend reports durable data sync for WAL data
SyncAll    -> backend reports durable data and metadata publish where required
```

Rules:

- unsupported durability returns `UnsupportedDurability`;
- the engine must not silently downgrade a requested durability level;
- memory mode accepts only volatile durability semantics;
- persistent open records the backend durability capabilities in stats or
  diagnostics;
- manifest publish uses the strongest available operation required by the
  selected durability policy.

## 7. Cancellation Rules

Async cancellation must not corrupt commit state.

Rules:

- cancelling before a request is accepted has no effect;
- once a write enters the side-effecting commit protocol, Trine owns completion;
- dropping the caller future only drops the waiter;
- the internal commit must reach a terminal state: visible, skipped, or failed
  before publication;
- once any user delta for a sequence is published, the commit must finish
  publication and become visible or crash recovery must replay it from WAL;
- flush, compaction, and close barriers may return cancellation to the caller,
  but their internal worker state must remain recoverable.

## 8. Background Work

Background work is expressed through the runtime boundary.

Native targets may use threads or async tasks. WASM targets may use async tasks
or cooperative workers.

Rules:

- maintenance units must have explicit budgets;
- a worker may yield and resume without losing protocol state;
- compaction output remains invisible until manifest publish completes;
- cleanup remains snapshot-safe even when split across multiple worker turns;
- startup recovery must not require background workers to complete before the
  database can reject unsupported modes or report corruption.

## 9. Backend Families

### 9.1 Memory Backend

The memory backend is volatile.

Rules:

- supports the primary async API;
- may complete requests immediately;
- preserves MVCC, transaction, cursor, and compaction semantics;
- does not promise process-exit durability;
- is the baseline backend for WASM logical correctness tests.

### 9.2 Native-File Backend

The native-file backend stores objects under a local directory.

Rules:

- uses the storage backend contract, not direct calls from engine core;
- must provide a writer lease for writable open;
- must provide atomic manifest publish for persistent writable mode;
- may implement async operations with a bounded blocking pool when the host file
  API is blocking;
- may use the opt-in `platform-io` runtime mode for native-file length, owned
  random reads, whole-object reads, temp-write publish operations, WAL
  append-object opening/append/persist/rewrite, object delete, directory
  create/sync, and writer lease acquisition when the build enables the matching
  platform I/O feature;
- may submit directory and object listing through the platform driver only as a
  separately reported platform blocking fallback until that driver exposes a
  real directory enumeration operation;
- may add stronger platform-specific implementations behind the same contract.
- must report `BlockingAdapter` separately from `PlatformAsyncIo`; using a
  bounded blocking pool must not be described as true platform async I/O.
- must report platform-driver blocking fallback tasks separately from both
  Trine's bounded blocking adapter and true platform async I/O tasks.

### 9.3 WASI Backend

The WASI backend is persistent only when the host grants suitable storage
capabilities.

Rules:

- option validation checks required capabilities during open;
- unsupported strict durability returns `UnsupportedDurability`;
- writer lease support is required for writable persistent open;
- recovery semantics match the main persistent protocol when required
  capabilities exist.

### 9.4 Browser Backend

The browser backend is async-only.

Rules:

- memory mode is always allowed when the build includes it;
- persistent writable mode requires reliable writer leasing;
- if writer leasing or atomic manifest publish is unavailable, writable
  persistent open fails;
- durability strength is capability-reported and may be weaker than native
  strict sync;
- background maintenance must be cooperative and budgeted;
- public APIs must never block the browser main thread.

## 10. Recovery Requirements

Recovery uses the storage backend contract:

1. acquire writer lease when writable;
2. read current manifest;
3. validate manifest-referenced objects;
4. list WAL objects newer than the replay floor;
5. replay valid WAL records;
6. rebuild in-memory write state;
7. report unsupported durability or missing capabilities before accepting
   writes that require them.

Rules:

- recovery must be deterministic per backend capability contract;
- missing required objects fail closed;
- final WAL tail truncation rules remain unchanged;
- manifest publish atomicity is validated by backend fixtures;
- recovery must not depend on native directory scanning order.

## 11. Observability

Stats and diagnostics should expose:

- backend kind;
- capability set;
- durability level actually supported;
- async storage request counts by operation;
- storage queue depth;
- storage request latency by operation;
- cooperative worker yields;
- background task budget exhaustion;
- blocking adapter call count when enabled;
- whether the storage backend uses a blocking adapter or platform async I/O;
- platform-driver blocking fallback count for operations without a real
  platform primitive;
- unsupported capability errors.

## 12. Required Tests

Correctness tests:

- primary `Db::open` path is async;
- memory backend passes the shared logical suite through the async API;
- blocking adapter passes a smoke suite by delegating to the async engine;
- unsupported durability returns `UnsupportedDurability`;
- missing writer lease rejects persistent writable open;
- manifest publish fixture proves either old or new manifest is current after
  injected failure;
- cursor `next().await` remains snapshot-consistent across backend reads;
- cancellation after commit acceptance leaves a terminal commit state;
- cancellation before acceptance has no side effects;
- cooperative compaction can yield and resume without publishing partial
  output.

Build tests:

- core engine builds without direct native filesystem access in portable
  configurations;
- memory backend builds for a WASM target;
- blocking adapter is excluded or unsupported where blocking is not safe.

## 13. Implementation Staging

Recommended staging:

1. introduce async primary API types while keeping existing behavior through a
   compatibility adapter;
2. define storage capability structs and typed unsupported-capability errors;
3. route memory backend through the async storage contract;
4. route native-file backend through the storage contract;
5. move manifest publish behind the backend publish operation;
6. convert range and prefix cursors to async advancement;
7. add blocking native adapter;
8. add portable build checks for memory mode on WASM;
9. add WASI and browser backends after the core protocol is stable.

Each stage must preserve MVCC visibility, WAL recovery, manifest publish, and
snapshot safety.

## 14. Relationship To The Foreground Write Path

The async-first storage protocol and the no-global-lock foreground write-path
protocol are designed together, but they should not land as one implementation
slice.

Implementation order:

1. land the async primary API and blocking adapter without changing write-path
   concurrency;
2. land storage capabilities, typed unsupported-capability errors, and backend
   manifest publish without changing commit visibility;
3. define cancellation-safe write acceptance so a dropped caller future cannot
   leave a half-committed write;
4. land commit tracker and visible-sequence slot states behind the current write
   coordinator;
5. route persistent commits through WAL shard front doors only after recovery
   and cancellation tests exist for the storage boundary;
6. publish key-sharded immutable deltas and remove the global foreground writer
   bottleneck only after visible-sequence behavior is proven.

Rules:

- async API migration must not silently change MVCC, WAL, manifest, transaction,
  cursor, or compaction behavior;
- no-global-lock write-path work must not bypass backend durability capability
  checks;
- cancellation rules in this protocol apply to every later write-path stage;
- visible sequence, skipped slot, and shard-delta rules remain owned by the
  foreground write-path protocol;
- if implementation needs a different order, update both protocols before
  changing Rust code.
