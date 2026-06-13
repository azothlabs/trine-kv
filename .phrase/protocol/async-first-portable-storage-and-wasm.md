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

Platform I/O capability is operation-level. `PlatformAsyncIo` is a coarse
backend capability used for compatibility and diagnostics; it must not be the
only design language for platform-io work.

Required platform I/O operation classes:

```text
TruePlatformAsync
PlatformNativeAsyncButPartial
PlatformManagedFallback
BlockingFallback
Unsupported
```

Class meanings:

- `TruePlatformAsync`: the complete Trine operation is submitted through a real
  platform async completion mechanism for the selected target backend.
- `PlatformNativeAsyncButPartial`: one or more lower-level steps can use a
  native async primitive, but the complete Trine operation still includes
  fallback-classified work.
- `PlatformManagedFallback`: the platform driver owns the operation and hides
  target mechanics from the KV engine, but completion is managed by a fallback
  path inside the platform layer.
- `BlockingFallback`: the platform driver uses an explicit blocking fallback
  for an operation that has no real async platform operation yet.
- `Unsupported`: the target cannot provide the operation through platform-io.

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
- manifest read and publish must be available through async storage-trait calls
  before browser persistence is wired;
- in-memory manifest state advances only after the async or sync publish
  operation succeeds;
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
- storage futures and storage object/backend trait bounds remain thread-safe on
  native and WASI targets, but `wasm32-unknown-unknown` browser storage backends
  may use thread-local futures and objects because browser storage handles are
  often tied to the main event loop;
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
Db::run_maintenance_with_budget(budget).await -> Result<MaintenanceOutcome>
Db::compact_range_with_budget(range, budget).await -> Result<MaintenanceOutcome>
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
- a worker may yield and resume by replanning from the current manifest and
  in-memory versions;
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
- may use the opt-in `platform-io` runtime mode as the cross-platform async I/O
  abstraction for native-file length, owned random reads, whole-object reads,
  temp-write publish operations, WAL append-object opening/append/persist/rewrite,
  object delete, directory create/sync/listing, and writer lease acquisition
  when the build enables the matching platform I/O feature;
- must keep Linux, Windows, macOS, BSD/other Unix, and fallback mechanics behind
  the platform-io driver contract so the KV engine awaits Trine operations
  rather than OS APIs;
- may submit directory and object listing through the platform driver as a
  separately reported `BlockingFallback` until that driver exposes a real async
  directory enumeration operation;
- may add stronger platform-specific implementations behind the same contract.
- must report `BlockingAdapter` separately from `PlatformAsyncIo`; using a
  bounded blocking pool must not be described as true platform async I/O.
- must report platform backend fallback tasks separately from true platform
  async I/O tasks when a platform driver is selected but a target or operation
  cannot honestly use a native async file primitive.
- must report platform-driver blocking fallback tasks separately from both
  Trine's bounded blocking adapter and true platform async I/O tasks.
- must advertise `PlatformAsyncIo` only when the current target has at least
  one true Trine-level platform async storage operation. A target whose current
  operations are all fallback-classified may still route work
  through the platform driver when the user selects `RuntimeOptions::platform_io`,
  but it must report fallback task counters and must not advertise
  `PlatformAsyncIo`.
- native persistent async writes may use the async WAL lane completion only when
  the native-file backend advertises `PlatformAsyncIo`; fallback targets must
  keep the bounded sync-adapter write boundary.
- native async writes must not append directly to WAL files outside
  `WalFrontDoor`; sync and async writers share that lane boundary.
- public native async flush may use async storage completions for table writes,
  manifest publish, directory sync, and WAL rewrite only when the native-file
  backend advertises `PlatformAsyncIo`; fallback targets must keep the bounded
  sync-adapter flush boundary.
- native async flush must not hold std locks or the publish barrier in the
  public caller future across await; native manifest publish may use a small
  runtime blocking task that holds the existing manifest/publish locks while it
  waits for platform storage completion, so sync and async manifest mutators
  stay serialized.

#### Native Platform Backend Matrix

Current platform backend classifications are operation-level. Linux has current
true async evidence for many operations. Windows, macOS, and BSD/Solaris-family
targets have native async evidence for selected lower-level file data paths but
still report partial rows when a complete Trine operation includes non-native
steps. Generic fallback targets remain managed or blocking fallback until a
target-specific backend is proven behind the same platform-io contract.

```text
Operation                     Linux            Windows          macOS            BSD/Solaris      Generic fallback
length lookup                 TruePlatformAsync ManagedFallback  ManagedFallback  ManagedFallback  ManagedFallback
random read                   TruePlatformAsync Partial          Partial          Partial          ManagedFallback
whole-object read             TruePlatformAsync Partial          Partial          Partial          ManagedFallback
temp write + rename publish   TruePlatformAsync Partial          Partial          Partial          ManagedFallback
append open                   TruePlatformAsync ManagedFallback  Partial          ManagedFallback  ManagedFallback
append                        TruePlatformAsync Partial          Partial          Partial          ManagedFallback
persist/fsync                 TruePlatformAsync ManagedFallback  Partial          Partial          ManagedFallback
WAL rewrite                   TruePlatformAsync Partial          Partial          Partial          ManagedFallback
delete                        TruePlatformAsync ManagedFallback  ManagedFallback  ManagedFallback  ManagedFallback
directory create              TruePlatformAsync ManagedFallback  ManagedFallback  ManagedFallback  ManagedFallback
directory sync                TruePlatformAsync ManagedFallback  Partial          Partial          ManagedFallback
directory listing             BlockingFallback  BlockingFallback BlockingFallback BlockingFallback BlockingFallback
writer lease                  TruePlatformAsync Partial          Partial          Partial          ManagedFallback/Unsupported
```

Legend: `Partial` means `PlatformNativeAsyncButPartial`, and
`ManagedFallback` means `PlatformManagedFallback`.

With the current backend matrix, `RuntimeOptions::platform_io()` advertises
`PlatformAsyncIo` when the `platform-io` Cargo feature is enabled on Linux,
Windows, macOS, FreeBSD, illumos, and Solaris-family targets. This coarse
capability means at least one operation has true or partial native async
coverage; the per-operation matrix remains the source of truth for complete
operation class. The Linux directory listing row remains `BlockingFallback`
because the selected Linux async stack exposes no directory enumeration
operation for a complete Trine listing request; Trine therefore treats listing
as an explicit platform driver fallback instead of an unexamined gap.
On Windows, the selected backend opens files with overlapped support and submits
positioned `ReadFile` / `WriteFile` operations through IOCP, but file open,
metadata, sync, rename, delete, directory creation/listing, and related publish
steps still include blocking or helper-managed work. Therefore Windows rows
with IOCP read/write substeps are `Partial`, rows without such a substep are
`ManagedFallback`, and directory listing remains `BlockingFallback` until a
complete Trine operation can be proven end to end.

On macOS, platform-io uses Apple `DispatchIO` through the `dispatch2` crate for
the file data path: open/create through `dispatch_io_create_with_path`, random
and whole-object read through `dispatch_io_read`, and write-like paths through
`dispatch_io_write`. Operations remain `Partial` when they also need metadata,
rename, delete, directory creation/listing, or durability work without an Apple
file-completion primitive. Directory listing remains `BlockingFallback`.

On FreeBSD and Solaris-family targets, the selected backend exposes libc AIO for
some regular-file read, write, and sync primitives. Complete Trine operations
that include those primitives are `PlatformNativeAsyncButPartial` while open,
stat, rename, delete, directory, listing, or lease steps remain blocking or
helper-managed. Other Unix targets remain `PlatformManagedFallback` unless a
target-specific audit proves stronger behavior.

On non-Linux targets it can still route native storage through the platform
driver, but current operations remain partial or fallback-classified until
stronger target backends exist. That current-state classification is not the
platform-io goal; the goal is that each platform backend handles its own async
or fallback mechanics while preserving one Trine operation boundary for KV code.

Public diagnostics expose that boundary through
`DbStats::storage_platform_io_operations`. Each operation has class counters for
true platform async, partial native async, platform-managed fallback, blocking
fallback, and unsupported completions. `PlatformIoOperationStats::total()` sums
all operation rows into one class counter set for dashboards and health checks;
individual rows remain the source of truth when a caller needs to know whether,
for example, random reads differ from directory listing on the selected target.

### 9.2.1 Native Engine Path Revalidation

The native engine must be judged at the storage operation boundary, not by the
name of the public async method. Current evidence:

```text
Path                         Current native platform-io status
write / WAL append           awaits append and persist storage completions
public flush table writes     awaits temp write plus rename publish
public flush directory sync   awaits directory sync
public flush WAL rewrite      awaits WAL rewrite
public flush cleanup          still uses synchronous delete helpers
compaction output writes      still use synchronous table/blob writers
compaction directory sync     still uses synchronous directory sync
compaction cleanup            still uses synchronous delete helpers
native maintenance            still wraps sync maintenance in a blocking task
native close                  still wraps sync close in a blocking task
```

This means platform-io is correctly wired for the existing write and flush
storage completions, but the next engine async work should target native
compaction output writes and cleanup deletes before claiming broader engine
async coverage. Close remains a separate lifecycle boundary because it also
coordinates worker shutdown, the publish barrier, writer lease release, and
best-effort cleanup.

### 9.3 WASI Backend

The WASI backend is persistent only when the host grants suitable storage
capabilities.

Rules:

- `DbOptions::wasi_persistent(path)` selects the WASI host boundary and uses
  the host-preopened filesystem at `path` on WASI targets;
- non-WASI targets return `UnsupportedBackend` for this option;
- WASI persistent defaults to inline runtime execution with no background
  worker threads;
- option validation checks required runtime and durability capabilities during
  open;
- unsupported strict durability returns `UnsupportedDurability`;
- writer lease support is required for writable persistent open;
- recovery semantics match the main persistent protocol when required
  capabilities exist.

### 9.4 Browser Backend

The browser backend is async-only.

Rules:

- memory mode is always allowed when the build includes it;
- `DbOptions::browser_persistent()` selects the browser host boundary and
  keeps synchronous `Db::open` unsupported;
- `Db::open_async(DbOptions::browser_persistent_read_only())` uses OPFS on
  `wasm32-unknown-unknown` for read-only persistent open;
- browser persistent storage uses an OPFS-backed adapter behind Trine storage
  traits on `wasm32-unknown-unknown`;
- browser storage exposes WAL append, WAL rewrite, and writer lease operations
  through Trine storage traits;
- writable browser persistent open uses `Db::open_async` only, acquires a
  writer lease, opens or creates manifest state through browser storage,
  repairs safe temporary files, replays WAL, and attaches the browser WAL front
  door;
- browser async writes must append WAL before publishing memtable deltas;
- once a browser async write has been accepted, its internal commit task owns
  completion and the caller future is only a result waiter;
- browser async maintenance must own side-effecting flush, compaction, manifest
  publish installation, WAL rewrite, and cleanup work after acceptance;
- synchronous browser writes, synchronous browser bucket creation, and
  synchronous browser maintenance APIs must return explicit unsupported errors;
- persistent writable mode requires reliable writer leasing;
- if writer leasing or atomic manifest publish is unavailable, writable
  persistent open fails;
- browser storage accepts `Buffered` and `Flush` durability and rejects
  `SyncData` and `SyncAll`;
- durability strength is capability-reported and may be weaker than native
  strict sync;
- background maintenance must be cooperative and budgeted;
- storage futures and object handles may be thread-local on
  `wasm32-unknown-unknown`;
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
- WAL object reads, WAL object listing, and recovery stream reads must be
  available through async storage-trait calls for browser read-only recovery;
- manifest-referenced table and blob reads use async storage-trait calls for
  browser read-only recovery;
- safe temporary file checks, referenced blob validation, and unreferenced
  table/blob checks must have async storage-trait paths before a browser
  persistent open is accepted;
- safe temporary repair and recovery-report writes must have async
  storage-trait paths before browser writable persistent open is accepted;
- browser writable recovery must acquire the writer lease before repairing safe
  temporary files or accepting writes;
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
- blocking-adapter queued/submitted/completed/rejected task counts;
- blocking adapter call count when enabled;
- whether the storage backend uses a blocking adapter or platform async I/O;
- true platform async task count;
- platform backend fallback count;
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
