# Changelog

All public crate releases use Semantic Versioning.

## Unreleased

No unreleased changes.

## 0.6.0 - 2026-07-25

Toolchain and audit-hardening release. This release keeps the v1 public API and
storage contracts while raising the minimum supported Rust version from 1.85
to 1.95, which starts a new pre-`1.0` minor line.

### Added

- Added requirement-first durable acceptance coverage for branch lineages,
  resumable-upload maintenance, leased-only access, leases, physical holds,
  cross-bucket recovery, and unpolled asynchronous mutations. The
  backend-neutral corpus runs unchanged on native storage and qualified R2;
  synchronous branch data operations use a separate durable-native profile.
- Added a persistent randomized branch/reference-model test, executable
  acceptance traceability checks, and an explicit upload-maintenance lifecycle
  decision table validated by focused mutation testing.
- Added explicit object-store content-reclamation qualification. A host-retained
  provider-evidence digest and live probes of both protected content path
  families produce a capability scoped to one database prefix.
- Object metadata now retains the provider's opaque version identity so
  versioned and delete-marker behavior can fail closed.

### Changed

- CI now runs native, WASI, and browser verification as independent jobs. This
  keeps target failures attributable and allows the three release gates to run
  in parallel.
- The crate, repository toolchain, CI, publishing, production-evidence
  workflows, documentation, and examples now use Rust 1.95 and the `0.6`
  release line consistently.
- Native platform I/O now uses the compio 0.19 dependency family, removing the
  transitive unmaintained `paste` macro.
- The wasm-bindgen CLI is installed by the same Rust 1.95 toolchain used to
  compile and test Trine; the auxiliary Rust 1.88 installation is removed.
- Source and test code now satisfy the Rust 1.95 strict Clippy gate, including
  let-chain, saturating arithmetic, derived-default, and integer-multiple
  idioms; unused scaffold-only version and WAL header types were removed.
- Prepared content sweeps use the `TRNCRSW2` protected record, bind the backend
  and provider-evidence digest, and require the same qualification after reopen.
  There is no migration from an unfinished `TRNCRSW1` sweep.
- Qualified cloud deletion checks for a provider version before DELETE and
  requires immediate HEAD, GET, and LIST absence afterward. Any uncertainty
  leaves the sweep Prepared for retry.

### Fixed

- Host-only test harness dependencies are now excluded from browser and WASI
  target builds. In particular, WASI persistence tests no longer compile
  `proptest`'s Unix/Windows-only process-timeout dependency chain.
- Platform-I/O flush verification now attributes bounded-adapter work to
  explicit manifest transitions or storage fallbacks instead of assuming a
  fixed task count. This keeps whole-operation fallback detectable while
  allowing durable file-ID reservation to add legitimate manifest cutovers.
- Commit ordering, bucket deletion, object-store lease/CAS/WAL fencing,
  recovery bounds, worker completion, browser publication, blob range reads,
  cache capacity, table validation/statistics, dependency advisories, and
  release-automation findings from the full-code audit are resolved at their
  owning invariants.

## 0.5.13 - 2026-07-18

Browser Worker persistence and release-maintenance patch. This release remains
compatible with the `0.5.x` public API and storage format while correcting the
Worker execution paths and keeping hosted verification on supported runtimes.

### Fixed

- Browser persistence now selects synchronous OPFS access handles only inside
  DedicatedWorker. SharedWorker keeps the async OPFS path used by shared
  multi-tab database instances, and the browser gate runs both contexts as real
  wasm-bindgen Worker targets.
- Browser compaction releases its own input-table handles before awaiting OPFS
  cleanup, so obsolete inputs are deleted before a successful compaction
  returns and do not make the next fail-closed reopen report orphaned files.
- Browser persistence tests preserve the default and named-bucket options used
  at creation when reopening, allowing persisted reads to run instead of
  correctly stopping at `InvalidOptions` because of test configuration drift.
- Browser CI and publish tests disable wasm-bindgen-test's optional COOP/COEP
  origin-isolation headers, which prevent Chrome SharedWorker from opening its
  OPFS root in this test environment.
- Production WASI builds no longer retain native/browser-only CommitTracker
  waker machinery, and WASI warnings now fail CI and publishing checks.
- CI and publishing install `wasm-bindgen-cli 0.2.122` with Rust 1.88 while
  continuing to compile and test Trine with its declared Rust 1.85 MSRV.
- The production-evidence workflow uses the valid matrix context for report
  names, and all workflows use the Node.js 24-based `actions/checkout@v7`.

### Changed

- Crates.io packages exclude repository-only integration tests, benchmarks,
  and extended documentation, reducing the compressed package from about
  548 KiB to about 417 KiB while keeping those assets in Git and CI.
- Browser persistence guidance now distinguishes Window, DedicatedWorker, and
  SharedWorker behavior, and bucket documentation states that explicit options
  must match the options stored when the bucket was created.
- The release drift gate now checks the browser runner environment, WASI warning
  policy, package exclusions, and checkout action runtime.

## 0.5.12 - 2026-07-18

Production-evidence and maintenance-correctness release. This patch keeps the
`0.5.x` public API and storage format compatible while turning performance,
abnormal-process recovery, and returned I/O failures into repeatable release
checks.

### Added

- A bounded ten-row production benchmark profile and same-runner comparison
  gate now cover representative write, read, scan, recovery, flush,
  compaction, large-value, and cold-open work without treating shared-runner
  timing as an application SLA.
- Configurable forced-process-exit recovery and concurrent mixed-load soak
  harnesses now verify confirmed writes, snapshots, maintenance, close, and
  reopen behavior with reproducible reports.
- A path-scoped test-only storage fault model and six-scenario destructive
  matrix now cover WAL append and persist failures, table and manifest publish
  failures, directory-sync failure, WAL rewrite, object deletion, repair,
  retry, and reopen behavior.
- The production-evidence workflow now runs target-native maturity checks on
  Linux, macOS, and Windows and retains JSONL and paired performance artifacts.

### Fixed

- A successful commit no longer returns before its sequence is continuously
  visible when an earlier concurrent commit slot is still open.
- Flush and table-replacing maintenance no longer race, and compaction plans
  are rejected after the source LSM version changes. Same-bucket compaction and
  Blob GC replacement are serialized while different buckets retain
  independent progress.
- The publish workflow keeps Cargo-version and changelog validation in the
  intended release step and runs recovery, destructive, and mixed-load gates
  before publishing.

### Changed

- Normal CI now includes macOS platform-I/O runtime coverage in addition to the
  existing Windows path.
- README production guidance now leads with pre-`1.0` adoption boundaries,
  evidence, a runnable verification path, and explicit limitations.
- An executable documentation-drift check now keeps active dependency
  examples, release commands, maturity claims, local links, CI, and publishing
  automation aligned.

## 0.5.11 - 2026-07-09

Browser worker OPFS groundwork release. This patch keeps the `0.5.x` storage
format compatible while removing the Window-only OPFS-root dependency and
adding the DedicatedWorker synchronous-access path. The SharedWorker backend
selection and Worker test harness are corrected in `0.5.13`.

### Added

- Browser persistent storage now opens the OPFS root through the current
  global's `navigator.storage.getDirectory()`, so the backend is no longer tied
  to a `window` object.
- DedicatedWorker browser file operations now use
  `FileSystemSyncAccessHandle` for whole-object reads, random reads, object
  writes, WAL append, manifest publish, and WAL rewrite.
- Initial Worker round-trip and sync-access probes were added; `0.5.13` moves
  them to wasm-bindgen's real DedicatedWorker and SharedWorker test targets.

### Fixed

- Worker OPFS-root discovery no longer fails merely because
  `web_sys::window()` is unavailable.
- Worker contexts that expose OPFS without `FileSystemSyncAccessHandle` now
  return `UnsupportedBackend` instead of falling through to the Window-oriented
  async file path.
- OPFS sync access handles are scoped to one storage operation and closed before
  returning, avoiding a live-handle conflict with later operations on the same
  file.

### Changed

- Browser persistence docs now describe the Window and worker backend split and
  the worker requirement for `FileSystemSyncAccessHandle`.

## 0.5.10 - 2026-07-09

WASI and browser persistence hardening release. This patch keeps the `0.5.x`
storage format compatible while making host-selected persistent backends more
usable and better covered by runtime verification.

### Added

- **WASI persistent open support**: `DbOptions::wasi_persistent` now uses the
  host-preopened filesystem on `wasm32-wasip1`, including inline WAL behavior,
  WASI-safe writer lease handling, and startup recovery for interrupted writes.
- **Browser persistent namespace options**:
  `DbOptions::browser_persistent`, `DbOptions::browser_persistent_at`,
  `DbOptions::browser_persistent_read_only`, and
  `DbOptions::browser_persistent_read_only_at` select OPFS-backed browser
  storage namespaces explicitly.
- **`trine_kv::browser` storage helpers**:
  `browser_storage_estimate`, `browser_persistent_storage_granted`, and
  `request_browser_persistent_storage` expose the browser storage-manager
  checks callers need before keeping important local data in OPFS.
- CI and publish verification now include targeted WASI runtime checks and a
  real browser OPFS/Web Locks integration gate.

### Fixed

- WASI flush, compaction, blob cleanup, and WAL paths now honor the host
  backend's `Flush` durability boundary instead of requesting unsupported file
  sync modes.
- WASI startup recovery now treats stale `LOCK` markers as repair candidates
  instead of letting a leftover marker permanently block reopen.
- Browser WAL appends now write only new bytes through OPFS, serialize appends
  per WAL shard, and honor explicit `WalShardPolicy` choices while defaulting
  host-selected WASM persistence to one WAL lane.
- Browser namespace paths are normalized before OPFS access and Web Locks
  naming, so path aliases share the same writer lease.
- Read-only browser open now fails with not-found when the selected namespace
  has no manifest instead of creating missing storage.
- Browser bucket drop, safe-temporary repair, blob-backed reads, oversized
  manifest rejection, and unflushed WAL reopen are covered by the browser
  integration test.

### Changed

- Browser persistence is documented as async-only and operation-level
  non-cancelable after first poll for writes and maintenance.
- WASI and browser persistent options now default to `Flush`, and unsupported
  durability modes return typed errors.

## 0.5.9 - 2026-07-06

Maintenance hardening release. This patch keeps the `0.5.x` storage format
compatible while tightening recovery, background maintenance, native WAL
durability, and platform-specific build coverage.

### Added

- **`S3ClientOptions` and `ObjectStoreClient::s3_with_options`**: S3-compatible
  endpoints can now opt in to plain HTTP explicitly for local or otherwise
  trusted deployments.

### Fixed

- Multi-shard native group commit now writes confirmed WAL marker temporary
  files with shard-distinct names, avoiding a race where concurrent marker
  publishes could share `trine.wal.tmp` and fail with `NotFound`.
- SSTable reads and WAL recovery now reject additional short, malformed, or
  inconsistent inputs before they can be treated as durable state.
- Object-store maintenance and blob cleanup paths now preserve the relevant
  state and error category instead of collapsing background failures into a
  generic corruption report.
- The wasm background-worker unsupported-backend path now resolves the crate
  error type under wasm checks.
- Platform I/O tests now cover the native read-append path and align writer
  lease expectations with each platform driver.

### Changed

- Large database, storage, table, WAL, blob, manifest, and benchmark modules
  were split into focused submodules without changing the public storage format.
- Native open, iterator scan grouping, and maintenance internals were refactored
  to reduce duplicated state assembly and hidden invariants.
- `ObjectStoreClient::s3` keeps HTTP disabled even for custom endpoints; use
  `s3_with_options` with `S3ClientOptions::allow_http` for local MinIO-style
  endpoints.

## 0.5.8 - 2026-06-29

Transaction and native durability hardening release. This patch keeps the
`0.5.x` storage format compatible while closing a flush race in serializable
transaction conflict checks and strengthening local rename publish durability.

### Fixed

- Transaction conflict validation now captures the relevant memtable sources
  before reading the table version, matching the engine's read ordering and
  preventing a flush handoff from hiding a committed write from conflict checks.
- Native file publishes now request a parent-directory sync after rename when
  strict durability requires the directory entry to be stable.
- Ordinary native object writes now use the same parent-directory sync boundary
  for `SyncAll` and `SyncAllStrict` durability modes.

### Changed

- Clarified object-store trust-mode comments so callers can distinguish
  production health checks from the trusted-client fast path without changing
  runtime behavior.

## 0.5.7 - 2026-06-28

Object-store branch metadata release. This patch keeps the `0.5.x` storage
format compatible while making durable branch creation and deletion usable on
object-store backends.

### Added

- **`Db::create_checkpoint_at`**: async checkpoint creation for an exact retained
  read version, including object-store manifest CAS support.
- **`Db::create_branch_at`**: async durable branch creation from an exact
  retained read version.
- **`Db::delete_branch_async`**: async durable branch deletion for object-store
  backends, including checkpoint release, branch data bucket cleanup, and branch
  registry updates.

## 0.5.6 - 2026-06-22

Object-store adapter health-check release. This patch moves the object-client
contract probe out of the default object-store open path, so production opens
stay cheap while deployments still have an explicit way to validate custom
adapters before trusting them.

### Changed

- **Object-store opens now trust the supplied `ObjectClient` by default.**
  `DbOptions::object_client_trust` defaults to
  `ObjectClientTrustMode::Trusted`, so `Db::open_object_store*` no longer writes
  temporary probe objects during normal writable opens. This avoids extra
  object-store requests, latency, cleanup risk, and permission requirements on
  every open.
- **Open-time probing is now opt-in.**
  `ObjectClientTrustMode::VerifyOnOpen` preserves the previous fail-closed
  behavior for adapter development, temporary diagnostics, or high-risk
  rollouts. When storage and WAL clients are the same shared `Arc`, the probe is
  run only once.

### Added

- **`verify_object_client_contract(client, prefix)`**: a public health-check
  helper for CI, process startup, and deployment checks. It validates same-key
  `put`, `head`, `get`, `IfNoneMatch`, `IfMatch`, and ETag advancement using a
  temporary object under the supplied prefix, then deletes the probe key.
- **`DbOptions::with_object_client_trust`** and
  **`ObjectClientTrustMode`**: explicit configuration for whether object-store
  open should trust the client or run the contract probe during writable open.

## 0.5.5 - 2026-06-20

Object-store production hardening release. This patch keeps the `0.5.x`
storage format compatible while tightening confirmed-write recovery, object
client assumptions, resource bounds, and the object-store WAL path.

### Added

- **Split object-store WAL tier open APIs**:
  `Db::open_object_store_with_wal` and `Db::open_object_store_with_wal_at` let a
  deployment use one `ObjectClient` for manifest/table/blob storage and another
  for the confirmed-write WAL, writer lease, and WAL head. This supports
  placing the write durability sink on a lower-latency tier without changing
  the table storage tier.
- **Object-store WAL group commit scheduling**: queued durable object-store
  commits can share one WAL segment publish and one WAL-head conditional write.
  The writer only advances the remote head after contiguous commit frames are
  present in the segment.
- **Writable object-store `ObjectClient` contract probe**: writable open now
  checks same-key `put`, `head`, `get`, `IfNoneMatch`, `IfMatch`, and ETag
  advancement before taking ownership, so unsafe adapters fail at open instead
  of creating latent durability risk.
- **Configurable write byte limits**:
  `DbOptions::max_key_bytes` / `with_max_key_bytes` and
  `DbOptions::max_value_bytes` / `with_max_value_bytes` bound accepted user
  keys, range-delete bounds, and values. Defaults remain compatible with the
  previous 64MiB internal safety ceiling.
- **Real S3-compatible object-store runtime support** under the `s3` feature:
  the object WAL worker can drive Tokio-backed S3 futures instead of requiring
  the caller thread to provide a reactor.

### Fixed

- Object-store recovery and read-only refresh now reject missing, truncated, or
  non-contiguous confirmed WAL frames instead of trusting a higher remote WAL
  head after a short or corrupt segment read.
- Object-store writer leases now refuse a live second writer and allow takeover
  only after expiry; graceful close releases the lease by expiring it while
  preserving the WAL head.
- Object-store open now rejects file-style sync durability modes that the
  backend cannot honor.
- Object-store reads use bounded range reads after `HEAD` size validation and
  reject short range reads, avoiding whole-object allocation after a misleading
  or stale metadata response.
- Native async close now waits for admitted commit, flush, or compaction publish
  activity before releasing the writer lease.
- Native-file writer leases are held by OS file locks on the open `LOCK` handle,
  preventing stale diagnostic `LOCK` contents from blocking crash recovery.
- Manifest, WAL, SSTable, blob, cursor, and object read paths now reject
  oversized lengths before unbounded buffer allocation or LZ4 decode.

## 0.5.4 - 2026-06-18

Enforces the object-store single-writer guarantee, and resets the manifest
format version. The crate has no users yet, so this ships as a patch despite the
storage-format reset; the public API change is additive (a new variant on the
`#[non_exhaustive]` `Error` enum).

### Changed

- **Manifest format version reset to 1** (was an accumulated `11`). Only the
  current format is read; any earlier on-disk manifest is rejected
  (`UnsupportedFormat`). Create a fresh database — there is no migration. A
  structured `vX.Y.Z` format-version scheme is preferred going forward.

### Added

- **Writer-lease fencing is now enforced at manifest publish (object-store
  backend).** Previously the object-store writer lease was acquired but its epoch
  was never checked, so a stale or partitioned prior owner was not actually
  fenced and two writers over one key prefix could both publish via
  compare-and-swap retry (split brain). The manifest now records the publishing
  writer's fencing epoch; a publish stamps the holder's epoch and is rejected
  with the new **`Error::Fenced`** — without retrying — when the current manifest
  carries a higher epoch, so a displaced owner is stopped instead of overwriting
  the new one. A writable open claims its epoch immediately, fencing a displaced
  owner before the new owner's first flush. The filesystem backend is unaffected
  (its single-writer guarantee remains the `LOCK` file).

## 0.5.3 - 2026-06-18

Additive release: makes the object-store `ObjectClient` a first-class WAL
durability sink for a higher layer (e.g. a multi-tenant service). No hot-path or
storage-format change; fully compatible with `0.5.x`.

### Added

- **`is_wal_object_key(key)`**: classifies an object key as a write-ahead-log
  object (`trine.wal` / `trine.wal.shard-NNNN`) by its final path segment, so a
  custom shared `ObjectClient` can route or coalesce the WAL writes (the
  durability-defining ones) distinctly from bulk data. The `ObjectClient` docs
  now state the contract: a database writes its WAL through that client and acks
  a commit only after the WAL `put` is durable, and one `Arc`-shared client
  across databases is the seam for cross-database group commit — with no engine
  change.

## 0.5.2 - 2026-06-17

Additive release for embedders that branch from a higher layer. No hot-path or
storage-format change; fully compatible with `0.5.0`/`0.5.1`.

### Added

- **`Db::branch_info`** and **`BranchInfo`** (`fork()`, `parent()`): returns a
  durable branch's fork `ReadVersion` and parent branch without assembling a
  read chain or opening a data bucket. This lets a higher layer that stores its
  own divergent data (e.g. an engine whose writes must commit as one atomic
  multi-bucket batch, which the per-key `Branch` overlay cannot express) reuse
  the durable branch lifecycle this crate manages — the fork checkpoint that
  survives restarts and aggressive GC, the registry, and nesting — while doing
  its own `snapshot_at` fall-through and walking its own ancestry.

## 0.5.1 - 2026-06-17

Branching release. Adds copy-on-write branches, durable named branches,
branch-of-branch nesting, fork-retention pinning, and a bucket-drop primitive
across all backends. No hot-path or storage-format change; databases written by
`0.5.0` are fully compatible.

### Added

- **Copy-on-write branches** (`Db::branch_at`, `Db::branch_from_latest`): fork
  from any pinned snapshot in O(1) (no data copied) with an in-memory overlay
  for divergent writes. Reads fall through to the frozen parent snapshot; the
  parent is unaffected. Supports time travel by forking at any retained version.
- **Durable named branches** (`Db::create_branch`, `open_branch`,
  `list_branches`): stores divergent writes in per-branch reserved buckets,
  reusing the existing `LsmTree` machinery for durability, compaction, and
  recovery with no manifest or WAL change and no hot-path overhead. Deletes are
  tombstone-tagged so a branch can hide a parent key without a parent write.
  Branch-level activity cannot affect the parent's compaction or read
  amplification because branch data lives in its own buckets.
- **Branch-of-branch nesting** (`Db::create_branch_from`): durable branches can
  fork from other branches, forming a git-style DAG. The registry records each
  branch's parent; `open_branch` assembles a leaf-first read chain where each
  ancestor is frozen at the version its child forked it. `delete_branch` refuses
  while a branch still has live children.
- **Fork-retention pinning**: a durable branch creates a checkpoint at fork time,
  so the parent's GC retains the history the branch reads across restarts and
  with `keep_last_read_versions = 1`. `delete_branch` removes the checkpoint to
  release the pin. No new GC subsystem; reuses the existing retained-history
  floor.
- **`Branch::range`** is now a lazy k-way merge iterator: streams the branch,
  each ancestor (frozen at its fork version), and the root in sorted order
  without materializing an intermediate `BTreeMap`.
- **`Db::drop_bucket_sync` / `Db::drop_bucket`**: removes a bucket and reclaims
  its storage. `drop_bucket_sync` covers in-memory and native backends (flushes,
  advances the WAL replay floor so recovery never replays into a dropped bucket,
  retires SSTable and blob files, refcount-guarded so active readers keep working
  until they drop). `drop_bucket` adds object-store support (CAS publish removes
  the bucket from the manifest; orphan GC reclaims unreferenced objects) and
  browser/WASI support (WASI routes through the native path; browser publishes
  the bucket removal to the IndexedDB-backed manifest and retires blob files via
  async cleanup). Bucket-drop now covers every backend.
- `delete_branch` drops a deleted branch's data buckets outright (falling back to
  clearing on unsupported backends), so a deleted branch leaves no garbage and a
  same-named branch created later starts clean.
- `Db::create_checkpoint_at_sync(name, version)`: checkpoint a specific retained
  past version (used internally by branch-fork pinning; `create_checkpoint_sync`
  is refactored to a special case).

### Fixed

- `delete_branch` previously left stale rows in the branch's data buckets; a
  same-named branch would inherit them. Now `delete_range` clears each written
  bucket so space is reclaimed by compaction and correctness is preserved.
- Dead-code warning for `durability_is_strict` on non-macOS targets.

## 0.5.0 - 2026-06-15

Performance, durability, and space-reclamation release. It adds guard-aware
non-uniform compaction, layered (Monkey-style) per-level filter allocation, WAL
group commit, read-path point-lookup optimizations, delete/GC space reclamation,
and a tiered fsync durability model whose default is fast. Pre-`1.0`, so the
breaking storage-format and durability-semantics changes below bump the minor
version.

### Breaking

- Manifest format is now version `11` with a clean break: only the current
  format is read (`MIN_SUPPORTED == MANIFEST_VERSION`). Databases written by
  earlier versions are rejected with `UnsupportedFormat` instead of migrated.
- `DurabilityMode::SyncData` and `DurabilityMode::SyncAll` are now **non-strict**:
  on macOS they issue a plain `fsync`, which flushes to the drive but is not
  guaranteed to survive sudden power loss (it does survive a process crash or
  kernel panic). Previously they went through the standard library, which always
  issues `F_FULLFSYNC` on macOS. Use the new strict tier for power-loss
  durability. On Linux/Windows behavior is unchanged (their `fsync` already
  flushes durably).

### Added

- Guard-aware, non-uniform per-level compaction: a picker that compacts based on
  per-level pressure and rewrite savings, gates local L0 compaction on real
  rewrite savings, and derives its guards at runtime (no storage-format change).
- Layered (Monkey-style) per-level Bloom/filter allocation via
  `FilterDepthCurve` (`Auto`, `Uniform`, `Custom { step, floor }`) applied to
  both point and prefix filters: deeper levels get fewer bits where a miss is a
  cheap local read, configurable per bucket.
- `FilterDepthCurve::CostWeighted { step, ceil }`: an opt-in ascending
  allocation for remote/cold backends (the `s3` feature) where a deep-level
  filter miss costs a network read. Default unchanged; classic Monkey stays the
  default for local storage.
- WAL group commit with a configurable shard count, plus a scenario-adaptive
  `WalShardPolicy` (`Auto` / `Fixed`): one lane under the per-commit-fsync
  regime so the worker coalesces concurrent commits under a single sync.
- `DurabilityMode::SyncAllStrict` and `WriteOptions::sync_all_strict()`: strict
  full sync that flushes through the drive's volatile cache. On macOS this is
  `fcntl(F_FULLFSYNC)` (the only call that survives sudden power loss), with a
  `fsync` fallback when a filesystem does not support it. Configurable as the
  database-wide floor via `DbOptions::with_durability(DurabilityMode::SyncAllStrict)`
  or per write. On macOS the non-strict default measured ~21x faster than strict
  for single-key sync writes.
- A tombstone-debt compaction trigger and delete/GC health observability
  (scan read-amplification and snapshot version-debt stats, per-level filter
  stats, compaction trigger/skip stats).

### Changed

- Point-read optimizations: small batched point reads and negative point lookups
  are faster, hot table metadata is protected in the block cache, and L0 point
  reads are pruned by table key bounds.
- Range deletes now reclaim space instead of lingering on the read path:
  compaction drops range-tombstone-covered point records at the source when
  retention-safe (measured scan read amplification dropped from ~10x to ~1x on
  the covered-data diagnostic), drops wholly-covered tables by file without
  rewriting them, and scans skip tables fully hidden by a visible tombstone.
- Obsolete table files are reclaimed by per-table liveness (`Arc` strong count)
  instead of being blocked whenever any snapshot is open, so a single long-lived
  snapshot no longer stalls all space reclamation.
- File-sync durability is centralized in one `durability` abstraction shared by
  the std and native backends.

### Fixed

- The native macOS DispatchIO backend (`platform-io-native`) previously issued a
  plain `fsync` for every sync mode; it now uses the same strict/non-strict
  decision as the std backend.

## 0.4.1 - 2026-06-14

Performance release. This release tunes write-path, storage-read, blob, and
background-maintenance hot paths and trims a transitive dependency. The public
API and the storage format are unchanged from `0.4.0`; this is a pre-`1.0` patch
release.

### Changed

- Table writer reuses encoded table metadata across blocking writes, returns the
  loaded table from async writes, and skips re-sorting already ordered table
  payloads.
- Level-merge rewrite caches native blob read objects by file id and inlines
  retained `BlobIndex` values, avoiding repeated open, length, and header
  validation work for the same blob file.
- Background maintenance admission is now foreground-first: pressure maintenance
  runs in the foreground, background maintenance gets an internal pressure-sized
  budget, and post-commit background flush admission waits for a useful
  immutable-memtable batch instead of splitting work into many small manifest,
  persist, and directory-sync turns.
- Table metadata decodes from shared block payloads, storage reads reuse buffers
  as shared bytes, and the none-codec data-block path avoids a payload copy.
- Block-cache hit maintenance, blob maintenance cleanup publishing, prefix-scan
  cursor metadata, WAL dirty-lane persist, localized point batch setup, and
  writer lease reopen are each made cheaper on their hot paths.
- `lz4_flex` is pinned to `default-features = false` with only the checked block
  decode features Trine uses.

### Added

- Foreground/background maintenance contention benchmark diagnostics for
  classifying read/write latency under flush and compaction pressure.
- Grouped multi-run benchmark summaries.

### Fixed

- Startup recovery benchmark boundaries.

### Dependencies

- Dropped the `twox-hash` transitive dependency, which followed from narrowing
  `lz4_flex` to its checked-decode feature set.

### Notes

- Background flush admission now waits for a useful immutable-memtable batch, so
  tiny write bursts may stay in memory longer until pressure, an explicit flush,
  close, or later writes trigger maintenance.

## 0.4.0 - 2026-06-13

Platform I/O release. This release makes `platform-io` the portable async
native-file storage boundary and adds `platform-io-native` for native-first
operation support with thread-pool managed async fallback for remaining rows.
The public storage format is unchanged from `0.3.0`, but the feature surface and
runtime behavior are meaningful enough for a pre-`1.0` minor release.

### Added

- `platform-io` feature for Trine-owned bounded thread-pool async completion on
  native-thread targets.
- `platform-io-threadpool` feature alias for callers that want to name the
  thread-pool baseline explicitly.
- `platform-io-native` feature for native-first platform I/O on Linux, Windows,
  macOS, and other Unix targets, with operation-level fallback to the same
  managed thread pool where native support is partial.
- Platform I/O operation stats and backend matrix reporting for random reads,
  whole-object reads, temp-write plus rename publish, WAL append/persist,
  rewrite, delete, directory operations, and writer leases.
- `examples/platform_io.rs` as a checked feature-selection smoke path.
- `docs/platform-io.md` with feature guidance, operation classes, and
  verification commands.

### Changed

- Native-file async read, write, flush, compaction, cleanup, directory, and
  lease paths can now enter Trine's operation-level platform I/O driver when
  `RuntimeOptions::platform_io()` is selected.
- Windows directory sync now treats directory-handle `PermissionDenied` as a
  best-effort directory-sync boundary after file sync and rename have completed,
  matching the strongest behavior available on common Windows filesystems and CI
  runners.
- README, usage docs, durability docs, release docs, and CI now describe and
  verify the platform I/O feature matrix.

## 0.3.0 - 2026-06-12

Read-version and checkpoint release. This release adds public historical-read
cursor APIs and advances the manifest payload to v9 for durable checkpoint
metadata. Existing v8 manifests remain readable, but manifest v9 is a
storage-contract change, so this is a pre-`1.0` minor release.

### Added

- Public `ReadVersion` cursor type for committed database states, with
  `ReadVersion::ZERO`, `ReadVersion::from_u64`, and `ReadVersion::as_u64`.
- Historical read APIs:
  - `Db::latest_read_version`;
  - `Db::oldest_retained_read_version`;
  - `Db::snapshot_at`;
  - `Snapshot::read_version`;
  - `CommitInfo::read_version`;
  - `Transaction::read_version`.
- Named checkpoint APIs with async-first and sync forms:
  - `Db::create_checkpoint` and `Db::create_checkpoint_sync`;
  - `Db::delete_checkpoint` and `Db::delete_checkpoint_sync`;
  - `Db::checkpoint_read_version` and
    `Db::checkpoint_read_version_sync`.
- `DbOptions::with_keep_last_read_versions` for configured recent
  read-version retention.
- Typed errors for historical-read and checkpoint boundaries:
  - `Error::ReadVersionTooNew`;
  - `Error::ReadVersionExpired`;
  - `Error::CheckpointAlreadyExists`;
  - `Error::CheckpointNotFound`.

### Changed

- Manifest payload version advanced to v9 to store named checkpoint pins.
- Compaction cleanup now protects the effective retained floor from active
  snapshots, named checkpoints, and configured recent retention.
- Public documentation now presents `ReadVersion` as the application-facing
  historical-read cursor.
- Added the `read_versions` example covering retained read versions,
  checkpoint lookup after reopen, and expiration after checkpoint deletion.

### Removed

- Removed the public `Sequence` / `SnapshotSequence` surface before the `0.3.0`
  release. Engine commit ordering remains an internal implementation detail;
  applications should use `ReadVersion`, `CommitInfo::read_version`,
  `Snapshot::read_version`, `Transaction::read_version`, and
  `Db::latest_read_version`.

## 0.2.2 - 2026-06-10

Adds an object-storage backend: run a Trine database on S3 / S3-compatible
object storage (Cloudflare R2, MinIO, …) or any custom object store. Additive
and backward compatible; existing native, in-memory, WASI, and browser backends
are unchanged.

### Added

- Object-storage databases (async-only):
  - `Db::open_object_store(client, options)` and
    `Db::open_object_store_at(client, prefix, options)` (a key prefix lets
    several databases share one bucket), with `DbOptions::object_store()`.
  - Durability is WAL-less: a commit is durable once its memtable is flushed to
    objects and the manifest is published via a conditional-PUT
    compare-and-swap. Open, flush, reopen, named buckets, compaction, and
    orphan-object GC are all supported.
- Public `object_store` module — "bring your own object store": the
  `ObjectClient` trait plus `ETag`, `Precondition`, `PutIf`, `ObjectMeta`,
  `ObjectFuture`, and an `InMemoryObjectStore`. The manifest commit requires
  `put_if` to be a real conditional write (compare-and-swap).
- `s3` feature: `s3::ObjectStoreClient` adapts any `object_store::ObjectStore`
  (S3, GCS, Azure, MinIO/R2/Ceph, local, in-memory) to `ObjectClient`, including
  an `ObjectStoreClient::s3(bucket, region, endpoint)` convenience constructor
  with conditional PUT (`ETagMatch`) enabled. Verified end to end against
  Cloudflare R2.

### Internal

- Introduced a durability-substrate seam isolating the backend-divergent runtime
  operations (write-ahead log lifecycle + single-writer lease), and made manifest
  publishing conflict-aware (`PublishOutcome`), so the object store's
  compare-and-swap commit and the filesystem's atomic-rename commit share one
  code path. Filesystem behavior is byte-identical.

## 0.2.0 - 2026-06-03

Compatible API and performance release for the pre-`1.0` crate line.

### Added

- Added batched point-read APIs for default buckets, named buckets, and
  bucket readers:
  - `Db::get_many` and `Db::get_many_sync`;
  - `Bucket::get_many` and `Bucket::get_many_sync`;
  - `BucketReader::get_many`, `BucketReader::get_many_sync`,
    `BucketReader::get_many_owned`, and `BucketReader::get_many_owned_sync`.

### Improved

- Improved internal `get_many` point-read batching by deduplicating batch keys,
  preserving duplicate output positions, grouping table lookups, and sharing
  same-block persistent reads.
- Reduced repeated block metadata checks during prefix scans that continue
  within an already-loaded table block.
- Reduced cold table open positioned reads by decoding small table metadata from
  one temporary table-file buffer while keeping data-block reads lazy.
- Reused one native directory listing during cold reopen recovery checks and
  WAL discovery.
- Reduced clean read-only reopen work by skipping WAL shard content reads when
  every discovered shard is empty, while preserving WAL replay for non-empty
  shards.
- Reused native directory listing metadata across sync and async open paths for
  temporary-file checks, recovery checks, WAL discovery, and clean-WAL proof.

### Documentation

- Added benchmark notes for read-only cold-open breakdown and batched point
  reads.

## 0.1.1 - 2026-06-02

Patch release metadata correction after the initial `0.1.0` publish.

### Fixed

- Added the GitHub repository URL to crate metadata so crates.io can link back
  to the source repository.
- Updated README installation guidance with the crates.io package page and the
  dependency-focused `cargo add trine-kv` path.

## 0.1.0 - 2026-06-01

Initial packaged release candidate for the embedded LSM MVCC engine.

### Added

- Embedded LSM MVCC key-value database with in-memory and persistent modes.
- Built-in default bucket plus optional named buckets, point reads/writes,
  range scans, prefix scans, snapshots, optimistic transactions, and atomic
  write batches.
- WAL recovery, SSTable flush/read, manifest metadata, leveled compaction,
  block compression through `lz4_flex`, prefix filters, block cache stats, and
  Titan-like blob files for large values.
- Value-lazy range and prefix iterators for large-value workloads that need
  keys before reading blob bytes.
- Automatic blob Level Merge policy and snapshot-safe blob GC with batched
  stale-file rewriting.
- Read-only open, safe temporary file repair policy, durability notes, usage
  guide, quickstart examples, integration examples, release checklist, and
  benchmark baselines.
- Path-first `Db::open(path)` and `Db::open_sync(path)` APIs for ordinary
  persistent databases, with `DbOptions::memory()` as the explicit in-memory
  mode.
- Native path-based open defaults to `SyncAll` for confirmed writes; `Buffered`
  remains available as an explicit advanced mode for rebuildable or loss-tolerant
  data.
- Async-first database, bucket, iterator, value, transaction, flush,
  compaction, and maintenance APIs, with explicit `*_sync` adapters for
  synchronous callers.
- Runnable `quickstart` example covering async persistent open, writes,
  lazy scans, transaction commit, maintenance, read-only reopen, and storage
  runtime stats.
- Native platform I/O capability reporting, fallback observability, bounded
  sync adapter stats, and cooperative maintenance budgets.
- Explicit WASI persistent options for host-preopened filesystems on WASI
  targets, including `Db::open` through the host storage boundary.
- Browser persistent options backed by the async browser storage path on
  `wasm32-unknown-unknown`, including writable async open, Web Locks writer
  lease, WAL-backed async writes, async bucket creation, and async maintenance.

### Hardened

- Manifest publish installs in-memory state only after durable file publish
  succeeds.
- WAL, manifest, and table decoders reject impossible count fields before large
  allocation.
- WAL, manifest, SSTable, and blob checksum fields use CRC-32C, with storage
  format versions advanced for the pre-release format.
- Failed flush/compaction publish removes unpublished table/blob output files.
- Recovery validates referenced table/blob files and fails closed on missing or
  corrupt storage files.
- Browser synchronous mutation and maintenance paths return typed unsupported
  errors instead of bypassing async storage guarantees.
- Browser async writes and maintenance own side-effecting work after acceptance,
  so caller future cancellation only drops the waiter.
