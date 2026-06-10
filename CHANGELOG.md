# Changelog

All public crate releases use Semantic Versioning.

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
