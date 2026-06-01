# Changelog

All public crate releases use Semantic Versioning.

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
- Async-compatible database, bucket, iterator, value, transaction, flush,
  compaction, and maintenance APIs.
- Native platform I/O capability reporting, fallback observability, bounded
  blocking adapter stats, and cooperative maintenance budgets.
- Explicit WASI persistent options for host-preopened filesystems on WASI
  targets, including `Db::open_async` through the host storage boundary.
- Browser persistent options backed by the async browser storage path on
  `wasm32-unknown-unknown`, including writable async open, Web Locks writer
  lease, WAL-backed async writes, async bucket creation, and async maintenance.

### Hardened

- Manifest publish installs in-memory state only after durable file publish
  succeeds.
- WAL, manifest, and table decoders reject impossible count fields before large
  allocation.
- Failed flush/compaction publish removes unpublished table/blob output files.
- Recovery validates referenced table/blob files and fails closed on missing or
  corrupt storage files.
- Browser synchronous mutation and maintenance paths return typed unsupported
  errors instead of bypassing async storage guarantees.
- Browser async writes and maintenance own side-effecting work after acceptance,
  so caller future cancellation only drops the waiter.
