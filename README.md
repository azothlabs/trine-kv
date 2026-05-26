# Trine KV

Trine KV is an embedded Rust key-value database for applications that need
ordered local storage without running a separate server. It gives simple code a
default bucket, and lets larger applications add named buckets with their own
prefix, filter, compression, and large-value settings.

The v1 engine is implemented and verified by the repository test suite,
benchmark harness, and durability notes. To see the main path work end to end:

```text
cargo run --example quickstart
```

Then read [docs/usage.md](docs/usage.md) for the API path and
[docs/durability.md](docs/durability.md) for persistence guarantees and limits.
Release packaging notes live in [docs/release.md](docs/release.md).

## Common Capabilities

- Direct default-bucket reads and writes with `Db::put`, `Db::get`, `Db::range`,
  and `Db::prefix`.
- Optional named buckets through `db.bucket("users")?` when data needs logical
  separation or independent tuning.
- Atomic write batches across the default bucket and named buckets.
- MVCC snapshots that keep old reads stable while newer writes commit.
- Optimistic transactions with point and range conflict checks.
- Ordered range scans and prefix scans.
- Value-lazy range and prefix scans for large-value workloads that need keys
  before reading blob bytes.
- Persistent mode with WAL replay, manifest recovery, directory locking, flush,
  compaction, and read-only open.
- Block-based SSTables with partitioned index/filter blocks, data-block hash
  lookup for point reads, block cache, compression, and linear/binary/auto
  index seek policies.
- Large values can be separated into Titan-like blob files with `BlobIndex`
  records in SSTables.
- Automatic blob Level Merge can rewrite retained large values into output blob
  files during compaction when it improves locality or removes stale blob refs.
- Snapshot-safe blob GC rewrites still-live large values out of stale blob
  files and delays old-file deletion while a read can still reach them.
- Live stats report table, cache, filter, blob read, blob byte, and blob GC
  counters.

## Install

Published releases use Semantic Versioning. The first packaged release
candidate is `0.1.0`:

```toml
[dependencies]
trine-kv = "0.1"
```

For local development, depend on a path:

```toml
[dependencies]
trine-kv = { path = "../trine-kv" }
```

## Common API Example

```rust
use trine_kv::{
    BucketOptions, Db, KeyRange, PrefixExtractor, TransactionOptions, WriteBatch, WriteOptions,
};

fn main() -> trine_kv::Result<()> {
    let db = Db::open_memory()?;

    // Simple applications can use the built-in default bucket directly.
    db.put(b"settings:theme", b"dark")?;
    assert_eq!(db.get(b"settings:theme")?, Some(b"dark".to_vec()));

    // Named buckets are created on demand and can carry their own options.
    let users = db.bucket_with_options(
        "users",
        BucketOptions::default().with_prefix_extractor(PrefixExtractor::Separator(b':')),
    )?;
    users.put(b"user:001", b"Ada")?;

    // Snapshots keep a stable read sequence while newer writes continue.
    let snapshot = db.snapshot();
    users.put(b"user:002", b"Lin")?;
    assert_eq!(snapshot.get(&users, b"user:002")?, None);

    // Batches can atomically span buckets.
    let mut batch = WriteBatch::new();
    batch.put(b"audit:001", b"user-created");
    batch.put_bucket("users", b"user:003", b"Grace")?;
    db.write(batch, WriteOptions::sync_all())?;

    // Transactions validate their read set when they commit.
    let mut txn = db.transaction(TransactionOptions::default());
    assert_eq!(txn.get_bucket("users", b"user:001")?, Some(b"Ada".to_vec()));
    txn.put_bucket("users", b"user:004", b"Barbara")?;
    txn.commit()?;

    let rows = users
        .range(&KeyRange::half_open(b"user:001", b"user:999"))?
        .collect::<trine_kv::Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 4);

    Ok(())
}
```

For persistent open, flush, reopen, and stats in one runnable program, use:

```text
cargo run --example quickstart
```

## Common Commands

```text
cargo fmt --check
cargo clippy
cargo test
cargo run --example quickstart
cargo run --example user_store
cargo run --example event_index
cargo bench --bench v1_bench
```

## Examples

- `quickstart`: first pass through persistent open, buckets, scans,
  transactions, flush, reopen, and stats.
- `user_store`: wraps Trine KV behind a small repository-style API.
- `event_index`: stores event payloads and a secondary account index with one
  atomic write batch.

## Documentation

- [Usage guide](docs/usage.md)
- [Durability notes](docs/durability.md)
- [Release packaging](docs/release.md)
- [V1 benchmark baseline](docs/benchmarks/v1-baseline.md)
- [Large-value direct read tuning](docs/benchmarks/v1-large-value-direct-read.md)
- [Blob maintenance and lazy value benchmark](docs/benchmarks/v1-blob-level-merge-lazy-gc.md)

## Current Boundaries

- Persistent mode uses a single local database directory.
- `SyncAll` is the strongest WAL commit mode, subject to platform filesystem
  behavior.
- Read-only open is for inspecting a stable directory state; v1 does not define
  live multi-process reads against an active writer.
- Repair is intentionally narrow and only removes known safe temporary files
  when explicitly requested.
