# Trine KV

Trine KV is an embedded Rust key-value database for applications that need
ordered local storage without running a separate server. It gives simple code a
default bucket, and lets larger applications add named buckets with their own
prefix, filter, compression, and large-value settings.

The crate is implemented and verified by the repository test suite, benchmark
harness, and durability notes. To see the main path work end to end:

```text
cargo run --example quickstart
```

Then read [docs/usage.md](docs/usage.md) for the API path and
[docs/durability.md](docs/durability.md) for persistence guarantees and limits.
Release packaging notes live in [docs/release.md](docs/release.md).

## Common Capabilities

- Async-first default-bucket reads and writes with `Db::put`, `Db::get`,
  `Db::range`, and `Db::prefix`.
- Explicit sync adapters with `*_sync` names, such as `Db::open_sync`,
  `db.put_sync`, `bucket.get_sync`, and `db.flush_sync`.
- Optional named buckets through `db.bucket("users").await?` when data needs
  logical separation or independent tuning.
- Atomic write batches across the default bucket and named buckets.
- MVCC snapshots that keep old reads stable while newer writes commit.
- Public `ReadVersion` cursors for reopening a retained committed state with
  `Db::snapshot_at`.
- Named checkpoints that pin important read versions until the application
  deletes them.
- Configurable recent read-version retention with
  `DbOptions::with_keep_last_read_versions`.
- Snapshot-bound point readers that can avoid copying inline table values when
  callers can work with borrowed bytes.
- Optimistic transactions with point and range conflict checks.
- Ordered range scans and prefix scans.
- Value-lazy range and prefix scans for large-value workloads that need keys
  before reading blob bytes.
- Persistent mode with WAL replay, manifest recovery, directory locking,
  safe native defaults, background maintenance, backpressure, flush,
  compaction, and read-only open.
- Explicit in-memory mode for tests, examples, and short-lived data that should
  disappear when the `Db` is dropped.
- Async open/read/write/scan/transaction/maintenance entry points for hosts
  that need to drive storage cooperatively. Native async persistent APIs enter
  storage waits through Trine's storage boundary and use runtime task
  boundaries for synchronous maintenance/WAL internals.
- Block-based SSTables with partitioned index/filter blocks, data-block hash
  lookup for point reads, high-priority metadata caching, compression, and
  linear/binary/auto index seek policies.
- Large values can be separated into Titan-like blob files with `BlobIndex`
  records in SSTables.
- Automatic blob Level Merge can rewrite retained large values into output blob
  files during compaction when it improves locality or removes stale blob refs.
- Snapshot-safe blob GC rewrites still-live large values out of stale blob
  files and delays old-file deletion while a read can still reach them.
- Live stats report table, cache, filter, blob read, blob byte, and blob GC
  counters.
- Explicit WASI and browser persistent options. WASI uses a host-preopened
  filesystem path on WASI targets and supports `Db::open` through the
  host storage boundary. Browser persistence uses the async API and the browser
  persistent backend on `wasm32-unknown-unknown`.

## Install

Add Trine KV from [crates.io](https://crates.io/crates/trine-kv):

```text
cargo add trine-kv
```

`cargo install` is for crates that provide command-line binaries. Trine KV is a
library crate, so application projects should depend on it instead.

For local development, depend on a path:

```toml
[dependencies]
trine-kv = { path = "../trine-kv" }
```

## Common API Example

```rust
use trine_kv::{Db, KeyRange, TransactionOptions, WriteBatch, WriteOptions};

async fn run() -> trine_kv::Result<()> {
    let db = Db::open("./trine-data").await?;

    // Simple applications can use the built-in default bucket directly.
    db.put(b"settings:theme", b"dark").await?;
    assert_eq!(db.get(b"settings:theme").await?, Some(b"dark".to_vec()));

    // Named buckets are created on demand when you need logical separation.
    let users = db.bucket("users").await?;
    users.put(b"user:001", b"Ada").await?;

    // Snapshots keep a stable read version while newer writes continue.
    let snapshot = db.snapshot();
    users.put(b"user:002", b"Lin").await?;
    assert_eq!(snapshot.get(&users, b"user:002").await?, None);

    // Batches can atomically span buckets.
    let mut batch = WriteBatch::new();
    batch.put(b"audit:001", b"user-created");
    batch.put_bucket("users", b"user:003", b"Grace")?;
    db.write(batch, WriteOptions::default()).await?;

    // Transactions validate their read set when they commit.
    let mut txn = db.transaction(TransactionOptions::default());
    assert_eq!(
        txn.get_bucket("users", b"user:001").await?,
        Some(b"Ada".to_vec())
    );
    txn.put_bucket("users", b"user:004", b"Barbara")?;
    txn.commit().await?;

    let mut rows = users
        .range(&KeyRange::half_open(b"user:001", b"user:999"))
        .await?;
    let mut row_count = 0;
    while rows.next().await?.is_some() {
        row_count += 1;
    }
    assert_eq!(row_count, 4);

    Ok(())
}
```

For the runnable async-first persistent path, use:

```text
cargo run --example quickstart
```

For tests or short-lived data, opt into memory mode explicitly:

```rust
use trine_kv::{Db, DbOptions};

let db = Db::open(DbOptions::memory()).await?;
```

For the explicit sync-adapter path, use:

```text
cargo run --example sync_quickstart
```

## Common Commands

```text
cargo fmt --check
cargo clippy
cargo test
cargo run --example quickstart
cargo run --example sync_quickstart
cargo run --example read_versions
cargo run --example user_store
cargo run --example event_index
cargo bench --bench v1_bench
```

## Examples

- `quickstart`: first pass through `Db::open`, async writes, lazy scans,
  transaction commit, maintenance, read-only reopen, and storage runtime stats.
- `sync_quickstart`: first pass through the explicit sync adapters, including
  persistent open, buckets, scans, transactions, flush, reopen, and stats.
- `read_versions`: captures public read-version cursors, creates a named
  checkpoint, reopens from the checkpoint, and shows expiration after deletion.
- `user_store`: wraps Trine KV behind a small repository-style API.
- `event_index`: stores event payloads and a secondary account index with one
  atomic write batch.

## Documentation

- [Usage guide](docs/usage.md)
- [Durability notes](docs/durability.md)
- [Release packaging](docs/release.md)
- [0.1 benchmark baseline](docs/benchmarks/0.1-baseline.md)
- [Large-value direct read tuning](docs/benchmarks/v1-large-value-direct-read.md)
- [Blob maintenance and lazy value benchmark](docs/benchmarks/v1-blob-level-merge-lazy-gc.md)
- [Read-pruning measurement](docs/benchmarks/v1-read-pruning-measurement.md)
- [Cold table open read](docs/benchmarks/v1-cold-table-open-read.md)
- [Cold manifest/open reopen](docs/benchmarks/v1-cold-manifest-open-reopen.md)
- [Read-only cold reopen](docs/benchmarks/v1-read-only-cold-reopen.md)
- [Read-only cold open breakdown](docs/benchmarks/v1-read-only-cold-open-breakdown.md)
- [Batched point reads](docs/benchmarks/v1-batched-point-reads.md)
- [Get-many internal batching](docs/benchmarks/v1-get-many-internal-batching.md)

## Current Boundaries

- Persistent mode uses a single local database directory.
- Native persistent open defaults to `SyncAll` for confirmed writes. `Buffered`
  is an explicit advanced mode for data that can tolerate losing recent writes
  after a crash or power loss.
- WASI and browser persistent backends do not claim `SyncData` or `SyncAll`.
- WASI persistent `Db::open` uses the host-preopened filesystem on WASI
  targets; current WASI file work completes inline and does not advertise
  platform async I/O.
- Browser persistence is async-only: use `Db::open` plus async mutation
  and maintenance methods. Synchronous browser persistent open, mutation, and
  maintenance `*_sync` APIs return typed unsupported errors.
- Read-only open is for inspecting a stable directory state; the current
  pre-`1.0` line does not define live multi-process reads against an active
  writer.
- Repair is intentionally narrow and only removes known safe temporary files
  when explicitly requested.
