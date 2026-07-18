# Trine KV

[![CI](https://github.com/azothlabs/trine-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/azothlabs/trine-kv/actions/workflows/ci.yml)
[![Production Evidence](https://github.com/azothlabs/trine-kv/actions/workflows/production-evidence.yml/badge.svg)](https://github.com/azothlabs/trine-kv/actions/workflows/production-evidence.yml)
[![Crates.io](https://img.shields.io/crates/v/trine-kv.svg)](https://crates.io/crates/trine-kv)
[![Docs.rs](https://docs.rs/trine-kv/badge.svg)](https://docs.rs/trine-kv)

Trine KV is an embedded, async-first Rust key-value database for applications
that need ordered local storage without operating a separate database server.
It combines buckets, atomic batches, stable snapshots, optimistic transactions,
range scans, WAL recovery, compaction, and optional large-value files behind one
crate.

## Production Status

Trine KV is pre-`1.0`. It is suitable for evaluation and controlled production
adoption when the application owner validates Trine against the real workload,
filesystem, storage device, restart policy, and backup procedure. It should not
yet be presented as a universally production-proven database.

Repository evidence currently includes:

- full Rust tests, strict linting, Rustdoc, doctests, WASI, and real-browser
  persistence gates;
- repeated child-process exit and reopen verification;
- deterministic I/O failure injection across WAL, publish, directory-sync, and
  cleanup boundaries;
- deterministic concurrent mixed-load soak and reopen verification;
- paired benchmark regression checks on the same runner;
- target-native Linux, macOS, and Windows maturity jobs with retained reports.

The current result of those jobs is visible in the badges above. Read
[Production readiness evidence](docs/production-readiness.md) for the exact
commands, what each test proves, and what still needs deployment evidence. The
cross-platform definition lives in
[`.github/workflows/production-evidence.yml`](.github/workflows/production-evidence.yml).

Trine is a reasonable candidate when you need one local writer, ordered keys,
atomic changes across buckets, snapshots, and crash recovery inside a Rust
application. Evaluate another design, or add application-level safeguards, when
you require a database server, live multi-process readers, online backup,
replication, sudden-power-loss certification, or established fleet incident
history.

## Try It in Five Minutes

Add the crate:

```text
cargo add trine-kv
```

Run the repository's persistent end-to-end example:

```text
cargo run --example quickstart
```

Or start with the main async API:

```rust
use trine_kv::{Db, KeyRange, TransactionOptions, WriteBatch, WriteOptions};

async fn run() -> trine_kv::Result<()> {
    let db = Db::open("./trine-data").await?;

    db.put(b"settings:theme", b"dark").await?;
    assert_eq!(db.get(b"settings:theme").await?, Some(b"dark".to_vec()));

    let users = db.bucket("users").await?;
    users.put(b"user:001", b"Ada").await?;

    let snapshot = db.snapshot();
    users.put(b"user:002", b"Lin").await?;
    assert_eq!(snapshot.get(&users, b"user:002").await?, None);

    let mut batch = WriteBatch::new();
    batch.put(b"audit:001", b"user-created");
    batch.put_bucket("users", b"user:003", b"Grace")?;
    db.write(batch, WriteOptions::default()).await?;

    let mut transaction = db.transaction(TransactionOptions::default());
    transaction.put_bucket("users", b"user:004", b"Barbara")?;
    transaction.commit().await?;

    let mut rows = users
        .range(&KeyRange::half_open(b"user:001", b"user:999"))
        .await?;
    let mut row_count = 0;
    while rows.next().await?.is_some() {
        row_count += 1;
    }
    assert_eq!(row_count, 4);

    db.flush().await?;
    Ok(())
}
```

Use `cargo run --example sync_quickstart` for the explicit `*_sync` adapter
path. Use `DbOptions::memory()` when data should disappear after the database is
dropped.

## What You Get

- **Ordered storage:** point reads, forward/reverse range scans, prefix scans,
  and value-lazy iteration for large values.
- **Consistent changes:** atomic batches across the default and named buckets,
  stable MVCC snapshots, retained read versions, named checkpoints, and
  optimistic transaction conflict checks.
- **Local persistence:** WAL replay, manifest recovery, one-writer directory
  locking, flush, compaction, backpressure, background maintenance, and
  read-only reopen.
- **Workload controls:** per-bucket prefix, filter, compression, index-seek, and
  large-value settings; separated blob files and snapshot-safe blob cleanup.
- **Runtime choices:** async-first APIs, explicit sync adapters, portable
  thread-pool platform I/O, audited native-first platform I/O with fallback,
  WASI host storage, and browser OPFS persistence. Browser databases run in
  Window, DedicatedWorker, and SharedWorker contexts; DedicatedWorker uses the
  synchronous OPFS fast path, while SharedWorker keeps the async path for a
  database instance shared across tabs.
- **Operational evidence:** live statistics plus repeatable recovery, soak,
  platform, browser, WASI, and benchmark gates.

## Install and Features

Most native applications can start without feature flags:

```toml
[dependencies]
trine-kv = "0.5"
```

Enable `platform-io` for Trine's bounded portable async file-I/O thread pool:

```toml
[dependencies]
trine-kv = { version = "0.5", features = ["platform-io"] }
```

Enable `platform-io-native` for audited native async operations with the same
thread-pool fallback for unsupported operation rows:

```toml
[dependencies]
trine-kv = { version = "0.5", features = ["platform-io-native"] }
```

Select platform I/O for a database explicitly:

```rust
use trine_kv::{Db, DbOptions, RuntimeOptions};

let mut options = DbOptions::new("./trine-data");
options.runtime = RuntimeOptions::platform_io();
let db = Db::open(options).await?;
```

Verify both feature paths with:

```text
cargo run --example platform_io --features platform-io
cargo run --example platform_io --features platform-io-native
```

See [Platform I/O and feature selection](docs/platform-io.md) for the
operation-by-operation runtime matrix and statistics fields.

## Durability in Plain Terms

Native persistent databases default to `SyncAll` for confirmed writes. On
macOS this uses ordinary `fsync`: it covers application crashes and kernel
panics, but does not claim survival across sudden power loss. Select
`SyncAllStrict` for macOS `F_FULLFSYNC`, either per write or as a database-wide
minimum. Select `Buffered` only when losing recent confirmed writes is an
accepted application tradeoff.

WASI and browser storage do not claim native device-sync modes. Browser
persistence is async-only and depends on OPFS; synchronous persistent mutation
and maintenance methods return typed unsupported errors.

Read [Durability notes](docs/durability.md) before choosing a durability mode.

## Current Boundaries

- One persistent database directory has one live writer. Read-only open is for
  a stable directory state, not live multi-process access beside a writer.
- There is no replication or database-server protocol.
- Online backup semantics are not defined; validate an application-level
  backup and restore procedure before relying on it.
- Forced process exit is tested, but sudden power loss, disk-full behavior, and
  damaged-filesystem recovery require deployment-specific evidence.
- Browser persistence depends on host OPFS and persistent-storage behavior;
  applications should inspect quota and eviction risk through the browser
  helpers.
- Repair is deliberately narrow and only removes known safe temporary files
  when explicitly requested.

These are adoption constraints, not footnotes. The maintained list and test
commands are in [Production readiness evidence](docs/production-readiness.md).

## Examples and Verification

| Command | What it verifies |
| --- | --- |
| `cargo run --example quickstart` | Async persistent lifecycle, transactions, maintenance, reopen, and stats. |
| `cargo run --example sync_quickstart` | The explicit synchronous adapter path. |
| `cargo run --example platform_io --features platform-io-native` | Selected platform-I/O path and operation counters. |
| `cargo run --example read_versions` | Retained read versions and named checkpoints. |
| `cargo run --example user_store` | A repository-style application wrapper. |
| `cargo run --example event_index` | Payload and secondary-index updates in one atomic batch. |
| `cargo run --example durability` | Default, strict, and database-wide durability selection. |

Repository verification starts with:

```text
python3 scripts/check_docs_drift.py
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Performance-sensitive changes should use the bounded paired procedure in
[Production readiness evidence](docs/production-readiness.md), not compare raw
wall times from different machines.

## Documentation

- [Usage guide](docs/usage.md)
- [Platform I/O and feature selection](docs/platform-io.md)
- [Durability notes](docs/durability.md)
- [Production readiness evidence](docs/production-readiness.md)
- [Release packaging](docs/release.md)
- [Benchmark notes](docs/benchmarks/)
- [Changelog](CHANGELOG.md)

Trine KV is dual-licensed under the repository's MIT and Apache-2.0 license
files.
