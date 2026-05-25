# Trine KV Usage Guide

This guide shows the shortest path from an empty Rust program to a working
Trine KV database. The examples use the public v1 API and avoid engine internals.

Run the checked quickstart first:

```text
cargo run --example quickstart
```

Then look at the integration examples when you want to embed the database
behind an application boundary:

```text
cargo run --example user_store
cargo run --example event_index
```

`user_store` wraps Trine KV behind a small repository-style API. `event_index`
uses two keyspaces and one write batch to keep event payloads and an account
index in sync.

## Add The Crate

Published releases use Semantic Versioning. For the `0.1` release line:

```toml
[dependencies]
trine-kv = "0.1"
```

For local development from this repository:

```toml
[dependencies]
trine-kv = { path = "../trine-kv" }
```

If you consume the crate through git, replace the path dependency with your
repository URL.

## Open A Database

Use an in-memory database for tests and short-lived data:

```rust
use trine_kv::Db;

let db = Db::open_memory()?;
```

Use a persistent database when data should live in a directory:

```rust
use trine_kv::Db;

let db = Db::open_persistent("./trine-data")?;
```

Persistent mode creates the directory when `create_if_missing` is true and the
database is not opened read-only.

Set a database-level durability floor when every write should be at least that
durable:

```rust
use trine_kv::DurabilityMode;

let db = Db::open(
    DbOptions::persistent("./trine-data").with_durability(DurabilityMode::Flush),
)?;
```

`Db`, `Keyspace`, and `Snapshot` are cheap handles. A `Keyspace` keeps its
database open, so release keyspace handles before reopening the same directory
in the same process.

## Create A Keyspace

A keyspace is a named collection of keys with fixed options. Reopening an
existing keyspace with different options returns an error, because the options
are part of the on-disk contract.

```rust
use trine_kv::{KeyspaceOptions, PrefixExtractor};

let users = db.keyspace(
    "users",
    KeyspaceOptions::default().with_prefix_extractor(PrefixExtractor::Separator(b':')),
)?;
```

Use `KeyspaceOptions::default()` when you do not need prefix filters or custom
storage tuning.

## Write And Read Keys

The keyspace helpers are the simplest way to write one key at a time:

```rust
users.insert(b"user:001", b"Ada")?;
users.insert(b"user:002", b"Lin")?;

assert_eq!(users.get(b"user:001")?, Some(b"Ada".to_vec()));
```

Use `insert_with_options` when a single-key helper needs explicit durability:

```rust
use trine_kv::WriteOptions;

users.insert_with_options(b"user:003", b"Grace", WriteOptions::sync_all())?;
```

Deletes use the same keyspace handle:

```rust
users.remove(b"user:002")?;
assert_eq!(users.get(b"user:002")?, None);
```

The matching `remove_with_options` and `remove_range_with_options` helpers are
available when deletes need explicit write options.

Keys and values are byte vectors. String keys are fine, but the database does
not require UTF-8.

## Write A Batch

Use `WriteBatch` when several changes must commit at the same sequence:

```rust
use trine_kv::{WriteBatch, WriteOptions};

let mut batch = WriteBatch::new();
batch.insert("users", b"user:003", b"Grace");
batch.remove("users", b"user:001");

let commit = db.write(
    batch,
    WriteOptions::sync_all(),
)?;

println!("committed sequence {}", commit.sequence().get());
```

Batch writes can span keyspaces. If validation fails, the batch is rejected
before it changes memtables.

## Range And Prefix Scans

Range scans return keys in sorted order:

```rust
use trine_kv::KeyRange;

let range = KeyRange::half_open(b"user:000", b"user:999");
for item in users.range(&range)? {
    let key_value = item?;
    println!("{:?} = {:?}", key_value.key, key_value.value);
}
```

Reverse scans use the same range:

```rust
for item in users.range_reverse(&range)? {
    let key_value = item?;
    println!("{:?}", key_value.key);
}
```

Prefix scans are most useful when the keyspace has a prefix extractor:

```rust
for item in users.prefix(b"user:")? {
    let key_value = item?;
    println!("{:?}", key_value.key);
}
```

Prefix filters are advisory: they can skip table work, but they do not replace
MVCC or range-delete checks.

## Snapshots

A snapshot keeps reads pinned to the database sequence that was current when
the snapshot was created:

```rust
let snapshot = db.snapshot();

users.insert(b"user:004", b"Barbara")?;

assert_eq!(snapshot.get(&users, b"user:004")?, None);
assert_eq!(users.get(b"user:004")?, Some(b"Barbara".to_vec()));
```

Snapshots can read points, ranges, reverse ranges, and prefixes:

```rust
for item in snapshot.prefix(&users, b"user:")? {
    let key_value = item?;
    println!("{:?}", key_value.key);
}
```

Keep snapshots short-lived when possible. Long-lived snapshots can delay
cleanup of old versions and blob files.

## Optimistic Transactions

Transactions read at a fixed sequence and validate their read set at commit:

```rust
use trine_kv::{Error, TransactionOptions};

let mut txn = db.transaction(TransactionOptions::default());
let previous = txn.get("users", b"user:001")?;
txn.insert("users", b"user:005", b"Margaret");

match txn.commit() {
    Ok(info) => println!("committed sequence {}", info.sequence().get()),
    Err(Error::Conflict { message }) => println!("retry transaction: {message}"),
    Err(error) => return Err(error),
}
```

Point reads conflict with later point writes, point deletes, or covering range
deletes. Range reads conflict with later point changes inside the range or later
overlapping range deletes.

## Durability

For persistent databases, committed writes append to the WAL before becoming
visible in memtables. Choose a durability mode per write:

```rust
use trine_kv::{WriteBatch, WriteOptions};

let mut batch = WriteBatch::new();
batch.insert("users", b"user:006", b"Edsger");

db.write(
    batch,
    WriteOptions::sync_all(),
)?;
```

`DbOptions::durability` is a database-level floor. Per-write options can ask for
a stronger mode, but they cannot weaken the mode chosen at open time.

Use `Db::persist` as an explicit WAL sync point:

```rust
use trine_kv::DurabilityMode;

db.persist(DurabilityMode::SyncAll)?;
```

Read [durability.md](durability.md) before choosing a mode for production data.

## Flush, Compaction, And Stats

Flush writes memtable contents into SSTables and advances the manifest replay
floor:

```rust
db.flush()?;
```

Manual compaction rewrites overlapping tables while preserving snapshot
visibility:

```rust
db.compact_range(KeyRange::all())?;
```

The database can also compact automatically after flush when L0 file pressure
exceeds `DbOptions::max_l0_files`.

Inspect live state with `Db::stats`:

```rust
let stats = db.stats();
println!(
    "keyspaces={} tables={} cache_hits={}",
    stats.live_keyspaces, stats.total_tables, stats.block_cache_hits
);
```

## Read-Only Open

Use read-only open for inspecting a stable persistent directory:

```rust
let db = Db::open_read_only("./trine-data")?;
```

Read-only open does not take the writer lock and does not create a WAL writer.
It still validates files and replays WAL records into memory. V1 does not define
live multi-process reads against a concurrent writer.

## Recovery Boundaries

Startup is conservative. It fails closed on missing referenced files, corrupt
WAL records before the final tail, corrupt tables, corrupt blobs, unsupported
formats, and unexpected formal storage files.

Safe temporary files can be repaired only when explicitly requested:

```rust
use trine_kv::FailOnCorruptionPolicy;

let mut options = DbOptions::persistent("./trine-data");
options.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;

let db = Db::open(options)?;
```

This policy is intentionally narrow. It does not repair WAL corruption,
manifest corruption, table corruption, missing referenced files, or blob
corruption.

## Verification Path

Use these commands before trusting a change to documentation or examples:

```text
cargo run --example quickstart
cargo run --example user_store
cargo run --example event_index
cargo fmt --check
cargo clippy
cargo test
```

For performance-sensitive changes, also run:

```text
cargo bench --bench v1_bench
```
