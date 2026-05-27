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
uses two buckets and one write batch to keep event payloads and an account
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

`Db`, `Bucket`, and `Snapshot` are cheap handles. `Db` writes to the built-in
default bucket. A named `Bucket` keeps its database open, so release bucket
handles before reopening the same directory in the same process.

## Use The Default Bucket

The default bucket is created automatically and is the right path for simple
embedded storage:

```rust
db.put(b"user:001", b"Ada")?;
assert_eq!(db.get(b"user:001")?, Some(b"Ada".to_vec()));

let rows = db.range(&trine_kv::KeyRange::all())?;
```

Configure the default bucket through `DbOptions`; do not open it by name:

```rust
use trine_kv::{BucketOptions, Db, DbOptions, PrefixExtractor};

let options = DbOptions::memory().with_default_bucket_options(
    BucketOptions::default().with_prefix_extractor(PrefixExtractor::Separator(b':')),
);
let db = Db::memory(options)?;
```

## Create A Bucket

A bucket is a named collection of keys with fixed options. `bucket` returns an
existing bucket or creates it with default `BucketOptions`.

```rust
let users = db.bucket("users")?;
```

Use `bucket_with_options` when the bucket needs prefix filters or custom
storage tuning. If the bucket already exists, the options must match because
they are part of the on-disk contract:

```rust
use trine_kv::{BucketOptions, PrefixExtractor};

let users = db.bucket_with_options(
    "users",
    BucketOptions::default().with_prefix_extractor(PrefixExtractor::Separator(b':')),
)?;
```

The name `"default"` is reserved for the built-in default bucket and cannot be
used as a named bucket.

## Write And Read Keys

The bucket helpers write one key at a time when you need a named bucket:

```rust
users.put(b"user:001", b"Ada")?;
users.put(b"user:002", b"Lin")?;

assert_eq!(users.get(b"user:001")?, Some(b"Ada".to_vec()));
```

Use `put_with_options` when a single-key helper needs explicit durability:

```rust
use trine_kv::WriteOptions;

users.put_with_options(b"user:003", b"Grace", WriteOptions::sync_all())?;
```

Deletes use the same bucket handle:

```rust
users.delete(b"user:002")?;
assert_eq!(users.get(b"user:002")?, None);
```

The matching `delete_with_options` and `delete_range_with_options` helpers are
available when deletes need explicit write options.

Keys and values are byte vectors. String keys are fine, but the database does
not require UTF-8.

## Write A Batch

Use `WriteBatch` when several changes must commit at the same sequence:

```rust
use trine_kv::{WriteBatch, WriteOptions};

let mut batch = WriteBatch::new();
batch.put(b"audit:001", b"created");
batch.delete(b"audit:000");
batch.put_bucket("users", b"user:003", b"Grace")?;
batch.delete_bucket("users", b"user:001")?;

let commit = db.write(
    batch,
    WriteOptions::sync_all(),
)?;

println!("committed sequence {}", commit.sequence().get());
```

Batch writes can span buckets. Named-bucket staging methods return `Result`
because empty names and the reserved `"default"` name are rejected before the
batch is submitted. If validation fails during commit, the batch is rejected
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

Prefix scans are most useful when the bucket has a prefix extractor:

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

users.put(b"user:004", b"Barbara")?;

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
let previous_default = txn.get(b"settings:theme")?;
txn.put(b"settings:theme", b"dark");

let previous_user = txn.get_bucket("users", b"user:001")?;
txn.put_bucket("users", b"user:005", b"Margaret")?;

match txn.commit() {
    Ok(info) => println!("committed sequence {}", info.sequence().get()),
    Err(Error::Conflict { message }) => println!("retry transaction: {message}"),
    Err(error) => return Err(error),
}
```

Point reads conflict with later point writes, point deletes, or covering range
deletes. Range reads conflict with later point changes inside the range or later
overlapping range deletes. Named-bucket write methods return `Result` for the
same bucket-name validation used by `WriteBatch`.

## Durability

For persistent databases, committed writes append to the WAL before becoming
visible in memtables. Choose a durability mode per write:

```rust
use trine_kv::{WriteBatch, WriteOptions};

let mut batch = WriteBatch::new();
batch.put_bucket("users", b"user:006", b"Edsger")?;

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

## Large Values And Blob GC

Small values stay inline in SSTables. In persistent mode, values at or above a
bucket's `blob_threshold_bytes` are written into Titan-like blob files when
memtables flush or compaction writes new SSTables. WAL records and memtables
still keep the complete value, so ordinary writes do not need a blob file on
the foreground path.

Configure the default bucket threshold and Level Merge policy through
`DbOptions`:

```rust
use trine_kv::{BlobLevelMergePolicy, BucketOptions, Db, DbOptions};

let db = Db::open(
    DbOptions::persistent("./trine-data").with_default_bucket_options(
        BucketOptions {
            blob_threshold_bytes: 64 * 1024,
            blob_level_merge_policy: BlobLevelMergePolicy::Auto,
            ..BucketOptions::default()
        },
    ),
)?;
```

`Auto` is the default. It rewrites retained blob values during compaction when
the output would otherwise keep references to multiple blob files or leave
stale bytes behind in input blob files. Use `Disabled` only when you want GC to
handle old blob files without compaction-time rewriting, and `Always` for
benchmarking or workloads that prefer aggressive blob locality.

When range or prefix scans mostly need keys first, use the value-lazy iterator
variants. They keep the same MVCC and range-delete semantics, but blob bytes
are read only when you call `LazyValue::read` or convert the row into a full
`KeyValue`:

```rust
for row in db.range_lazy(&trine_kv::KeyRange::all())? {
    let row = row?;
    println!("key={:?}", row.key);
    let value = row.value.read()?;
    println!("value bytes={}", value.len());
}
```

Blob GC is enabled for persistent databases by default. It runs from the
compaction path, batches all blob files that pass the discard threshold,
rewrites still-live records out of stale blob files, and keeps old blob files
until no snapshot or range iterator can still reach them.

Use database-level options to tune when GC is considered:

```rust
use trine_kv::{BlobGcRatio, DbOptions};

let mut options = DbOptions::persistent("./trine-data");
options.blob_gc_min_file_bytes = 64 * 1024 * 1024;
options.blob_gc_discardable_ratio = BlobGcRatio::from_millionths(500_000);
```

In-memory databases keep values inline and do not create disk blob files.

## Flush, Compaction, And Stats

Flush writes memtable contents into SSTables and advances the manifest replay
floor:

```rust
db.flush()?;
```

For persistent writable databases, `flush()` is a barrier for writes committed
before the call. It returns after those writes have left active and immutable
memtables and have been published as SSTables. Writes committed concurrently
after the call starts may remain in memory for a later flush.

Manual compaction rewrites overlapping tables while preserving snapshot
visibility:

```rust
db.compact_range(KeyRange::all())?;
```

If another compaction already owns an overlapping key range, `compact_range()`
waits and retries instead of reporting success while the guard is busy.

Persistent writable databases start one background maintenance worker by
default. Set `DbOptions::background_worker_count = 0` when a test or embedding
needs fully manual maintenance. In-memory and read-only databases never start
background workers.

The database can also compact automatically after flush when L0 file pressure
exceeds `DbOptions::max_l0_files`. Automatic L0 compaction chooses a local
overlapping key span, so unrelated L0 files may remain for later passes instead
of every pressure event rewriting the whole level.

Writes apply pressure handling when immutable memtables or L0 files exceed
configured limits. The write may wait briefly for the background worker or help
with one foreground maintenance pass before accepting more work.

Persistent reads keep one cached file handle per table and reuse it for block
reads. L0/L1 tables keep their table filters and index partitions resident;
deeper levels load partition metadata lazily and let the global block cache
protect metadata entries ahead of data blocks under cache pressure.

Inspect live state with `Db::stats`:

```rust
let stats = db.stats();
println!(
    "buckets={} tables={} cache_hits={} blob_reads={} blob_gc_runs={}",
    stats.live_buckets,
    stats.total_tables,
    stats.block_cache_hits,
    stats.blob_read_count,
    stats.blob_gc_runs,
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
