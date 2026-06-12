# Trine KV V1 Specification

Date: 2026-05-25

## 1. Purpose

Trine KV is a clean embedded key-value database implemented in Rust. It uses an
LSM tree as its primary data structure and supports volatile in-memory mode plus
durable persistent backends.

Trine is async-first at the database API and storage boundary. Blocking native
APIs are adapters over the primary async engine, not the source of engine
semantics.

The v1 target is a complete database-shaped engine, not a temporary toy. The
implementation may be sliced, but each slice must preserve the final v1 model:

- ordered keys
- MVCC snapshots
- atomic write batches
- serializable optimistic transactions
- durable WAL
- immutable SSTables
- manifest-published versions
- compaction with snapshot safety
- range and prefix iteration
- prefix extractors and prefix filters
- search-policy aware immutable indexes
- configurable durability
- in-memory mode using the same logical engine
- async-first public API over portable storage backends
- future WASM-capable storage and runtime boundaries

## 2. Source Of Truth

Trine's implementation source of truth is this specification, its ADRs, and its
tests.

Rules:

- do not add another storage engine as the implementation;
- do not copy another database file format;
- do not change engine behavior without updating this spec or a follow-up ADR;
- implementation decisions must be proven by Trine tests and benchmarks.
- internal database/LSM module boundaries are governed by
  `.phrase/protocol/lsm-core-boundary-spec.md`; that boundary spec does not
  change the public API or storage format by itself.
- the next production write-path target is governed by
  `.phrase/protocol/lock-free-foreground-write-path.md`; that protocol defines
  foreground no-global-lock write semantics, WAL sharding, delta publication,
  visible sequence advancement, and recovery requirements.
- async-first API, portable storage backend, durability capability, runtime,
  and WASM-readiness rules are governed by
  `.phrase/protocol/async-first-portable-storage-and-wasm.md`.
- implementation order between the async-first storage protocol and the
  foreground write-path protocol must remain staged: API/storage/cancellation
  boundaries first, then commit tracker and visible sequence, then WAL shards
  and key-sharded delta publication.

## 3. Vocabulary

- **User key**: byte key supplied by the caller.
- **Internal key**: encoded key used inside memtables and SSTables.
- **Sequence**: monotonic commit number assigned by Trine.
- **Snapshot sequence**: read boundary for repeatable reads.
- **ReadVersion**: public stable numeric cursor for a committed database state.
  It is database-scoped and does not require callers to understand internal
  sequence allocation.
- **Default bucket**: built-in ordered KV namespace addressed directly through
  `Db`.
- **Bucket**: optional named ordered KV namespace with independent options.
- **Memtable**: mutable in-memory ordered table for recent writes.
- **Immutable memtable**: frozen memtable waiting for flush.
- **SSTable**: immutable sorted table file or in-memory table object.
- **Manifest**: durable description of the current live version set.
- **VersionSet**: immutable published view of live SSTables and WAL state.
- **WAL**: write-ahead log for committed batches not yet safely represented by
  published SSTables.
- **Compaction**: merge of sorted tables into new sorted tables while preserving
  MVCC visibility.
- **Storage backend**: async capability-based implementation used by the engine
  for WAL, SSTable, blob, manifest, lease, report, and temporary objects.
- **Blocking adapter**: native synchronous wrapper over the primary async API.
- **Cursor**: fallible async range or prefix traversal handle.

## 4. Storage Modes

Trine supports the same primary async public API in every mode.

### 4.1 Persistent Mode

Persistent mode stores WALs, SSTables, manifests, value files, leases, and
repair reports through a storage backend. The native-file backend stores these
objects under a local directory.

Rules:

- only one writer may open the same persistent database scope for writing;
- startup must acquire an exclusive writer lease unless opened read-only;
- all files include format version and CRC-32C checksum coverage;
- manifest publish is atomic;
- crash recovery must replay committed WAL records after the manifest snapshot.
- the engine core must not call platform filesystem APIs directly;
- backend capability checks must reject unsupported durability or lease modes.

### 4.2 In-Memory Mode

In-memory mode uses a volatile storage backend. It still uses memtables,
VersionSet, table builders, filters, compaction, MVCC, snapshots, and
transactions.

Rules:

- no filesystem durability is promised;
- process exit loses data;
- WAL and SSTable abstractions may be backed by memory buffers;
- the same correctness tests should run against both persistent and in-memory
  mode when the behavior is not durability-specific.

In-memory mode is not a separate toy engine. It is the same engine over a
volatile storage backend.

### 4.3 Portable Storage And WASM Readiness

The storage model is capability-based. Local files, memory, WASI storage, and
browser persistence are backend families behind the same database-level
operations.

Rules:

- core LSM, MVCC, WAL, SSTable, manifest, and compaction logic must not depend
  on native threads, blocking filesystem calls, process locks, memory mapping,
  direct I/O, OS page size, or native endian behavior;
- persistent writable open requires a reliable writer lease;
- manifest publish is a backend protocol operation, not a hard-coded rename;
- unsupported durability strength returns a typed error instead of silently
  downgrading;
- background maintenance must support both native workers and cooperative async
  workers;
- memory mode must remain available for WASM-capable builds.

## 5. Public API Shape

The exact Rust names may evolve, but the v1 API must expose these async-first
concepts:

```rust
Db::open(options).await -> Result<Db>
Db::memory(options) -> Result<Db>
Db::put(key, value).await -> Result<()>
Db::get(key).await -> Result<Option<Value>>
Db::range(range).await -> Result<Cursor>
Db::prefix(prefix).await -> Result<Cursor>
Db::default_bucket() -> Result<Bucket>
Db::bucket(name) -> Result<Bucket>
Db::bucket_with_options(name, options) -> Result<Bucket>
Db::persist(mode).await -> Result<()>
Db::flush().await -> Result<()>
Db::compact_range(range).await -> Result<()>
Db::close().await -> Result<()>
Db::snapshot() -> Snapshot
Db::snapshot_at(read_version) -> Result<Snapshot>
Db::latest_read_version() -> ReadVersion
Db::oldest_retained_read_version() -> ReadVersion
Db::create_checkpoint(name).await -> Result<ReadVersion>
Db::delete_checkpoint(name).await -> Result<()>
Db::checkpoint_read_version(name).await -> Result<ReadVersion>
Db::transaction(options) -> Transaction
Db::stats() -> DbStats

DbOptions::with_default_bucket_options(options) -> DbOptions
DbOptions::with_keep_last_read_versions(count) -> DbOptions

Bucket::get(key).await -> Result<Option<Value>>
Bucket::put(key, value).await -> Result<()>
Bucket::delete(key).await -> Result<()>
Bucket::range(range).await -> Result<Cursor>
Bucket::prefix(prefix).await -> Result<Cursor>

Cursor::next().await -> Result<Option<KeyValue>>

WriteBatch::put(key, value)
WriteBatch::delete(key)
WriteBatch::delete_range(range)
WriteBatch::put_bucket(bucket, key, value) -> Result<()>
WriteBatch::delete_bucket(bucket, key) -> Result<()>
WriteBatch::delete_range_bucket(bucket, range) -> Result<()>
Db::write(batch, write_options).await -> Result<CommitInfo>
CommitInfo::read_version() -> ReadVersion
Snapshot::read_version() -> ReadVersion

Transaction::get(key).await -> Result<Option<Value>>
Transaction::put(key, value) -> Result<()>
Transaction::delete(key) -> Result<()>
Transaction::read_range(range).await -> Result<()>
Transaction::get_bucket(bucket, key).await -> Result<Option<Value>>
Transaction::put_bucket(bucket, key, value) -> Result<()>
Transaction::delete_bucket(bucket, key) -> Result<()>
Transaction::delete_range_bucket(bucket, range) -> Result<()>
Transaction::read_range_bucket(bucket, range).await -> Result<()>
Transaction::commit().await -> Result<CommitInfo>

BlockingDb::open(options) -> Result<BlockingDb>
BlockingDb::get(key) -> Result<Option<Value>>
BlockingDb::put(key, value) -> Result<()>
```

API rules:

- `Db`, `Bucket`, and `Snapshot` handles are cloneable and thread-safe.
- persistent operations that can wait on storage or maintenance are async in
  the primary API;
- builders and local staging methods may remain synchronous;
- blocking APIs are optional native adapters and must delegate to the primary
  async engine;
- the default bucket always exists and is the target for direct `Db` reads and
  writes;
- the default bucket is configured with `DbOptions::default_bucket_options`;
- `"default"` is reserved; `Db::bucket("default")` and
  `Db::bucket_with_options("default", ...)` return an invalid-options
  error;
- `Db::bucket(name)` returns an existing named bucket or creates one with
  default `BucketOptions`;
- `Db::bucket_with_options(name, options)` returns an existing named bucket
  only when options match, or creates one with those fixed options;
- `Db::flush().await` is a public barrier for persistent writable databases:
  when it returns `Ok(())`, data committed before the call has left active and
  immutable memtables and has been published as SSTables. Concurrent writes
  committed after the call boundary may remain in the active memtable.
- `Db::compact_range(range).await` waits and retries if an overlapping
  compaction reservation is already active. It may return after one successful
  compaction pass or when no compaction plan exists, but it must not report
  success solely because a maintenance guard was busy.
- named buckets are optional and used for logical isolation or different
  per-bucket options;
- Cursors keep the read VersionSet alive.
- Cursors are snapshot-consistent.
- Cursor advancement is async because storage reads can be required after
  construction;
- `WriteBatch` is atomic across buckets.
- values returned by reads may borrow shared immutable buffers through an owned
  guard type; callers can copy to `Vec<u8>` when they need independent storage.
- errors are explicit typed errors, not strings as the primary contract.

## 6. Key And Value Rules

- user keys are arbitrary bytes;
- ordering is lexicographic over user key bytes;
- callers storing integers should encode them big-endian when numeric ordering
  matters;
- empty keys are allowed unless a bucket option forbids them;
- v1 supports values up to at least `u32::MAX` bytes in the format, though
  practical limits may be lower by configuration;
- large value handling uses the same visible semantics as inline values.

## 7. Internal Key Format

All memtables and SSTables sort by internal key:

```text
InternalKey = user_key || sequence_desc || kind
```

Logical ordering:

1. `user_key` ascending
2. `sequence` descending
3. `kind` deterministic tie-breaker

Kinds:

- `Put`
- `PointDelete`
- `RangeDelete`

Sequence rules:

- every committed write batch receives one commit sequence;
- all writes in that batch share the same commit sequence;
- batch-local operation order is preserved for duplicate keys by an internal
  batch index when needed;
- a reader at snapshot sequence `S` can see versions with `sequence <= S`.

## 8. MVCC And Snapshots

Snapshot creation captures the current published sequence:

```text
snapshot.read_seq = db.last_committed_seq()
```

Read visibility:

- ignore records with `sequence > read_seq`;
- for a user key, the first visible internal key decides the result;
- `Put` returns a value;
- `PointDelete` returns missing;
- `RangeDelete` hides covered point versions at or below the tombstone sequence;
- range scans return the newest visible live value per user key.

Snapshot lifetime:

- active snapshots pin their `read_seq`;
- active snapshots, named checkpoints, and configured recent-history retention
  together control compaction cleanup;
- dropping a snapshot releases its pin;
- deleting a checkpoint releases its named pin;
- no compaction may remove a version that a retained read version could still
  read.

Public read-version rules:

- `ReadVersion` is the user-facing historical-read cursor for a database state;
- `latest_read_version` returns the newest state visible to readers;
- `oldest_retained_read_version` returns the oldest state Trine promises to
  answer;
- `snapshot_at(read_version)` validates the read version, pins it, and never
  falls back to latest;
- requesting a future read version returns `ReadVersionTooNew`;
- requesting a read version below the retained floor returns
  `ReadVersionExpired`;
- an accepted empty write batch returns the current latest read version without
  creating a new database state;
- checkpoints are named read-version pins stored in manifest metadata for
  durable storage modes;
- `DbOptions::with_keep_last_read_versions(count)` retains a configured number
  of recent read versions, with `count = 1` as the default latest-only window.

## 9. Transactions

Trine v1 supports three write surfaces:

1. single-key convenience writes;
2. atomic `WriteBatch`;
3. optimistic serializable transactions.

### 9.1 WriteBatch

`WriteBatch` is atomic. Either all operations become visible at one commit
sequence or none do.

Rules:

- a batch may touch multiple buckets;
- a batch commit appends one WAL batch record;
- a batch commit publishes one sequence;
- batch commit is serialized through the writer coordinator.

### 9.2 Optimistic Transaction

An optimistic transaction captures a read sequence at begin:

```text
txn.read_seq = db.last_committed_seq()
```

It records:

- point keys read;
- key ranges read;
- bucket names read or written;
- writes staged by the transaction.

Commit validation:

- fail if any read key was modified after `txn.read_seq`;
- fail if any read range overlaps a write committed after `txn.read_seq`;
- fail if a bucket required by the transaction was dropped or recreated after
  `txn.read_seq`;
- otherwise assign one new commit sequence and commit the staged write batch.

Isolation:

- successful optimistic transactions are serializable;
- failed transactions return a conflict error and do not partially commit.

## 10. Writer And Concurrency Model

Reads:

- load an immutable `Arc<VersionSet>`;
- consult active memtable, immutable memtables, and SSTables;
- point reads check overlapping L0 tables and at most one table per
  non-overlapping deeper level for point records and table range tombstones;
- range and prefix scans select only SSTables whose key bounds overlap the
  query range;
- prefix filters may skip point-data cursors, but range tombstone metadata
  remains authoritative for matching key ranges;
- do not wait for compaction except for bounded cache misses or file reads.

Writes:

- enter a writer coordinator;
- receive a sequence;
- append WAL through the async storage backend;
- apply to the active memtable;
- publish the new `last_committed_seq`;
- optionally trigger flush or compaction scheduling.

The writer coordinator may serialize commits. That is acceptable for v1. Reads
must not require the writer coordinator.

Background work:

- persistent writable databases start one maintenance worker by default;
- `background_worker_count == 0` explicitly disables background maintenance;
- persistent databases with `background_worker_count > 0` start that many
  maintenance workers after open;
- in-memory and read-only databases do not start maintenance workers;
- flush immutable memtables;
- compact SSTables;
- clean obsolete files after snapshot safety allows it;
- never publish partially written tables.
- track flush requests, compaction requests, in-flight flushes, in-flight
  compaction key ranges, progress notifications, and the last maintenance
  error;
- public `flush` waits until pending flush requests and active flushes that can
  contain its captured sequence boundary are idle before reporting success;
- public `compact_range` waits for overlapping active compaction reservations
  instead of treating a busy guard as a successful no-op;
- writes apply pressure handling before taking the writer coordinator when
  immutable memtables or L0 files exceed configured limits; pressure handling
  may wait for a background worker or help with a foreground maintenance pass;
- automatic L0 compaction may choose a local overlapping key span to reduce
  rewrite work, while explicit `compact_range` preserves requested-range
  semantics;
- worker shutdown must join outstanding maintenance threads before the process
  lock is released;
- background maintenance failures must be returned by a later write, `flush`,
  or `compact_range` call instead of being silently ignored.

## 11. Durability Modes

Write options include a durability mode. The requested mode is mapped through
the storage backend capability contract:

```text
Buffered   -> append to WAL buffer and apply to memtable
Flush      -> push WAL bytes to the backend flush boundary
SyncData   -> backend durable data sync for WAL data
SyncAll    -> backend durable data and metadata sync where required
```

The exact backend implementation may vary, but the names must remain honest.
A write acknowledged with `SyncAll` must survive the backend's declared normal
power-loss model. If the backend cannot support the requested durability, Trine
must return `UnsupportedDurability`.

`Db::persist(mode).await` forces pending durable state according to the
requested mode.

## 12. WAL Format

The WAL is append-only and contains framed batch records. WAL format version 2
uses CRC-32C for both header and payload checksum fields.

Record frame:

```text
magic
format_version
record_len
header_checksum
payload_checksum
payload
```

Payload includes:

- database id;
- bucket id mapping version;
- commit sequence;
- batch operation count;
- operation records;
- optional compression marker for the payload;
- checksum coverage over decoded logical payload.

Recovery rules:

- a complete valid record is replayable;
- a torn final record may be ignored if it is only a tail truncation;
- checksum mismatch before the final tail is corruption and startup fails
  closed;
- a WAL record is never visible unless it replays successfully into memtables
  after manifest load.

## 13. Memtable

The default memtable is an ordered map keyed by internal key.

Rules:

- active memtable accepts writes;
- once size or entry thresholds are reached, it freezes into an immutable
  memtable;
- immutable memtables are read before SSTables;
- immutable memtables are flushed in sequence order;
- flush output is an SSTable plus manifest edit.

The implementation may use a skiplist, arena-backed tree, or another ordered
structure, but the behavioral contract is the ordered internal-key table above.

## 14. SSTable Format

An SSTable is immutable. It may live as a file in persistent mode or as a memory
object in in-memory mode. SSTable format version 6 uses CRC-32C for checked
blocks, table payloads, and footer checksums.

Logical sections:

```text
data blocks
range tombstone blocks
filter blocks
index blocks
properties block
footer
```

Data block rules:

- records are sorted by internal key;
- blocks have restart points for efficient seek;
- blocks store a checked user-key hash index for point lookup, and reads still
  compare the real user key to handle hash collisions;
- prefix compression is allowed inside a block;
- every block has checksum coverage;
- every block declares compression codec id;
- codec id `none` must be supported.

Index rules:

- top-level table index maps key ranges to partition index blocks;
- partition index blocks map key ranges to data blocks and carry per-block
  point/prefix filters;
- partitioned index is required for large tables;
- index blocks keep a canonical sorted order for validation, range traversal,
  and debugging;
- index blocks may also carry a search layout optimized for point seek;
- table properties include smallest/largest user key, sequence range,
  referenced blob file ids, referenced blob bytes, record counts, and blob key
  spans.

Filter rules:

- each table partition may have point-key Bloom filters;
- each table partition may have prefix filters when a bucket prefix extractor is
  configured;
- filters are partitioned by table key range for large tables;
- filters are advisory only; false positives are allowed, false negatives are
  not.
- `bits_per_key` and `bits_per_prefix` control Bloom bit counts; implementations
  must not store every full key or prefix as the long-lived filter structure.

Footer rules:

- fixed-size footer contains magic, format version, section offsets, and footer
  checksum;
- unknown incompatible major versions fail closed;
- compatible minor versions may ignore unknown optional sections.

Persistent read rule:

- opening a table reads only footer, properties, and the small top-level index;
- L0/L1 tables pin table filters and index partitions because they are checked
  frequently by point reads and compaction pressure;
- deeper levels read partition index/filter blocks lazily before their covered
  data blocks and may keep those partitions in the global block cache;
- persistent block reads reuse the table's cached file handle; implementations
  must not clone or reopen the table file for every block read;
- data blocks are read on demand and must verify checksum, codec, and index
  bounds before decoded records affect a read;
- corrupt data blocks fail the read that touches them, while unrelated filter
  misses may still return without reading that data block.

## 15. Prefix Extractors And Filters

Prefix scan is a first-class operation. Prefix filtering must be designed into
the table format and bucket options instead of treated as a caller-side range
hack.

Bucket prefix extractor:

```text
PrefixExtractor::FixedLen(n)
PrefixExtractor::Separator(byte)
PrefixExtractor::Custom(name)
PrefixExtractor::Disabled
```

Rules:

- a bucket declares at most one prefix extractor at a time;
- prefix extractor changes are manifest edits and affect newly written tables;
- existing tables retain the extractor id used when they were written;
- prefix filters are stored per table or per partition;
- prefix filters may return false positives;
- prefix filters must not return false negatives for the extractor version used
  by the table;
- if a table has no compatible prefix filter, prefix scan must fall back to
  index seek and ordered scan;
- point filters and prefix filters are separate because a good point-key filter
  is not automatically a good prefix filter;
- range tombstones must still be checked after a prefix filter hit.

Prefix scan shape:

```text
prefix_scan(prefix, read_seq)
  -> compute key range from prefix when possible
  -> skip tables whose key range cannot overlap
  -> use compatible prefix filters to skip tables or partitions
  -> seek to first candidate key
  -> merge visible records until prefix no longer matches
```

Correctness rule:

Prefix filters only skip table or partition reads. They never decide visibility
and never replace MVCC, point tombstone, or range tombstone checks.

## 16. Search Policy And Index Layout

Trine treats search algorithms as internal policies behind stable index APIs.
The storage model remains sorted by internal key. Search-policy work must never
change MVCC visibility, cursor ordering, or table publish rules.

Required index APIs:

```text
seek_ge(key)
seek_gt(key)
seek_le(key)
advance_to(cursor, key)
```

Allowed policies:

- linear search for tiny arrays;
- binary search for general sorted arrays;
- auto policy, currently linear for tiny arrays and binary otherwise.

Design rules:

- data blocks and SSTable record order remain sorted; do not store primary data
  in a search-only layout;
- do not expose a public search policy unless the implementation has a distinct
  data structure or algorithm behind it;
- retired manifest search-policy tags must decode to `Auto` when they preserve
  read correctness;
- search policy thresholds are configuration and benchmark decisions, not
  public API contracts;
- every optimized policy must have a simple sorted-search fallback;
- unsafe code is not required for these policies.

Default policy shape:

```text
small index:
  linear search

medium sorted index:
  binary search

large immutable index:
  partitioned sorted index blocks, loaded lazily

cursor advance with position hint:
  advance from the current sorted position, then binary search when needed
```

This is a good design only when it stays local to immutable indexes and cursor
movement. It is a bad design if it makes SSTable format hard to inspect, forces
range scans through non-sequential layouts, or adds memory overhead without a
measured point-read or seek benefit.

## 17. Compression

Trine uses a pluggable block compression interface. The storage format stores
stable codec ids, not Rust crate names.

Required behavior:

- `none` codec is always available;
- compressed blocks store codec id and uncompressed length;
- checksum is checked before trusting decoded records;
- a database must refuse to open if it needs a codec that is not available;
- compression can be configured per bucket; option changes affect newly
  written tables and do not rewrite existing tables by themselves.

V1 codec policy:

- default codec profile: `Fast`;
- `Fast` is implemented with `lz4_flex` block compression;
- `None` stores uncompressed blocks;
- table metadata records the concrete codec id used for every compressed block.

Design judgment:

- `lz4_flex` is the default because SSTable block decompression is on the read
  path and LSM point/range reads benefit more from low CPU cost than maximum
  compression ratio.
- V1 deliberately does not include a zlib/DEFLATE codec. If a future version
  needs another codec, it must get a new stable Trine codec id and explicit
  fixtures before implementation.
- codec choice must be benchmarked with Trine blocks, not generic text files.

Crate binding rule:

The public format uses Trine codec ids such as `none` and `fast-lz4-block`.
The implementation uses `lz4_flex`, but crate names do not become on-disk
compatibility names.

## 18. Large Values

V1 uses `.phrase/protocol/titan-like-blob-storage-spec.md` as the stable
large-value storage contract. The older primitive blob-offload shape is
superseded by a Titan-like design that separates only large values during flush
and compaction.

Stable value references:

```text
Inline(bytes)
BlobIndex {
  file_id,
  offset,
  encoded_len,
  value_len,
  value_checksum,
  record_checksum,
  compression,
}
```

Small values stay inline. Values at or above `blob_threshold_bytes` may be
stored in blob files. WAL records and memtables still store complete user
values; separation happens when immutable data is written to SSTables.
Blob file format version 3 uses CRC-32C for header, record, value, properties,
and footer checksum fields.

Rules:

- blob references are visible only through committed WAL replay or published
  SSTables;
- blob records include key/version metadata so GC can check whether a blob is
  still referenced by the current LSM state;
- blob files include header, ordered records, properties, footer, and checksums;
- ordinary compaction may keep existing `BlobIndex` records instead of
  rewriting large values;
- blob GC is snapshot-safe and recoverable;
- manifest edits can mark obsolete blob files as pending deletion, and startup
  can resume cleanup from that metadata;
- point reads use `BlobIndex.offset` to read the indexed blob record directly;
- cleanup cannot remove a blob file referenced by any live table, active
  snapshot, read pin, or pending old tree version.

## 19. Manifest And VersionSet

The manifest stores a sequence of version edits. Manifest format version 8 uses
CRC-32C. Earlier pre-release manifest versions are rejected instead of being
decoded with the old checksum algorithm.

Version edit operations:

- create bucket;
- update bucket options;
- add table;
- remove table;
- add blob file;
- mark blob file pending deletion;
- clear pending blob deletion after safe physical cleanup;
- update WAL replay floor;
- update compaction metadata;

Publish rules:

- new SSTables become visible only after a manifest edit is durably published;
- the manifest table entry is authoritative for current level placement;
- an SSTable file may retain the level it was originally written with after a
  one-table move, but recovery must validate every other table property against
  the manifest before using the manifest level;
- manifest publish is an atomic backend operation;
- recovery loads the latest valid manifest state and then replays WAL records
  newer than the replay floor;
- obsolete files are removed only after they are no longer referenced by any
  live VersionSet or snapshot.

## 20. Levels And Compaction

Trine v1 uses leveled compaction:

- L0 may contain overlapping flush outputs;
- L1 and deeper levels are non-overlapping within a bucket;
- reads check newer levels before older levels;
- compaction picks input tables, merges sorted streams, writes new tables, and
  publishes a manifest edit.
- L0 compaction groups overlapping L0 tables and includes overlapping L1 tables
  before publishing L1 replacements;
- automatic L0 compaction may start from a local seed table and close only the
  overlapping L0/L1 span needed for that seed; unrelated L0 files can remain for
  later maintenance passes;
- L1 and deeper compaction uses level-size pressure from
  `target_table_bytes * level_size_multiplier^(level - 1)` and moves selected
  inputs down one level together with overlapping next-level inputs;
- a single input table may move down one level without rewriting when no
  next-level table overlaps its key range and the move preserves the level
  non-overlap invariant;
- compaction output SSTables are split at user-key boundaries according to
  `target_table_bytes`, except a single oversized user-key group may exceed the
  target by itself, and compaction carrying range tombstones may keep one wider
  output to preserve level non-overlap.

Compaction must preserve:

- latest visible value for each key;
- versions needed by active snapshots;
- point tombstones needed to hide lower-level records;
- range tombstones needed to hide covered lower-level records;
- bucket boundaries.
- range tombstones may be clipped to output table key spans only when the
  compaction scope proves older covered data outside the span has been removed;
  a request over all user keys is still partial when the picker did not include
  every live table in the bucket;
  partial compaction must retain the original tombstone bounds.
- point tombstones may be dropped only when the compaction input proves no
  older value for that user key can survive in another live table; partial
  compaction keeps point tombstones even when its selected inputs contain no
  older local value.

Version cleanup rules for a user key:

- keep every version with `sequence > oldest_retained_seq`;
- keep the newest version with `sequence <= oldest_retained_seq`;
- drop older versions only when no retained read version can read them;
- drop point tombstones only when all older covered versions are removed from
  the live bucket, not merely from the selected input tables;
- drop range tombstones only when all covered older versions are removed from
  the relevant compaction scope.

Compaction output is never visible until manifest publish completes.

## 21. Range Deletes

Range delete records are first-class v1 records.

Rules:

- range deletes are assigned a commit sequence;
- range deletes hide covered point versions with sequence <= tombstone sequence;
- range tombstones participate in memtable reads, SSTable reads, scans, and
  compaction;
- range tombstone indexes must allow reads to avoid scanning every tombstone in
  the database;
- point reads query tombstones whose start bounds can cover the user key;
- scan setup uses only tombstones whose bounds overlap the scan selector;
- table tombstone blocks remain on disk and are loaded on demand when a
  tombstone query needs that table;
- partial compaction must retain tombstones if older covered data may still
  exist outside the compaction input.

## 22. Iteration

Cursors are created from a snapshot sequence.

Required cursors:

- full range forward;
- full range reverse;
- bounded range forward;
- bounded range reverse;
- prefix forward;
- prefix reverse.

Cursor rules:

- return each user key at most once;
- return newest visible live value;
- skip point-deleted and range-deleted keys;
- preserve lexicographic ordering;
- hold a VersionSet guard for repeatability;
- expose fallible async advancement because storage reads can fail;
- merge source cursors through heap selection so one returned key advances only
  the sources that currently point at that key;
- use `advance_to` rather than restarting from the beginning when a merge or
  range cursor can provide a position hint.

## 23. Buckets

A database always contains a default bucket. A database may also contain named
buckets. Each bucket is an ordered KV namespace with independent LSM tables and
options.

Rules:

- direct `Db` read/write helpers target the default bucket;
- the default bucket is built in and callers do not open it by name for common
  usage;
- bucket names map to stable numeric ids;
- bucket ids appear in WAL and manifest records;
- cross-bucket write batches are atomic;
- bucket creation and option changes are manifest edits;
- dropping buckets requires snapshot-safe cleanup;
- compaction does not merge tables across buckets.

## 24. Caching

V1 includes:

- block cache;
- table metadata cache;
- filter cache;
- optional blob read cache.

Rules:

- caches are advisory and can be cleared without changing correctness;
- cache memory is bounded by options;
- block cache keys include block kind, table id, and block index;
- block cache eviction protects high-priority metadata entries such as index,
  filter, and range-tombstone blocks before low-priority data/blob blocks;
- snapshots never depend on cache entries for correctness;
- returned value guards may keep cached blocks alive until dropped.

## 25. Recovery

Persistent startup:

1. acquire writer lease when writable;
2. read current manifest through the storage backend;
3. load manifest edits and build VersionSet;
4. validate referenced table files and blob files named by table metadata;
5. replay WAL records newer than the replay floor;
6. rebuild memtables from replay;
7. detect obsolete unreferenced files;
8. fail closed on corruption except allowed final WAL tail truncation.

Recovery must be deterministic. If startup repairs safe temporary files, it
must record a repair report.

In-memory startup starts empty.

## 26. Configuration

V1 options include:

- storage mode;
- storage backend;
- runtime backend;
- create-if-missing;
- read-only;
- default bucket options;
- durability default;
- write buffer size;
- max immutable memtables;
- target table size;
- level size multiplier;
- max L0 files before slowdown or flush pressure;
- background worker count, defaulting to one for persistent writable databases;
- compression codec;
- compression profile;
- block size;
- filter policy;
- prefix extractor;
- prefix filter policy;
- index search policy;
- index search policy thresholds;
- block cache capacity;
- blob threshold;
- background worker count;
- fail-on-corruption policy.

Defaults must be conservative and documented.

## 27. Error Model

Errors are typed:

- `Io`
- `Corruption`
- `InvalidFormat`
- `UnsupportedFormat`
- `CodecUnavailable`
- `Conflict`
- `ReadOnly`
- `Closed`
- `BucketMissing`
- `InvalidOptions`
- `UnsupportedBackend`
- `UnsupportedDurability`
- `LeaseUnavailable`

Library code must not panic for expected runtime errors. Panics are only
acceptable for internal invariant violations in tests or debug assertions.

## 28. Observability

V1 exposes structured stats:

- live buckets;
- active snapshots;
- memtable bytes;
- immutable memtable count;
- L0 table count;
- per-level table count and bytes;
- WAL bytes pending flush/sync;
- block cache hits and misses;
- filter hits and misses;
- prefix filter hits, misses, false-positive probes, and skipped partitions;
- blob read count and bytes;
- compression ratio and compression/decompression time by codec id;
- index seek count by search policy;
- index search comparison/probe counts where practical;
- compaction input/output bytes;
- tombstone counts;
- blob bytes live, stale, and obsolete;
- blob GC runs, input bytes, output bytes, and discarded bytes;
- recovery replay bytes and time.

## 29. Required Tests

Correctness tests:

- put/get/delete round trip;
- atomic write batch success and failure;
- cross-bucket batch atomicity;
- snapshot repeatable read;
- read-version latest, retained floor, empty-batch, too-new, and expired
  behavior;
- snapshot survives compaction;
- optimistic transaction conflict on point read;
- optimistic transaction conflict on range read;
- range delete hides covered values;
- range scan returns sorted live keys;
- prefix scan returns only prefix matches;
- prefix scan skips incompatible tables safely;
- prefix filter false positives still run MVCC and tombstone checks;
- reverse iteration ordering;
- async cursor advancement stays snapshot-consistent across backend reads;
- blocking adapter delegates to the primary async engine;
- unsupported durability returns a typed error;
- missing writer lease rejects persistent writable open;
- WAL replay recovers committed batches;
- torn final WAL record is ignored;
- non-tail WAL corruption fails closed;
- manifest publish atomicity;
- SSTable checksum mismatch fails closed;
- compaction preserves MVCC visibility;
- compaction drops obsolete versions only when safe;
- blob value survives reopen and compaction;
- in-memory mode matches persistent mode for logical operations;
- concurrent readers observe consistent snapshots during writes and compaction.

Format tests:

- internal key ordering;
- block restart seek;
- search policy fallback returns the same block as canonical binary search;
- retired search-policy manifest tags decode to `auto`;
- sorted `advance_to` never skips the first matching visible key;
- filter false-negative rejection through deterministic fixtures;
- prefix extractor compatibility across manifest option changes;
- prefix filter partition skip behavior;
- footer version compatibility;
- unknown codec fail-closed behavior;
- portable memory backend builds for a WASM target.

## 30. Required Benchmarks

Benchmarks must cover persistent and in-memory modes where relevant:

- single-key put;
- batch write;
- random get;
- missing get;
- bounded range scan;
- prefix scan;
- prefix scan with matching and non-matching table partitions;
- snapshot read under concurrent writes;
- optimistic transaction commit;
- optimistic transaction conflict;
- WAL replay;
- flush throughput;
- compaction throughput;
- large inline values;
- separated blob values;
- block cache warm read;
- cold table read;
- primary async API overhead for hot memory reads and warm persistent reads;
- index seek policy comparison over small, medium, and large index arrays;
- long shared-prefix point reads before changing key encoding;
- cursor `advance_to` with near, far, and random targets;
- codec comparison for `none` and fast block compression over
  Trine data blocks, index blocks, and range tombstone blocks.

## 31. V1 Acceptance Gate

Trine KV v1 is complete when:

- all public API concepts in this spec are implemented;
- primary public database operations are async-first, with blocking APIs only as
  native adapters;
- persistent mode passes crash/recovery tests;
- in-memory mode passes the shared logical test suite;
- portable memory mode builds for a WASM target;
- MVCC snapshots and optimistic transactions pass conflict tests;
- range deletes work through memtable, SSTable, scan, and compaction paths;
- prefix filters are implemented and prefix scans remain correct under MVCC and
  range tombstones;
- compaction is enabled and snapshot-safe;
- block compression interface works with `none` and the fast default codec;
- optimized index search policies match canonical sorted search behavior;
- checksums guard WAL, blocks, and table footers;
- benchmark output exists for the required benchmark set;
- docs describe durability tradeoffs honestly.
