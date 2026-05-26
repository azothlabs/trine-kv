# Trine KV V1 Specification

Date: 2026-05-25

## 1. Purpose

Trine KV is a clean embedded key-value database implemented in Rust. It uses an
LSM tree as its primary data structure and supports both volatile in-memory mode
and durable local-file mode.

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

## 3. Vocabulary

- **User key**: byte key supplied by the caller.
- **Internal key**: encoded key used inside memtables and SSTables.
- **Sequence**: monotonic commit number assigned by Trine.
- **Snapshot sequence**: read boundary for repeatable reads.
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

## 4. Storage Modes

Trine supports the same public API in both modes.

### 4.1 Persistent Mode

Persistent mode stores WALs, SSTables, manifests, value files, locks, and repair
reports under a local directory.

Rules:

- only one process may open a database path for writing;
- startup must acquire an exclusive lock unless opened read-only;
- all files include format version and checksum coverage;
- manifest publish is atomic;
- crash recovery must replay committed WAL records after the manifest snapshot.

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

## 5. Public API Shape

The exact Rust names may evolve, but the v1 API must expose these concepts:

```rust
Db::open(options) -> Result<Db>
Db::memory(options) -> Result<Db>
Db::put(key, value) -> Result<()>
Db::get(key) -> Result<Option<Value>>
Db::range(range) -> Result<Iter>
Db::prefix(prefix) -> Result<Iter>
Db::default_bucket() -> Result<Bucket>
Db::bucket(name) -> Result<Bucket>
Db::bucket_with_options(name, options) -> Result<Bucket>
Db::persist(mode) -> Result<()>
Db::flush() -> Result<()>
Db::compact_range(range) -> Result<()>
Db::snapshot() -> Snapshot
Db::transaction(options) -> Transaction
Db::stats() -> DbStats

DbOptions::with_default_bucket_options(options) -> DbOptions

Bucket::get(key) -> Result<Option<Value>>
Bucket::put(key, value) -> Result<()>
Bucket::delete(key) -> Result<()>
Bucket::range(range) -> Result<Iter>
Bucket::prefix(prefix) -> Result<Iter>

WriteBatch::put(key, value)
WriteBatch::delete(key)
WriteBatch::delete_range(range)
WriteBatch::put_bucket(bucket, key, value) -> Result<()>
WriteBatch::delete_bucket(bucket, key) -> Result<()>
WriteBatch::delete_range_bucket(bucket, range) -> Result<()>
Db::write(batch, write_options) -> Result<CommitInfo>

Transaction::get(key) -> Result<Option<Value>>
Transaction::put(key, value)
Transaction::delete(key)
Transaction::read_range(range) -> Result<()>
Transaction::get_bucket(bucket, key) -> Result<Option<Value>>
Transaction::put_bucket(bucket, key, value) -> Result<()>
Transaction::delete_bucket(bucket, key) -> Result<()>
Transaction::delete_range_bucket(bucket, range) -> Result<()>
Transaction::read_range_bucket(bucket, range) -> Result<()>
```

API rules:

- `Db`, `Bucket`, and `Snapshot` handles are cloneable and thread-safe.
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
- named buckets are optional and used for logical isolation or different
  per-bucket options;
- Iterators keep the read VersionSet alive.
- Iterators are snapshot-consistent.
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
- `oldest_active_snapshot_seq` controls compaction cleanup;
- dropping a snapshot releases its pin;
- no compaction may remove a version that an active snapshot could still read.

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
- append WAL;
- apply to the active memtable;
- publish the new `last_committed_seq`;
- optionally trigger flush or compaction scheduling.

The writer coordinator may serialize commits. That is acceptable for v1. Reads
must not require the writer coordinator.

Background work:

- `background_worker_count == 0` disables background maintenance;
- persistent databases with `background_worker_count > 0` start maintenance
  workers after open;
- flush immutable memtables;
- compact SSTables;
- clean obsolete files after snapshot safety allows it;
- never publish partially written tables.
- background maintenance failures must be returned by a later write, `flush`,
  or `compact_range` call instead of being silently ignored.

## 11. Durability Modes

Write options include a durability mode:

```text
Buffered   -> append to WAL buffer and apply to memtable
Flush      -> flush WAL bytes to the OS
SyncData   -> fsync data needed for WAL durability
SyncAll    -> fsync WAL and required directory or metadata state
```

The exact platform implementation may vary, but the names must remain honest.
A write acknowledged with `SyncAll` must survive a normal power-loss model as
far as the platform allows.

`Db::persist(mode)` forces pending durable state according to the requested
mode.

## 12. WAL Format

The WAL is append-only and contains framed batch records.

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
object in in-memory mode.

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
- prefix compression is allowed inside a block;
- every block has checksum coverage;
- every block declares compression codec id;
- codec id `none` must be supported.

Index rules:

- top-level table index maps key ranges to blocks;
- partitioned index is allowed and preferred for large tables;
- index blocks keep a canonical sorted order for validation, range traversal,
  and debugging;
- index blocks may also carry a search layout optimized for point seek;
- table properties include smallest/largest user key, sequence range,
  referenced blob file ids, referenced blob bytes, record counts, and blob key
  spans.

Filter rules:

- each table may have point-key Bloom filters;
- each table may have prefix filters when a bucket prefix extractor is
  configured;
- filters are partitionable by table key range for large tables;
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

- opening a table reads only footer, properties, index, and filter metadata;
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
change MVCC visibility, iterator ordering, or table publish rules.

Required index APIs:

```text
seek_ge(key)
seek_gt(key)
seek_le(key)
advance_to(cursor, key)
```

Allowed policies:

- linear search for tiny arrays;
- binary or branchless binary search for general sorted arrays;
- Eytzinger layout for immutable search-only arrays;
- galloping search followed by local binary search when a cursor has a useful
  current-position hint.

Design rules:

- data blocks and SSTable record order remain sorted; do not store primary data
  only in Eytzinger order;
- Eytzinger layout is allowed for top-level indexes, partition indexes, block
  restart indexes, and other immutable offset arrays;
- if Eytzinger is used, the canonical sorted order must still be reconstructable
  or separately available for verification and sequential traversal;
- galloping search is used only when the target is likely to be near or after a
  known cursor position;
- random point lookup must not default to galloping search without evidence;
- search policy thresholds are configuration and benchmark decisions, not
  public API contracts;
- every optimized policy must have a simple sorted-search fallback;
- unsafe code is not required for these policies.

Default policy shape:

```text
small index:
  linear search

medium sorted index:
  binary or branchless binary search

large immutable index:
  Eytzinger search layout if benchmarks justify the memory cost

cursor advance with position hint:
  galloping search, then local binary search
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

Rules:

- blob references are visible only through committed WAL replay or published
  SSTables;
- blob records include key/version metadata so GC can check whether a blob is
  still referenced by the current LSM state;
- blob files include header, ordered records, properties, footer, and checksums;
- ordinary compaction may keep existing `BlobIndex` records instead of
  rewriting large values;
- blob GC is snapshot-safe and recoverable;
- cleanup cannot remove a blob file referenced by any live table, active
  snapshot, read pin, or pending old tree version.

## 19. Manifest And VersionSet

The manifest stores a sequence of version edits.

Version edit operations:

- create bucket;
- update bucket options;
- add table;
- remove table;
- add blob file;
- remove blob file;
- update WAL replay floor;
- update compaction metadata;

Publish rules:

- new SSTables become visible only after a manifest edit is durably published;
- the manifest table entry is authoritative for current level placement;
- an SSTable file may retain the level it was originally written with after a
  one-table move, but recovery must validate every other table property against
  the manifest before using the manifest level;
- manifest publish is atomic;
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

- keep every version with `sequence > oldest_active_snapshot_seq`;
- keep the newest version with `sequence <= oldest_active_snapshot_seq`;
- drop older versions only when no active snapshot can read them;
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

Iterators are created from a snapshot sequence.

Required iterators:

- full range forward;
- full range reverse;
- bounded range forward;
- bounded range reverse;
- prefix forward;
- prefix reverse.

Iterator rules:

- return each user key at most once;
- return newest visible live value;
- skip point-deleted and range-deleted keys;
- preserve lexicographic ordering;
- hold a VersionSet guard for repeatability;
- expose fallible iteration because storage reads can fail;
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
- snapshots never depend on cache entries for correctness;
- returned value guards may keep cached blocks alive until dropped.

## 25. Recovery

Persistent startup:

1. acquire process lock;
2. read current manifest pointer;
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
- create-if-missing;
- read-only;
- default bucket options;
- durability default;
- write buffer size;
- max immutable memtables;
- target table size;
- level size multiplier;
- max L0 files before slowdown or flush pressure;
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
- snapshot survives compaction;
- optimistic transaction conflict on point read;
- optimistic transaction conflict on range read;
- range delete hides covered values;
- range scan returns sorted live keys;
- prefix scan returns only prefix matches;
- prefix scan skips incompatible tables safely;
- prefix filter false positives still run MVCC and tombstone checks;
- reverse iteration ordering;
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
- Eytzinger search layout preserves canonical index ordering externally;
- galloping `advance_to` never skips the first matching visible key;
- filter false-negative rejection through deterministic fixtures;
- prefix extractor compatibility across manifest option changes;
- prefix filter partition skip behavior;
- footer version compatibility;
- unknown codec fail-closed behavior.

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
- cold table read.
- index seek policy comparison over small, medium, and large index arrays;
- iterator `advance_to` with near, far, and random targets.
- codec comparison for `none` and fast block compression over
  Trine data blocks, index blocks, and range tombstone blocks.

## 31. V1 Acceptance Gate

Trine KV v1 is complete when:

- all public API concepts in this spec are implemented;
- persistent mode passes crash/recovery tests;
- in-memory mode passes the shared logical test suite;
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
