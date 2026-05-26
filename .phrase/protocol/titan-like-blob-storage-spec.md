# Titan-Like Large-Value Storage Specification

Date: 2026-05-26
Status: Accepted as the next storage-format direction before implementation

## 1. Purpose

This specification upgrades Trine KV's primitive large-value offload path into
a Titan-like key-value separation subsystem.

The goal is narrow:

- keep small values inline in SSTables;
- separate only values at or above the configured threshold;
- reduce ordinary LSM compaction write amplification for large values;
- keep WAL, MVCC, recovery, snapshot, and GC behavior explicit.

This is a Trine design. Titan is a design reference only. Do not copy Titan
source code, depend on Titan, or import Titan storage formats.

Reference signals used for this spec:

- PingCAP Titan overview: value separation is done during flush and compaction,
  blob records keep key metadata, and GC uses blob-reference information from
  SSTables.
- TiKV Titan configuration: Titan is intended for large values and has known
  space/range-scan tradeoffs.
- Titan repository: Titan is a RocksDB plugin inspired by WiscKey, not a crate
  or code dependency for Trine.
- Titan design article: Titan keeps large values in WAL and creates blob files
  during memtable flush to avoid invasive foreground-write changes.

## 2. Durable Decisions

Trine's large-value subsystem follows these rules:

- WAL records always store the complete user value.
- Memtables initially store the complete user value.
- Large-value separation happens during flush and compaction.
- SSTables store either inline value bytes or a `BlobIndex`.
- Blob files store the actual large value bytes.
- Reads first find the visible LSM record under MVCC and range-delete rules.
  Blob bytes are read only after that record is known to be visible.
- GC is part of the storage contract, not an optional later cleanup.
- In-memory mode keeps values inline unless a future phase proves an in-memory
  blob store is necessary.
- Point deletes and range deletes remove LSM references, but blob file
  reclamation goes through GC and snapshot-safe file lifetime rules.

## 3. Non-Goals For The First Pass

- Do not separate small values.
- Do not separate values during WAL append or foreground memtable writes.
- Do not add punch-hole GC first.
- Do not force aggressive Level Merge without correctness and benchmark
  evidence.
- Do not change user-visible MVCC semantics.
- Do not delete old blob files while any snapshot or read pin can still reach
  an old `BlobIndex`.

## 4. Value Model

The stable value reference model becomes:

```text
ValueRef:
  Inline(value_bytes)
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

Field rules:

- `file_id` identifies one manifest-tracked blob file.
- `offset` points to the start of the blob record.
- `encoded_len` is the stored byte length after optional compression.
- `value_len` is the original user value length.
- `value_checksum` protects decoded user value bytes.
- `record_checksum` protects the full blob record header and payload.
- `compression` is a Trine codec id, not a crate name.

The old primitive shape:

```text
Blob { file_id, offset, len, checksum }
```

is no longer sufficient because it cannot validate record metadata, compressed
payloads, or GC liveness against the exact blob record.

## 5. Blob Record Model

Each blob record stores enough key/version metadata for GC to decide whether
the record is still referenced by the current LSM state:

```text
BlobRecord {
  internal_key {
    user_key,
    sequence,
    value_kind,
    batch_index,
  },
  value_len,
  encoded_len,
  compression,
  value_checksum,
  record_checksum,
  value_bytes,
}
```

Rules:

- blob records are written in internal-key order during flush and compaction;
- `value_kind` must be `Put`;
- point deletes and range tombstones do not get blob records;
- the record checksum covers all record metadata that affects decoding and
  liveness validation;
- GC must be able to read only the blob file and recover the user key, sequence,
  and batch index for every blob record.

## 6. Blob File Format

Blob files use Trine-owned magic values and version numbers.

```text
BlobFile:
  Header
  BlobRecord*
  PropertiesBlock
  Footer
```

Header:

```text
magic
format_version
file_id
creation_sequence
bucket_options_digest
blob_options
header_checksum
```

Properties block:

```text
record_count
value_bytes
encoded_bytes
compression_saved_bytes
smallest_internal_key
largest_internal_key
smallest_sequence
largest_sequence
referenced_table_count
properties_checksum
```

Footer:

```text
properties_offset
properties_len
footer_checksum
magic
```

Recovery must reject:

- missing manifest-referenced blob files;
- wrong magic or unsupported format version;
- footer, properties, record, or value checksum mismatch;
- `file_id` mismatch between path, header, and manifest;
- unordered blob records in a file that claims ordered records;
- record metadata that cannot decode to a valid internal key.

## 7. Options

Bucket-level options:

- `blob_threshold_bytes`: default `1 MiB`, with smaller thresholds allowed for
  large-value-heavy buckets;
- blob record compression follows the bucket compression profile, using Trine's
  `none` or `fast-lz4-block` codec ids;
- `blob_level_merge_policy`: default `auto`, with `disabled` and `always`
  available for workload-specific tuning.

Database-level persistent options:

- `blob_gc_enabled`: default `true` for persistent mode;
- `blob_gc_discardable_ratio`: default `0.5`;
- `blob_gc_min_file_bytes`: default large enough to avoid GC on tiny files.

Validation:

- threshold must be non-zero;
- GC ratio must be in `(0.0, 1.0]`;
- in-memory mode ignores on-disk blob file options and keeps values inline.

## 8. Flush Behavior

Flush consumes sorted memtable records and writes SSTables plus optional blob
files.

For each retained point record:

- if there is no value, keep the tombstone in the SSTable;
- if value size is below `blob_threshold_bytes`, store `Inline(value_bytes)`;
- if value size is at least `blob_threshold_bytes`, append a `BlobRecord` and
  store a `BlobIndex` in the SSTable.

Crash-safe publish order:

1. write table and blob temporary files;
2. sync table and blob temporary files;
3. rename table and blob files into formal names;
4. sync the parent directory;
5. publish one manifest edit referencing both SSTables and blob files;
6. install the new LSM tree version.

If manifest publish fails, the newly written table/blob files are not live.
Recovery must treat formal unreferenced files according to
`FailOnCorruptionPolicy`.

## 9. Compaction Behavior

Compaction reads logical records from input SSTables and applies the existing
MVCC retention rules.

For retained records:

- inline small values remain inline;
- existing `BlobIndex` records may be kept unchanged during normal compaction;
- large inline values created by WAL replay or legacy tables are separated when
  written into new SSTables;
- if `blob_level_merge_policy` is `auto`, compaction rewrites retained
  `BlobIndex` values when the output keeps references to multiple blob files or
  when compaction drops part of an input blob file's references;
- if `blob_level_merge_policy` is `always`, compaction rewrites retained
  `BlobIndex` values whenever the output still references blob values;
- if `blob_level_merge_policy` is `disabled`, compaction keeps retained
  `BlobIndex` values unchanged.

For dropped records:

- point delete and range delete decisions remove LSM references only;
- blob file bytes become discardable estimates after table properties and live
  manifest state prove references disappeared;
- physical blob deletion remains snapshot-gated.

## 10. SSTable Blob-Reference Properties

Each SSTable properties block records blob references used by that table:

```text
BlobReferenceProperties:
  per_blob_file:
    file_id
    referenced_bytes
    referenced_record_count
    smallest_internal_key
    largest_internal_key
```

These properties are required for:

- live blob byte accounting;
- stale/discardable byte estimates after compaction;
- recovery cross-checks between manifest, tables, and blob files;
- GC candidate selection.

## 11. Manifest Metadata

The manifest is the source of truth for live table and blob files. Live blob
references are stored through SSTable properties referenced by the manifest.
Durable pending deletion state is stored directly in the manifest.

Per-blob-file state is derived from live table properties and blob file
properties:

```text
BlobFileMetadata {
  file_id,
  total_bytes,
  referenced_bytes_estimate,
  discardable_bytes_estimate,
  creation_sequence,
  pending_deletion_sequence,
  smallest_internal_key,
  largest_internal_key,
}
```

Rules:

- adding SSTables and blob files happens in one manifest edit;
- GC output and old-file pending deletion happen in one manifest edit;
- deleting old blob files happens only after no active snapshot, read pin, or
  old table handle can still reach the old `BlobIndex`;
- cleanup must refuse to delete a pending blob file if any manifest-live table
  still references it;
- recovery must rebuild or validate blob estimates from live SSTable
  properties when needed.

## 12. Read Path

Point read:

1. find the newest visible LSM point record;
2. check covering range tombstones;
3. if the value is inline, return it;
4. if the value is a `BlobIndex`, read and verify the blob record and return
   decoded bytes.

Range/prefix scan:

- current value-returning iterators read blob bytes as rows are returned;
- value-lazy iterators (`range_lazy` and `prefix_lazy`) return keys plus lazy
  values, and read blob bytes only when the caller asks for the value;
- stats must count blob read operations and bytes.

Blob reads must validate:

- record checksum;
- value checksum;
- decoded length;
- compression id;
- that the record's internal key matches the LSM `BlobIndex` owner when the
  caller has enough context to check it.

Point reads should use `BlobIndex.offset` to read the indexed record directly.
Full blob-file decode is still required for recovery validation. Point reads,
value-lazy reads, and GC live-record copying should use indexed blob reads when
the exact `BlobIndex` is known.

## 13. Blob GC

GC has two parts: estimate stale bytes and rewrite live blob records.

Estimate maintenance:

- after compaction, compare input and output SSTable blob-reference properties;
- when references disappear from live SSTables, add the removed referenced bytes
  to the owning blob file's discardable estimate;
- select candidates when
  `discardable_bytes / total_bytes >= blob_gc_discardable_ratio` and
  `total_bytes >= blob_gc_min_file_bytes`.

GC rewrite:

1. pin the current LSM versions and snapshot floor;
2. read candidate blob-file properties from each footer/properties block
   without decoding every record payload;
3. batch all candidates that pass the GC threshold in the same maintenance
   publish;
4. for each live SSTable reference to a selected blob file, read the referenced
   blob record by `BlobIndex.offset` and validate that the
   `BlobRecord` metadata matches the exact internal key and old `BlobIndex`;
5. copy still-referenced values into a new blob file and publish equivalent
   `BlobIndex` records for the same internal keys;
6. drop blob records that are no longer referenced by live SSTables;
7. publish one manifest edit that marks the old blob files pending deletion at
   the GC publish sequence.

GC must not create a new user-visible MVCC version. It rewrites blob indexes for
the same internal keys as maintenance work.

The old blob file can be physically removed only after no snapshot or read pin
can still reach the old `BlobIndex`.

Idempotence:

- a crash before manifest publish leaves GC output unreferenced;
- a crash after manifest publish can resume cleanup from manifest metadata;
- repeating GC for the same file must either find no exact old references or
  produce an equivalent live state.

## 14. Recovery

On persistent open:

- load manifest as the source of truth;
- verify every manifest-referenced table exists and passes table checks;
- verify every manifest-referenced blob file exists and passes blob checks;
- reject unreferenced formal table/blob files unless the repair policy handles
  them explicitly;
- remove safe temporary table/blob files only when repair policy allows;
- replay WAL into memtables with inline user values;
- separate large replayed values again only when those memtables flush.

Corruption policy remains fail-closed by default.

## 15. Stats

`DbStats` must expose at least:

- `live_blob_files`;
- `live_blob_bytes`;
- `stale_blob_files`;
- `stale_blob_bytes`;
- `blob_gc_runs`;
- `blob_gc_input_bytes`;
- `blob_gc_output_bytes`;
- `blob_gc_discarded_bytes`;
- `blob_read_count`;
- `blob_read_bytes`.

`obsolete_blob_*` aliases may stay for compatibility until the next breaking
API sweep, but new docs should use `stale_blob_*`.

## 16. Testing Requirements

Correctness:

- large value write/read/reopen;
- mixed small and large values;
- overwrite large value, compact, GC, reopen;
- delete large value, compact, GC, reopen;
- range tombstone covering large values;
- snapshot holds old `BlobIndex` while GC runs and old value remains readable;
- missing blob file fails on open;
- corrupt blob header/footer/properties/record/value checksums fail closed;
- WAL replay rebuilds inline memtables and separates on flush.

Crash windows:

- blob temporary file written but not renamed;
- blob formal file exists but manifest publish failed;
- table formal file exists but blob file is missing;
- manifest references a blob file with bad metadata;
- GC output exists without manifest publish;
- old blob file marked pending deletion but still needed by a snapshot.

Benchmarks:

- write-heavy large values;
- point read large values;
- range scan large values;
- mixed small/large workload;
- GC rewrite throughput and foreground read impact.

## 17. Implementation Order

1. Stabilize `BlobIndex` and `BlobFile` format with decode/encode tests.
2. Make flush produce Titan-like blob files and SSTable `BlobIndex` records.
3. Add manifest and recovery correctness for blob file metadata.
4. Add SSTable blob-reference properties and stats.
5. Add snapshot-safe blob GC.
6. Add compaction-integrated blob rewriting only after correctness and
   benchmark evidence.

Each step must add focused tests before broad implementation changes and then
run the current Rust verification gate.

## 18. Acceptance Gate

Trine behaves as a normal LSM KV for small values and as a Titan-like
large-value LSM for values at or above the configured threshold:

- ordinary compaction does not repeatedly rewrite large value bytes without an
  explicit blob policy;
- blob files are crash-safe, snapshot-safe, recoverable, measurable, and
  reclaimable by GC;
- persistent recovery validates table/blob/manifest consistency;
- in-memory mode remains inline and simple;
- benchmark evidence exists before forcing aggressive Level Merge beyond the
  automatic policy.
