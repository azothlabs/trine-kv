# Trine KV V1 Durability Notes

This document describes the v1 durability contract as implemented today. It is
intentionally plain: storage behavior should be predictable under normal use,
and conservative when files are incomplete or suspicious.

## Scope

Persistent databases store WAL, SSTable, manifest, blob, lock, and repair-report
files under one database directory. In-memory databases do not promise
filesystem durability.

Durability in v1 means:

- committed persistent writes are appended to the WAL before they become visible
  in memtables;
- startup replays valid WAL records newer than the manifest replay floor;
- table, manifest, WAL, and blob corruption is detected with checksums or
  format checks;
- checksum fields use CRC-32C across WAL, manifest, SSTable, block, and blob
  records;
- uncertain formal storage files fail closed instead of being guessed into a
  live database state.

Durability in v1 does not mean:

- replication across machines;
- atomic multi-directory updates;
- online backup semantics;
- protection from broken disks, broken filesystems, or disabled platform syncs;
- encryption or authentication of on-disk files.

## Write Durability Modes

`WriteOptions::durability` controls how the WAL writer handles each committed
batch:

| mode | implementation | honest guarantee |
| --- | --- | --- |
| `Buffered` | write the framed WAL record and return after `write_all` succeeds | Fastest mode. The write is in the OS path, but no explicit flush or sync is requested. A crash or power loss can lose it. |
| `Flush` | call `File::flush` after the WAL append | Pushes Rust and OS-facing buffered bytes as far as `flush` provides. It is not a power-loss guarantee. |
| `SyncData` | call `File::sync_data` after the WAL append | Requests durable WAL file data without requiring all metadata to be synced. |
| `SyncAll` | call `File::sync_all` after the WAL append | Strongest commit mode for the WAL file. It asks the platform to sync file data and metadata. |

`DbOptions::durability` is the database-level durability floor. Per-write
`WriteOptions` can ask for a stronger mode, but cannot quietly weaken the mode
selected at open time. Passing a path to `Db::open` uses native persistent
options and defaults this floor to `SyncAll`, so ordinary callers get
database-style durability for confirmed writes. `Buffered` is available only
when callers explicitly choose it for data that can tolerate losing recent
confirmed writes after a crash or power loss.

`Db::persist(mode).await` applies the same persistence request to the WAL
through the primary async API. `Db::persist_sync(mode)` is the explicit sync
adapter. Neither form forces a memtable flush, runs compaction, or rewrites
already published tables.

## Host Persistent Backends

Native persistent databases use local files and the durability modes above.
Host-selected persistent backends expose only the durability they can honestly
provide:

- WASI persistence uses the host-preopened filesystem path on WASI targets,
  supports `Db::open` through that host boundary, defaults to `Flush`, and
  rejects `SyncData` and `SyncAll` until strict host sync guarantees are proven.
  Current WASI file work completes inline and does not advertise platform async
  I/O.
- Browser persistence is async-only on `wasm32-unknown-unknown`. It accepts
  `Buffered` and `Flush`, defaults to `Flush`, rejects `SyncData` and
  `SyncAll`, acquires a Web Locks writer lease for writable open, and uses
  WAL-backed async writes.

Synchronous browser persistent open, mutation, bucket creation, and maintenance
`*_sync` adapters return typed unsupported errors. Browser callers should use
`Db::open` and the async write, bucket, flush, compaction, and maintenance
methods.

## Commit Ordering

The writer coordinator serializes commit sequence assignment and memtable
updates. A non-empty commit follows this order:

1. validate batch-wide preconditions and optimistic transaction read sets;
2. assign the next commit sequence;
3. append a complete WAL record with checksums;
4. apply the batch to memtables;
5. publish the new last committed sequence.

Reads do not take the writer coordinator. Snapshots use sequence visibility, so
readers can keep a stable view while later writes, flushes, or compactions
continue.

## WAL Recovery

The WAL is append-only. Each record stores a magic value, format version, record
length, header checksum, payload checksum, commit sequence, and batch
operations.

Startup behavior:

- valid records replay in commit-sequence order;
- records at or below the manifest replay floor are skipped;
- a torn final record can be ignored as a tail truncation;
- checksum mismatch before the final tail fails closed;
- a WAL record is not visible unless it replays successfully after manifest
  load.

If a WAL record references a bucket that is missing from the manifest, startup
fails closed. This prevents a partially published bucket change from silently
turning into a different database state.

## Flush And Manifest Publish

Flush writes immutable memtable contents into SSTables and then publishes a
manifest edit. SSTable files are written with checked blocks and `sync_all`
before the manifest points at them. Manifest publishing writes a temporary file,
syncs it, and renames it into place.

The manifest stores:

- bucket definitions and options;
- live SSTable ids and compaction levels;
- live blob file ids plus per-table blob reference bytes, counts, and key
  spans;
- blob file ids that are obsolete but waiting for snapshot/read-pin-safe
  deletion;
- the WAL replay floor.

New SSTables become part of the live database only after the manifest edit is
published. If a crash leaves a formal table or blob file that is not referenced
by the manifest, v1 fails closed at startup. Safe temporary files are handled by
the repair policy described below.

On Unix, table and blob output paths sync the temporary file, rename it into
place, and sync the database directory before the manifest points at those
files. Manifest and repair-report publish paths sync their temporary file,
rename it into place, and then sync the parent directory. Windows attempts the
same directory-handle sync with backup semantics, but some filesystems and CI
runners reject directory flush with access denied even when file sync and
rename succeeded. In that case Trine treats the directory flush as a
best-effort Windows step rather than failing the write path. Rust's standard
library does not expose a portable directory sync for every target, so other
targets keep a best-effort rename path and the same conservative recovery
checks.

## SSTable And Blob Checks

SSTables are immutable files made of checked blocks:

- data blocks;
- range tombstone blocks;
- filter blocks;
- index blocks;
- properties block;
- footer.

Each block records codec id, decoded length, encoded length, and checksum. The
footer records magic, format version, section handles, and footer checksum.
Startup fails closed when a manifest-referenced table is missing, has corrupt
checksums, has an unsupported format, or disagrees with manifest metadata.

Large separated values live in blob files. Blob references are visible only
through committed WAL records or published SSTables. Blob files include
internal-key metadata, properties, footer checksums, and per-record value
checksums. Startup decodes manifest-referenced blob files and fails closed if a
referenced file is missing or corrupt. Cleanup cannot remove a blob that is
still referenced by a live table or an active snapshot.

Blob GC publishes replacement SSTables and the old blob file's pending-deletion
marker in one manifest edit. If a crash happens after that edit, writable open
can resume physical cleanup from the manifest. If the pending file is still
referenced by any live table, cleanup leaves it on disk and keeps the pending
marker for later repair.

## Compaction And Cleanup

Compaction writes new SSTables first and publishes them through a manifest edit.
Input tables and obsolete blob files are removed only after the new manifest
state is installed and active snapshot/read-pin counts allow cleanup.

Compaction must preserve:

- the newest visible point value for each key;
- versions pinned by active snapshots;
- point tombstones needed to hide older records;
- range tombstones needed to hide covered records;
- blob references still reachable from live tables or snapshots.

Persistent writable databases start one background maintenance worker by
default. `DbOptions::background_worker_count = 0` keeps maintenance fully
manual, and read-only or in-memory opens do not start workers.

Native async maintenance methods use the same durability rules through
operation-level async storage boundaries. Flush, compaction, blob GC,
obsolete-file cleanup, manifest publish, directory sync, WAL rewrite, and WAL
persistence are awaited through Trine storage or durability substrate helpers.
With `platform-io`, those helpers complete through Trine's managed platform
thread pool; with `platform-io-native`, each operation uses native async where
the selected target has audited support and the managed thread pool for the
remaining rows.

WASI and browser persistent options default to inline runtime execution with no
background worker threads. Browser persistent maintenance is exposed through
async flush, compaction, and budgeted maintenance methods so hosts can run work
from their scheduler. If browser write pressure cannot await maintenance inside
write preflight, the write returns `RuntimeBusy` until the caller runs async
maintenance and retries.

Automatic compaction may run after flush when L0 file pressure exceeds
`DbOptions::max_l0_files`. Automatic L0 compaction can choose a local
overlapping key span and leave unrelated L0 files for later passes. The same
recovery and manifest-publish rules apply to manual and automatic compaction.
`Db::flush().await` and `Db::flush_sync()` are foreground barriers for writes
committed before the call: those writes must be published out of active and
immutable memtables before the method returns `Ok(())`.
`Db::compact_range().await` and `Db::compact_range_sync()` wait for overlapping
active compaction reservations instead of treating a busy maintenance guard as
success.

When immutable memtables or L0 files exceed configured limits, writes apply
pressure handling before taking the writer coordinator. They may wait for the
background worker or help with one maintenance pass, while table building and
compaction output construction stay outside the short publish lock.

Persistent table readers reuse a cached table file handle for block reads. The
handle is only a resource optimization; manifest versions and snapshot/read pins
still decide whether a table file remains live. L0/L1 tables pin hot
index/filter metadata, while deeper tables keep partition metadata lazy and
cacheable.

## Recovery And Repair Policy

Persistent startup is conservative:

1. acquire the database directory lock unless opened read-only;
2. load the manifest;
3. validate referenced tables and blobs;
4. reject unexpected formal table/blob files;
5. replay WAL records newer than the replay floor;
6. rebuild memtables from replayed records.

Default corruption policy is `FailClosed`. In that mode, startup returns an
error for corruption, unsupported files, missing referenced files, unreferenced
formal storage files, and safe temporary files.

`FailOnCorruptionPolicy::RepairSafeTemporaryFiles` is intentionally narrow. It
can delete known safe temporary files and writes a repair report. It does not
repair WAL corruption, manifest corruption, table corruption, missing referenced
files, unreferenced formal storage files, or blob corruption.

## Locking And Read-Only Open

Writer opens take an exclusive process lock in the database directory. A second
writer fails while the lock is held. The lock is released after the writer
coordinator is idle during close/drop, so another process cannot open while the
current process is still publishing files.

Read-only opens do not take the writer lock and do not create a WAL writer. They
still load the manifest, validate referenced files, and replay WAL records into
memory. Use read-only opens for inspection of a stable directory state; v1 does
not define live multi-process read coordination with a concurrent writer.

## Operational Guidance

- Native persistent open already defaults to `SyncAll`; keep that default for
  ordinary database data.
- Use `Buffered` only for caches, indexes, or other data that can tolerate
  losing recent confirmed writes after a crash or power loss.
- Call `Db::persist(SyncAll).await` or `Db::persist_sync(SyncAll)` after a group
  of explicit lower-durability writes when a batch-level sync point is
  acceptable.
- Call `Db::flush().await` or `Db::flush_sync()` when reducing WAL replay time
  matters.
- Keep the whole database directory on one local filesystem.
- Treat any fail-closed startup error as a signal to preserve the directory for
  inspection before deleting files.
- Use explicit safe-temporary-file repair only when the caller accepts the
  repair report and understands that formal storage files are not repaired.
