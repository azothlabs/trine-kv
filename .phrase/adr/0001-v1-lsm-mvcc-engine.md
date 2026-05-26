# ADR 0001: V1 LSM MVCC Engine

Date: 2026-05-25

## Status

Accepted for specification.

## Context

Trine KV starts as a clean embedded key-value database project. It is not a
rewrite of another local project and does not inherit another engine's object
model. The database should be a serious LSM-tree based KV, not a small demo that
only becomes database-shaped after repeated rewrites.

Trine specs, tests, and local design notes are the source of truth. The project
should stand on its own design.

## Decision

Trine KV v1 is a complete embedded LSM MVCC database target:

- LSM-tree storage with memtables, WAL, immutable SSTables, manifest, and
  compaction.
- MVCC from the first on-disk format through internal keys.
- Repeatable snapshots.
- Atomic write batches.
- Serializable optimistic transactions.
- Buckets with cross-bucket atomic write batches.
- Persistent local-file mode.
- In-memory mode with the same logical engine and volatile storage.
- Range and prefix iteration.
- Point delete and range delete.
- Prefix extractors and prefix filters for common prefix scans.
- Pluggable block compression with a fast default codec and compact optional
  codec.
- Block filters, block cache, table metadata cache, and checksums.
- Search-policy aware immutable indexes for point lookup and iterator seek.
- Crash recovery, corruption detection, and fail-closed startup rules.

The v1 implementation may be delivered in slices, but the accepted v1 design is
not a toy subset. Any slice that lands must preserve the final v1 contracts.

## Non-Goals

- No server process.
- No SQL layer.
- No distributed replication.
- No dependency on another storage engine.
- No compatibility promise for another database's file format.
- No unsafe code requirement.

## Consequences

- The first specs must define the storage format and MVCC rules before coding.
- Tests must be written against database behavior, not just individual structs.
- If implementation exposes a gap, update Trine specs first instead of silently
  changing code behavior.
- The engine will take longer to implement than a minimal WAL plus map, but it
  avoids a format rewrite for MVCC, transactions, and compaction safety.
