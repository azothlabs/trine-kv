# Current Phase

## Status

Active

## Goal

Reduce the current largest persistent-write and maintenance costs without
weakening confirmed-write durability or changing V1 storage formats.

## Scope

- Classify `persistent single-key put sync-data/sync-all` against the durability
  contract and existing batch write path.
- Improve blob level merge and compaction throughput when a safe table/blob
  rewrite optimization exists.
- Native persistent local-file workloads first; object-store/browser behavior
  stays on its existing durability path unless separately proven.
- Evidence from `v1_bench` grouped rows and targeted diagnostics.

## Out Of Scope

- Reducing `SyncData` or `SyncAll` semantics for confirmed single-key writes.
- Storage format changes.
- Public API changes unless evidence shows existing batch APIs cannot express
  the safe path.
- New compaction selection policy.
- Platform-io backend changes.
- Publishing, tagging, pushing, or release workflow changes.

## Acceptance Gate

- The single-key sync write row is classified as inherent per-commit sync cost
  unless a safe implementation change lowers it without changing durability.
- A retained maintenance optimization improves or explains blob level merge and
  compaction throughput with benchmark evidence.
- Focused tests cover the behavior boundary.
- Formatting, strict clippy, relevant tests, and diff whitespace checks pass
  before any commit.

## Active Task Slice

```text
task814 [x] goal:reject unsafe/unhelpful output-file durability change | scope:src/table.rs src/blob.rs src/db.rs | verify:targeted bench
task815 [ ] goal:record sync-write classification and maintenance evidence | scope:docs/benchmarks .phrase/evidence.md | verify:evidence review
task816 [ ] goal:profile and reduce table/blob payload rewrite cost | scope:src/table.rs src/blob.rs src/lsm src/db.rs | verify:compaction/blob diagnostics + grouped bench
```

## Evidence

- Current grouped benchmark medians put `persistent single-key put sync-data`
  around 523738 us and `sync-all` around 521558 us for 256 sequential writes.
- `persistent batch write sync-data` is much lower, around 21938 us for 1024
  writes, which shows the safe user-facing path for many confirmed writes is
  batching.
- WAL append lanes already keep the append writer open; sequential single-key
  sync cost is dominated by one storage sync per confirmed commit.
- WAL append lanes already keep the append writer open; changing output table
  or blob file durability from `SyncAll` to `SyncData` did not improve grouped
  maintenance rows and was rejected.
- A retained table-writer cleanup skips redundant sorting for already-sorted
  flush/compaction payloads and lets encoded table metadata be reused after
  blocking writes instead of re-reading table metadata from disk.

## Known Residuals

- Sequential single-key `SyncData`/`SyncAll` cannot be made cheap without
  relaxing the meaning of a confirmed write or adding a different explicit
  group-commit API.
- Blob level merge and compaction are still dominated outside output-file
  durability; the next blocker is likely table/blob encode, blob rewrite, or
  compaction payload assembly.

## Next Recommendation

- Continue with compaction/blob encode and rewrite profiling; do not revisit
  output-file `SyncData` without new filesystem-specific evidence.
