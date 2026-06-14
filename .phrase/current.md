# Current Phase

## Status

Complete

## Goal

Measure and reduce flush, compaction, and blob-GC write amplification while
preserving MVCC visibility, manifest publish semantics, blob reference safety,
durability, and storage formats.

## Scope

- Flush throughput, compaction throughput, blob-GC rewrite, and blob-level
  merge benchmark rows.
- Output table/blob write counts and bytes, manifest publish counts and
  latency, directory sync counts and latency, obsolete cleanup timing, object
  delete counts, and compaction selection behavior.
- One or more measured write-amplification reductions if diagnostics expose a
  safe dominant cost.

## Out Of Scope

- Storage format changes.
- MVCC visibility, snapshot, range-delete, prefix-filter, table format,
  manifest durable-cutover behavior, blob reference safety, platform-io,
  publishing, tagging, pushing, or release workflow changes.
- Read-path block-cache policy, compression codec changes, public durability
  defaults, and startup/recovery optimization.

## Acceptance Gate

- Diagnostics classify table/blob write requests, manifest publish requests,
  directory sync requests, object delete requests, compaction table input and
  output bytes, blob-GC input/output/discarded bytes, and cleanup publish
  behavior before optimization.
- Any retained code change preserves manifest publish semantics, blob file
  reachability, snapshot-delayed cleanup, durability, and storage formats.
- Focused flush/compaction/blob tests pass.
- Strict clippy passes.
- Single-run and grouped benchmark evidence records before/after behavior.

## Active Task Slice

```text
task792 [x] goal:add write-amplification diagnostics | scope:benches/v1_bench.rs | verify:TRINE_BENCH_RUNS=1 cargo bench --bench v1_bench
task793 [x] goal:optimize measured maintenance write-amplification cost | scope:src/db.rs benches/v1_bench.rs | verify:cargo test -q compaction --lib && cargo test -q blob --lib
task794 [x] goal:record maintenance write-amplification evidence | scope:docs/benchmarks .phrase/evidence.md | verify:git diff review
```

## Evidence

- Phase 178 completed prefix table cursor metadata reuse.
- Benchmark baseline refresh previously identified compaction throughput,
  blob-GC rewrite, and blob-level merge as the largest grouped write-side
  costs.
- Added write-amplification diagnostics for flush, compaction, blob-GC rewrite,
  and blob-level merge. The diagnostics classify output writes, manifest
  publishes, directory syncs, object deletes, compaction bytes, blob-GC bytes,
  and wall time.
- The measured foreground amplification was cleanup metadata publication after
  blob maintenance. Blob-GC rewrite previously used three manifest publishes;
  blob-level merge used two. Both still had to delete obsolete files
  immediately.
- The retained change keeps foreground blob-file deletion immediate but defers
  clearing manifest pending-deletion metadata to later cleanup boundaries such
  as flush, open, or close.
- After the change, blob-GC rewrite uses two manifest publishes and blob-level
  merge uses one, while write-object, directory-sync, and delete counts remain
  unchanged.

## Known Residuals

- The optimization intentionally reduces foreground manifest publishes, not
  physical table/blob output count. Obsolete blob files are still deleted before
  foreground compaction returns when no snapshot pin keeps them reachable.

## Next Recommendation

- Move next to block-cache/decode or search-policy work based on grouped
  benchmark evidence.
