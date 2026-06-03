# Current Phase

## Status

Closed: No Kept Code

## Goal

Test whether smaller internal changes can make the existing batched point-read
APIs faster without changing public API or storage behavior.

## Scope

- Internal point-read path experiments for `BucketReader::get_many_sync`,
  `BucketReader::get_many_owned_sync`, and their async forms.
- Existing sequential vs batched point-read benchmark rows.
- Evidence about whether the attempted changes should be kept.

## Out Of Scope

- New public API surface.
- Storage format, MVCC rules, WAL, manifest, compaction, recovery,
  transaction, blob layout, writer-lease, or cold reopen behavior changes.
- Cross-bucket batching.
- Input-key semantic changes.

## Acceptance Gate

- Existing `get_many` results still preserve input order, duplicates, and
  `None` for missing or deleted keys.
- Benchmark evidence shows whether the attempted internal reuse is worth
  keeping.
- If benchmark evidence is negative, revert the experiment instead of keeping
  complexity.
- Diff checks and forbidden-term scans pass for kept files.

## Active Task Slice

```text
task591 [x] goal:test batch-local point-read reuse | scope:src/lsm/read.rs src/db.rs src/bucket.rs | verify:cargo test -q get_many --lib
task592 [x] goal:measure attempted internal changes | scope:benches/v1_bench.rs | verify:cargo bench --bench v1_bench
task593 [x] goal:revert unhelpful code experiment | scope:source and bench files | verify:git status --short
task594 [x] goal:record discovery evidence | scope:.phrase | verify:git diff --check
```

## Known Residuals

- Phase 141 left batched point-read benchmark timing mixed.
- This phase did not find a small internal change worth keeping.

## Evidence

- Tested a batch path that reused one memtable range-tombstone index per batch.
- Tested single-pass owned batch conversion to avoid an intermediate
  `Vec<Option<PointValue>>`.
- Tested batch memtable candidate collection that locks each memtable source
  once per batch.
- Tested a no-table/no-range-tombstone fast path for memory point reads.
- Batch size 4 remained slower for memory random point reads after the
  experiments. One run showed 1047 us sequential vs 2521 us batched.
- Batch size 16 was worse for memory and did not produce a stable persistent
  win.
- All source and benchmark experiments were reverted.

## Next Recommendation

- Do not continue with small `get_many` internals under the current random-key
  benchmark shape.
- The next useful phase should either target cold reopen/read-only open cost,
  or first redesign the batched point-read benchmark around locality and table
  grouping before implementing deeper changes.
