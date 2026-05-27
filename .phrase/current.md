# Current Phase

## Status

Complete

## Goal

Fix the measured point-read bottleneck at its source: public read ownership,
shared SSTable block access, and avoidable overlapping-L0 read amplification.

## Scope

- Add a point-read value handle that can return inline SSTable values without
  copying bytes out of decoded data blocks.
- Let `BucketReader` keep a stable point-read source snapshot for repeated
  reads under one snapshot.
- Update the Azoth benchmark adapter to use the reader/value path so it
  measures point lookup instead of forcing an owned `Vec` for every read.
- Reduce block-cache shared-state contention on the measured point-read path.
- Trigger local compaction when L0 key spans overlap, because overlapping L0
  files directly multiply point-read table probes.

## Out Of Scope

- Changing MVCC, WAL, manifest, table format, compaction retention, or recovery
  rules.
- Keeping unbounded decoded data block pinning: the experiment improved 4/8
  threads but regressed 16/32 threads and was rejected.
- Replacing the global block cache without a bounded design.

## Acceptance Gate

- `BucketReader::get` returns a `PointValue` with `as_bytes`.
- `BucketReader::get_owned` keeps the explicit owned-value path.
- Existing `get` APIs keep returning owned `Vec<u8>` for compatibility.
- A reader remains correct if memtable data is flushed after reader creation.
- The benchmark adapter uses the reader value-handle path.
- Block-cache hit/miss accounting no longer writes one global atomic counter on
  every hit.
- With background workers disabled, foreground `flush()` compacts overlapping
  L0 tables before read-heavy states keep paying repeated table-filter misses.
- Full Rust tests pass.
- Evidence records which cache paths improved and which alternatives were
  rejected.

## Active Task Slice

```text
task152 [x] goal:point value handle | scope:src/point_value.rs src/table.rs src/lsm/read.rs | verify:tests
task153 [x] goal:reader stable source snapshot | scope:src/bucket.rs src/db.rs src/lsm/read.rs | verify:flush-after-reader test
task154 [x] goal:benchmark adapter uses value handle | scope:azoth-kv-bench trine_benchmark.rs | verify:bench check
task155 [x] goal:block-cache counter sharding | scope:src/cache.rs | verify:cache tests/clippy
task156 [x] goal:overlapping-L0 pressure trigger | scope:src/lsm/version.rs src/db.rs | verify:L0 overlap tests
task157 [x] goal:evidence and verification closeout | scope:.phrase/evidence.md cargo test | verify:full gate
```

## Known Blockers

- The measured threaded-read source problem is closed: diagnostics now show one
  table probe per data-block read after overlapping L0 compaction.
- A tried L0 decoded-block `OnceLock` cache removed global cache hits but
  regressed 16/32 threads, so table-local decoded-block pinning remains a
  rejected path.
- Remote CI still has to run after push.

## Evidence

- Rust skill, performance skill, zero-cost skill, SPEC-AGENTS context, and
  current roadmap files were read before review.
- Review found that pinning only the table version was not enough for
  `BucketReader`: a flush after reader creation could move records from the
  memtable into a newer table version. `BucketReader` now captures active and
  immutable memtable sources together with the table version.
- `persistent_bucket_reader_keeps_memtable_source_after_flush` covers that
  regression.
- Verification passed: `cargo test --all-targets --all-features`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --all --check`, `git diff --check`, and the forbidden-term scan.
- Benchmark evidence in `.phrase/evidence.md` records the value-handle path,
  rejected decoded-block pinning experiment, block-cache counter sharding, and
  overlapping-L0 source fix.

## Next Recommendation

- Commit this performance/correctness slice, then run the broad benchmark suite
  before making broader tuning decisions.
