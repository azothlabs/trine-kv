# Current Phase

## Status

Complete

## Goal

Add benchmark coverage for the Titan-like large-value path, then harden the
measured blob point-read hotspot without changing visible KV semantics.

## Entry Condition

- Phase 36 completed snapshot-safe blob GC correctness.
- Evidence says blob GC throughput lacks a dedicated benchmark baseline.
- Code audit shows `BlobIndex` point reads can still decode a whole blob file
  instead of reading the indexed record by offset.

## Scope

- Add benchmark rows for large-value point reads, range scans, and GC rewrite.
- Record a pre-tuning benchmark baseline.
- Change `BlobIndex` reads to seek to the indexed blob record and verify only
  that record.
- Keep full blob-file decode for recovery validation and GC scanning.
- Record post-tuning benchmark evidence and update docs.

## Out Of Scope

- Titan Level Merge policy and range-locality optimization.
- WAL-time value separation.
- Hole punching or in-place blob-file rewriting.
- A separate in-memory blob store.
- Public API changes.

## Acceptance Gate

- Benchmark harness reports large-value point read, range scan, and GC rewrite
  rows.
- Pre/post benchmark evidence exists for the selected read-path change.
- `BlobIndex` reads use the stored offset and still verify record checksum,
  value checksum, compression id, index metadata, and expected internal key.
- Recovery validation still decodes full blob files.
- Existing blob correctness and full Rust verification pass.
- Rust verification passes.

## Active Task Slice

```text
task123 [x] goal:add large-value benchmark rows | scope:benches/v1_bench.rs docs/benchmarks | verify:cargo bench --bench v1_bench
task124 [x] goal:read BlobIndex by offset | scope:src/blob.rs tests/persistent_wal.rs | verify:blob read tests + benchmark delta
task125 [x] goal:run full verification and record evidence | scope:repo .phrase docs | verify:cargo test + clippy + diff checks
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.
- Benchmarks are local and noisy; use them to choose direction, not as a
  permanent performance guarantee.

## Evidence

- Phase 36 verification passed before commit `f666492`.
- Current audit found the point-read blob path decodes the whole blob file even
  though `BlobIndex` stores a record offset.
- Pre-tuning benchmark after adding large-value rows:
  - `blob point read`: 1140632 us for 256 reads.
  - `blob range scan`: 1128654 us for 32 scans.
  - `blob GC rewrite`: 148488 us.
- Post-tuning benchmark:
  - `blob point read`: 13976 us for 256 reads.
  - `blob range scan`: 13719 us for 32 scans.
  - `blob GC rewrite`: 153518 us.
- `cargo test blob::tests --all-features` passes.
- `cargo test --all-targets --all-features` passes.
- `cargo clippy --all-targets --all-features` passes.
- `cargo fmt --all --check` passes.
- `git diff --check` passes.
- Forbidden-term scan over `.phrase`, `src`, `tests`, `benches`, `examples`,
  `docs`, and `README.md` passes.

## Next Recommendation

- Commit Phase 37 once the user wants a checkpoint. The next meaningful work is
  a new phase, not a Phase 37 correctness tail.
