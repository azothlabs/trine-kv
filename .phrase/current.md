# Current Phase

## Status

Complete

## Goal

Close the policy gaps from Phase 38 by making blob Level Merge automatic by
default and letting blob GC rewrite multiple stale candidates in one maintenance
publish.

## Entry Condition

- Phase 38 completed the first pass of Level Merge, value-lazy iteration, GC
  throughput tightening, and recovery fault injection.
- User clarified that Level Merge should use an automatic strategy and blob GC
  should batch multiple candidates.

## Scope

- Replace the boolean Level Merge option with `BlobLevelMergePolicy`.
- Default Level Merge to `Auto`.
- Make `Auto` rewrite retained blob values when compaction output would keep
  references to multiple blob files or leave stale input blob refs behind.
- Preserve `Disabled` and `Always` for tuning and benchmarks.
- Bump manifest encoding to v7 and decode v5/v6 manifests into the new policy.
- Batch all blob GC candidates that pass the discard threshold into one rewrite
  plan and one manifest publish.
- Update tests, benchmarks, protocol, usage docs, roadmap, and evidence.

## Out Of Scope

- New knobs for maximum GC batch size.
- Workload-adaptive policy learned from long-running telemetry.
- Changing the blob file format.

## Acceptance Gate

- Default bucket options use `BlobLevelMergePolicy::Auto`.
- Auto Level Merge rewrites retained large values without requiring users to
  enable a boolean option.
- `Disabled` keeps old blob references for tests and GC-only workloads.
- GC rewrites multiple stale candidates in one pass and records one run.
- Manifest v5/v6 compatibility tests pass.
- Benchmark rows continue to distinguish lazy range, GC rewrite, and Level
  Merge.
- Full local Rust verification passes.

## Active Task Slice

```text
task131 [x] goal:replace Level Merge boolean with policy | scope:options manifest docs tests | verify:manifest_decode_* + persistent_manifest_keeps_bucket_options_across_reopen
task132 [x] goal:auto-rewrite retained blob refs | scope:db compaction tests | verify:persistent_blob_level_merge_auto_rewrites_retained_blob_indexes
task133 [x] goal:batch blob GC candidates | scope:db tests benches | verify:persistent_blob_gc_batches_multiple_stale_candidates
task134 [x] goal:update docs/evidence and full gate | scope:.phrase docs README | verify:full Rust verification
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.
- GC still writes one output blob file per batch. A future phase can add a
  maximum output size if benchmarks show large batches causing stalls.

## Evidence

- Rust skill and SPEC-AGENTS context were read before implementation.
- `BucketOptions` now stores `blob_level_merge_policy`; default is `Auto`.
- Manifest v7 writes the policy enum. v6 boolean manifests decode `true` as
  `Always` and `false` as `Auto`; v5 and older manifests default to `Auto`.
- Auto Level Merge rewrites retained blob refs when output refs span multiple
  blob files or when compaction drops part of an input blob file's refs.
- `Disabled` prevents Level Merge and lets GC-only tests exercise stale blob
  cleanup.
- Blob GC candidate selection returns all candidates that pass the configured
  threshold, and the rewrite plan handles each table once even if it references
  multiple candidates.
- Benchmark rows from `cargo bench --bench v1_bench`:
  - `blob range scan`: 12929 us for 32 scans.
  - `blob range lazy keys`: 173 us for 32 scans.
  - `blob GC rewrite`: 126649 us.
  - `blob level merge`: 123261 us.
- Targeted tests passed:
  - `cargo test --test persistent_wal --all-features`
  - `cargo test manifest_decode --all-features`
  - `cargo test blob::tests --all-features`
- Full local gate passed:
  - `cargo test --all-targets --all-features`
  - `cargo clippy --all-targets --all-features`
  - `cargo fmt --all --check`
  - `git diff --check`
  - forbidden-term scan over `.phrase`, `src`, `tests`, `benches`,
    `examples`, `docs`, and `README.md`

## Next Recommendation

- Commit Phase 39. After CI, use workload benchmarks to decide whether GC
  batching needs a configurable byte limit.
