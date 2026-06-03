# Current Phase

## Status

Complete

## Goal

Measure and reduce the remaining cold persistent reopen overhead around
manifest and open operations, without changing table contents, recovery rules,
or public API behavior.

## Scope

- Cold-read benchmark diagnostics for manifest/open request counts and latency
  totals.
- Native persistent reopen path only, selected by measured cold-read evidence.
- Documentation of before/after benchmark evidence for the kept change.

## Out Of Scope

- Public API additions such as batched point reads.
- Storage format, MVCC, WAL, table, blob, compaction, transaction, recovery,
  browser persistence, or release metadata changes.
- Async/browser backend behavior changes unless required to preserve shared
  semantics.
- Large table data-block eager loading.

## Acceptance Gate

- Before-change evidence identifies the remaining cold-read storage request
  shape.
- The kept change reduces one measured cold reopen/open cost without changing
  recovery or persistence semantics.
- Focused storage/manifest/table and benchmark checks pass.
- Formatting, clippy, full tests, diff checks, and forbidden-term scans pass.

## Active Task Slice

```text
task583 [x] goal:record cold manifest/open baseline | scope:benches/v1_bench.rs docs/benchmarks | verify:cargo bench --bench v1_bench
task584 [x] goal:apply one measured reopen/open optimization | scope:src | verify:focused cargo test -q
task585 [x] goal:record before-after evidence | scope:docs/benchmarks .phrase | verify:cargo bench --bench v1_bench
task586 [x] goal:finish local verification | scope:full workspace | verify:cargo clippy/test/diff checks
```

## Known Residuals

- Phase 139 reduced positioned owned reads from 288 to 96 for 32 reopen/get
  operations.
- Phase 140 leaves writer-lease acquisition intact. The diagnostic run showed
  it is the largest fixed cost for writable cold reopen, but it is a safety
  boundary.
- Remaining cold-read cost includes writer-lease acquisition, WAL whole-object
  reads, current-manifest reads, table open, and one lazy data-block read per
  reopened database.

## Evidence

- Phase 139 release-profile cold table read after-change runs were 174933 us
  and 167430 us, with the same 96 positioned owned read requests.
- Phase 139 diagnostics recorded current-manifest read latency at 461 us and
  493 us across the two after-change release-profile runs.
- Phase 140 before-change discovery showed 96 directory file-list requests and
  64 object-list requests across 32 reopen/get operations.
- After directory snapshot reuse, the same diagnostic shape showed 32 directory
  file-list requests and zero object-list requests.
- The after-change release-profile `cold table read` row was 168137 us.
- Focused recovery, WAL, persistent, and benchmark harness checks passed.
- `cargo clippy --all-targets --all-features -- -D warnings` and
  `cargo test -q --all-targets --all-features` passed with output redirected
  to temporary files.

## Next Recommendation

- Commit the directory snapshot reuse, then choose whether to continue cold
  reopen work around writer-lease cost or switch to batched point reads.
