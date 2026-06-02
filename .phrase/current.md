# Current Phase

## Status

Complete

## Goal

Refresh the `0.1` benchmark baseline before release and align the release-facing
baseline filename with crate SemVer rather than the internal v1 engine label.

## Scope

- `cargo bench --bench v1_bench` output for the current release candidate.
- Baseline benchmark document under `docs/benchmarks/`.
- README benchmark baseline link.
- Decision evidence for the refreshed baseline.

## Out Of Scope

- Publishing, tagging, or changing crate version metadata.
- Storage format, MVCC, WAL, manifest, table, blob, compaction, transaction, or
  recovery semantic changes.
- Benchmark harness behavior changes.
- Renaming older tuning documents unless they are release-facing baseline links.

## Acceptance Gate

- Fresh benchmark output is recorded from `cargo bench --bench v1_bench`.
- The release baseline file is named with the `0.1` release line.
- README links to the refreshed baseline.
- `cargo fmt --check`, release-facing link/name scans, and `git diff --check`
  pass.

## Active Task Slice

```text
task567 [x] goal:refresh 0.1 benchmark baseline | scope:benches/v1_bench.rs docs/benchmarks | verify:cargo bench --bench v1_bench
task568 [x] goal:rename release baseline file and update links | scope:docs/benchmarks README.md | verify:rg benchmark link/name scan
task569 [x] goal:record benchmark evidence | scope:.phrase/current.md .phrase/evidence.md .phrase/roadmap.md | verify:git diff --check
```

## Known Residuals

- Benchmark numbers are local machine measurements and are suitable as a
  repository baseline, not a cross-machine performance guarantee.

## Evidence

- `cargo bench --bench v1_bench` passed on 2026-06-02 and produced the
  refreshed local baseline.
- The release-facing baseline moved from `docs/benchmarks/v1-baseline.md` to
  `docs/benchmarks/0.1-baseline.md`.
- README now links the `v0.1.0 benchmark baseline` text to
  `docs/benchmarks/0.1-baseline.md`.

## Next Recommendation

- Continue with final release-candidate claim or tag/publish decisions.
