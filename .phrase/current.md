# Current Phase

## Status

Complete

## Goal

Prepare the crate for a publishable release candidate after async host storage
support landed, with release-facing docs, CI, package gates, and evidence
matching the implemented behavior.

## Scope

- Phase 129: pre-release polish and verification.
- Release-facing documentation: README, usage, durability, changelog, and
  release checklist.
- CI/publish verification gates.
- Package content and local release checks.

## Out Of Scope

- Changing public API, storage formats, WAL, manifest, table, MVCC,
  transaction, compaction, or browser storage behavior.
- Adding an in-browser persistence fixture.
- Publishing to crates.io or creating a release tag.
- Performance tuning unless release verification exposes a blocking regression.

## Acceptance Gate

- README, usage docs, durability notes, release checklist, and changelog state
  the current native, WASI, and browser persistence boundaries honestly.
- CI and publish workflows check native release gates plus WASI/browser target
  compilation for the storage host boundary.
- `cargo package --list` excludes repository-only workflow files and includes
  the intended crate consumer files.
- Release verification passes or any blocker is recorded with a clear next
  action.

## Active Task Slice

```text
task543 [x] goal:update release-facing docs for async host storage status | scope:README.md docs CHANGELOG.md | verify:docs diff
task544 [x] goal:add WASI/browser target checks to release gates | scope:docs/release.md .github/workflows | verify:workflow diff and local target checks
task545 [x] goal:run pre-release verification gate | scope:cargo package/tests/examples/scans | verify:command results recorded in evidence
```

## Known Residuals

- The package and publish dry-run checks were run with `--allow-dirty` because
  this phase's release-polish files are still uncommitted. After commit, rerun
  the standard release commands without `--allow-dirty`.
- Browser runtime persistence is still compile-verified in this repository; an
  in-browser fixture remains a follow-up phase, not a release-polish blocker.
- Browser write preflight cannot await maintenance; under pressure it returns
  `RuntimeBusy` until the caller runs async maintenance.

## Evidence

- Phase 128 completed browser writable `Db::open_async`, async WAL-backed
  writes, async named bucket manifest publish, async flush/compaction,
  budgeted maintenance, WAL rewrite, table/blob cleanup, and blob GC through
  browser storage traits.
- Phase 128 kept synchronous browser mutation and synchronous maintenance APIs
  rejected with typed errors.
- Phase 129 updated release-facing docs and changelog for async host storage,
  added WASI/browser target checks to CI and publish workflows, verified native
  checks, WASI/browser checks, examples, package contents, package verification,
  and crates.io dry-run.

## Next Recommendation

- Commit the pre-release polish, rerun the standard package commands without
  `--allow-dirty`, then decide whether to add the in-browser persistence
  fixture before tagging or treat it as the first post-candidate hardening
  phase.
