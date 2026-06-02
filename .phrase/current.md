# Current Phase

## Status

Complete

## Goal

Prepare a `0.1.1` patch release that corrects crates.io metadata and README
installation guidance after the initial `0.1.0` publish.

## Scope

- `Cargo.toml` package metadata and patch version.
- `Cargo.lock` root package version alignment for locked publishing.
- README install section and release-facing version references.
- Changelog and release checklist wording for the patch release target.
- Decision evidence for the metadata patch.

## Out Of Scope

- Storage format, MVCC, WAL, manifest, SSTable, blob, compaction, transaction,
  recovery, or browser persistence behavior changes.
- Public API behavior changes.
- Benchmark harness or benchmark baseline changes.
- Publishing, tagging, or pushing unless the user requests it separately.

## Acceptance Gate

- Crate metadata includes the GitHub repository URL.
- README links the crates.io package page and shows `cargo add trine-kv` as the
  dependency installation path.
- README does not present `cargo install` as the normal path for this library
  crate.
- Package metadata and docs reflect the `0.1.1` patch release target.
- `cargo package --allow-dirty --locked`, `cargo fmt --check`, and
  `git diff --check` pass.

## Active Task Slice

```text
task570 [x] goal:add repository metadata | scope:Cargo.toml | verify:cargo package --allow-dirty --locked
task571 [x] goal:update install guidance | scope:README.md | verify:README install scan
task572 [x] goal:record patch release intent | scope:CHANGELOG.md docs/release.md .phrase | verify:git diff --check
```

## Known Residuals

- The already-published `0.1.0` crate page metadata cannot be corrected in
  place; the repository link appears on crates.io after publishing `0.1.1`.

## Evidence

- `0.1.0` was published successfully, but the crate metadata did not include a
  `repository` URL, so crates.io had no GitHub link to show.
- `Cargo.toml` now targets `0.1.1` and includes
  `repository = "https://github.com/azothlabs/trine-kv"`.
- README now links the crates.io package page and gives `cargo add trine-kv` as
  the application dependency path.

## Next Recommendation

- Commit the metadata patch, tag `v0.1.1` after CI passes, then publish
  `0.1.1`.
