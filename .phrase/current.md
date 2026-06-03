# Current Phase

## Status

Complete

## Goal

Prepare the compatible `0.2.0` minor release.

## Scope

- Version metadata for the crate package.
- Changelog and release checklist target version.
- Package lockfile consistency.
- Local release-prep verification.

## Out Of Scope

- Public API, storage format, recovery contract, and runtime behavior changes.
- Publishing to crates.io.
- Creating or pushing a release tag.

## Acceptance Gate

- `Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`, and `docs/release.md` agree on
  `0.2.0`.
- Release notes describe the compatible `get_many` API addition and performance
  fixes without claiming storage-format changes.
- Package verification passes locally, or any external-network blocker is
  recorded.
- Formatting, diff checks, and release-surface scans pass.

## Active Task Slice

```text
task613 [x] goal:bump minor release metadata | scope:Cargo.toml Cargo.lock docs/release.md | verify:version scan
task614 [x] goal:write 0.2.0 changelog | scope:CHANGELOG.md | verify:changelog scan
task615 [x] goal:run local release-prep gate | scope:package/release surface | verify:cargo package --allow-dirty --locked --offline
```

## Evidence

- Recent compatible performance commits:
  - `db8e116 Reduce prefix scan metadata rechecks`
  - `b2ca66e Reduce cold table open reads`
  - `b8f0b3f Reuse cold reopen directory listing`
  - `e423bc6 Add batched point reads`
  - `a66cc75 Optimize get_many internal batching`
  - `4a1db01 Reuse clean WAL proof in async native open`
  - `3219a11 Skip clean WAL reads on read-only open`
- `get_many` is a compatible public API addition, so the correct release target
  is `0.2.0` rather than `0.1.2`.

## Known Residuals

- Publish workflow still needs CI, a release-prep commit, and the final manual
  crates.io publish action.

## Next Recommendation

- Commit the `0.2.0` release-prep changes, then run CI and the manual publish
  workflow for `0.2.0`.
