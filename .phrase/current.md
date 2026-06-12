# Current Phase

## Status

Complete

## Goal

Prepare release metadata for the read-version and manifest v9 work.

## Scope

- Cargo crate version and lockfile package version.
- `CHANGELOG.md` entry for the new release target.
- Release checklist wording for the current minor target.
- Evidence and roadmap state for the metadata phase.

## Out Of Scope

- Further Rust engine behavior changes.
- Publishing to crates.io.
- Tagging, pushing, or opening a PR.

## Acceptance Gate

- Version metadata agrees on the release target.
- Changelog records public API additions and the manifest v9 storage-contract
  change.
- Release docs name the current crate minor target.
- Formatting, package metadata checks, diff checks, and scans pass.

## Active Task Slice

```text
task629 [x] goal:bump crate metadata | scope:Cargo.toml Cargo.lock docs/release.md | verify:cargo metadata/check
task630 [x] goal:record changelog | scope:CHANGELOG.md | verify:doc review
task631 [x] goal:verify and commit | scope:package/diff/scans | verify:all pass
```

## Evidence

- Phase 146 advanced the manifest payload to v9 for durable checkpoint pins.
- Project release rules say pre-`1.0` storage-contract changes should increment
  the minor version.
- Phase 147 completed the non-breaking public boundary cleanup.

## Known Residuals

- Publishing workflow remains manual and out of scope for this phase.

## Next Recommendation

- Metadata is ready for review. Publishing, tagging, and pushing remain manual
  follow-up work.
