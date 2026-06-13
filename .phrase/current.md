# Current Phase

## Status

In Progress

## Goal

Prepare the `0.4.0` release metadata for the completed platform I/O work and
create the Git tag used by the GitHub release flow.

## Scope

- Cargo crate version and lockfile version.
- User-facing version references in README and docs.
- Changelog entry for `0.4.0`.
- Release checklist target version.
- Local packaging and dry-run publishing verification.
- Local `v0.4.0` tag after the release commit.

## Out Of Scope

- New platform I/O behavior.
- Storage format changes.
- Actual crates.io publish from this local workspace.
- Pushing commits or tags unless explicitly requested.
- Creating a GitHub release page beyond the tag.

## Acceptance Gate

- `Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`, and release docs agree on
  `0.4.0`.
- README, usage docs, and platform I/O docs use the `0.4` dependency line.
- Cargo package contents exclude repository-only workflow files.
- `cargo publish --dry-run --locked` passes.
- `git diff --check` and formatting checks pass.
- A local `v0.4.0` tag points at the release metadata commit.

## Active Task Slice

```text
task770 [ ] goal:align release metadata to 0.4.0 | scope:Cargo.toml Cargo.lock CHANGELOG.md README.md docs | verify:rg version audit
task771 [ ] goal:verify package and dry-run publish | scope:cargo package/publish | verify:cargo package --locked && cargo publish --dry-run --locked
task772 [ ] goal:create release commit and v0.4.0 tag | scope:git metadata | verify:git status && git show v0.4.0
```

## Evidence

- GitHub Actions has passed after the Windows directory-sync permission fix,
  according to the user.
- The release is a pre-`1.0` minor bump because platform I/O adds a meaningful
  feature surface and cross-platform async behavior.

## Known Residuals

- Actual crates.io publish should happen through the guarded manual workflow or
  with an available local registry token.
- Push remains pending until the user asks for it.

## Next Recommendation

- Finish local release verification, commit the metadata, and create the local
  `v0.4.0` tag.
