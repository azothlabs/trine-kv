# Current Phase

## Status

Complete

## Goal

Automate release verification and add a guarded manual crates.io publishing
workflow.

## Entry Condition

- Phase 8 integration examples is complete.
- User asked to finish CI/release verification and publishing workflow.

## Scope

- GitHub Actions workflow for regular CI verification.
- GitHub Actions workflow for manual dry-run/publish to crates.io.
- Release docs that explain required secrets, version checks, and workflow
  order.
- Local validation of the Cargo commands that can run outside GitHub Actions.

## Out Of Scope

- Actually publishing the crate.
- Creating release tags.
- Adding non-Rust package targets.
- Changing v1 storage contracts or public APIs.

## Acceptance Gate

- CI workflow contains formatting, clippy, tests, examples, package content
  guard, and package verification.
- Publish workflow is manual, verifies the requested SemVer version against
  `Cargo.toml` and `CHANGELOG.md`, runs a dry run first, and only publishes
  when `mode=publish`.
- Release docs describe CI and publish workflow usage.
- Local verification passes for `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, examples, package list, package
  verification, publish dry-run, and `git diff --check`.

## Active Task Slice

```text
task044 [x] goal:CI/release verification and guarded publishing workflow are defined and documented | scope:.github/workflows,docs/release.md,.phrase | verify:local release gate + workflow YAML syntax parse
```

## Known Blockers

- GitHub Actions cannot be executed locally in this environment; remote CI must
  run after push.
- Real `cargo publish` needs a configured `CARGO_REGISTRY_TOKEN` secret and an
  explicit manual workflow run with `mode=publish`.

## Evidence To Record

- Local command verification.
- Workflow syntax sanity check.
- Publishing workflow safety boundaries.

## Next Recommendation

- When ready to ship, configure the `crates-io` environment and
  `CARGO_REGISTRY_TOKEN`, then run the `Publish` workflow first with
  `mode=dry-run`.
