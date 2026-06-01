# Release Packaging

This document records the local release checklist for Trine KV crate packages.

## Versioning

Trine KV crate versions use Semantic Versioning:

- `MAJOR` changes for incompatible public API or storage-contract changes once
  the crate reaches `1.0.0`.
- `MINOR` changes for compatible public API additions.
- `PATCH` changes for compatible fixes, documentation updates, and packaging
  corrections.

Before `1.0.0`, the crate still uses SemVer-formatted versions. Breaking public
API or storage-contract changes should increment the minor version, and patch
releases should stay compatible with the same minor line.

The current crate release candidate is `0.1.0`. The v1 engine protocol remains
documented separately in `.phrase/protocol/trine-kv-v1-spec.md`.

## Package Contents

The crate package should contain only files useful to crate consumers:

- `src/`
- `tests/`
- `examples/`
- `benches/`
- `docs/`
- `README.md`
- `CHANGELOG.md`
- license files
- Cargo manifest and lockfile

Agent workflow files, local skill files, and repository-only notes are not part
of the crate package.

## Pre-Publish Gate

Run this gate before tagging or publishing:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo check --target wasm32-unknown-unknown --lib
cargo check --target wasm32-wasip1 --lib
cargo clippy --target wasm32-unknown-unknown --lib -- -D warnings
cargo run --example quickstart
cargo run --example user_store
cargo run --example event_index
cargo package --list
cargo package --locked
cargo publish --dry-run --locked
git diff --check
```

For performance-sensitive changes, also run:

```text
cargo bench --bench v1_bench
```

The package list should not include `.github/`, `.phrase/`, `.rust-skills/`,
`.claude/`, or other repository-only workflow directories.

## CI Verification

`.github/workflows/ci.yml` runs the release verification gate on pushes to
`main`, pull requests, and manual dispatch:

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo check --target wasm32-unknown-unknown --lib`
- `cargo check --target wasm32-wasip1 --lib`
- `cargo clippy --target wasm32-unknown-unknown --lib -- -D warnings`
- `cargo run --example quickstart`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo package --list` with a package-content guard
- `cargo package --locked`

The package-content guard fails if repository-only workflow directories such as
`.github/`, `.phrase/`, `.rust-skills/`, or `.claude/` enter the crate package.

## Publishing Workflow

`.github/workflows/publish.yml` is a manual workflow. It requires:

- a `version` input matching `Cargo.toml`;
- a matching `CHANGELOG.md` entry;
- a `mode` input set to either `dry-run` or `publish`;
- the `CARGO_REGISTRY_TOKEN` repository or environment secret;
- the `crates-io` environment when environment protection is desired.

The workflow always runs the full verification gate and `cargo publish
--dry-run --locked`. It runs `cargo publish --locked` only when `mode` is
`publish`.

Recommended release flow:

1. Update `Cargo.toml` and `CHANGELOG.md`.
2. Let CI pass on the release branch.
3. Trigger `Publish` with `mode=dry-run`.
4. Create and push the release tag after review.
5. Trigger `Publish` with `mode=publish`.
