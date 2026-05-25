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
cargo clippy
cargo test
cargo run --example quickstart
cargo run --example user_store
cargo run --example event_index
cargo package --list
cargo package
git diff --check
```

For performance-sensitive changes, also run:

```text
cargo bench --bench v1_bench
```

The package list should not include `.phrase/`, `.rust-skills/`, `.claude/`, or
other local workflow directories.
