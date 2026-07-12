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

The current crate minor release line is `0.5.x`. The v1 engine protocol
remains documented separately in `.phrase/protocol/trine-kv-v1-spec.md`.

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
python3 scripts/check_docs_drift.py
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo check --target wasm32-unknown-unknown --lib
cargo check --target wasm32-wasip1 --lib
CARGO_TARGET_WASM32_WASIP1_RUNNER="wasmtime run --dir ." cargo test --target wasm32-wasip1 --lib wasi_persistent
cargo clippy --target wasm32-unknown-unknown --lib -- -D warnings
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner WASM_BINDGEN_TEST_ONLY_WEB=1 cargo test --target wasm32-unknown-unknown --test browser_persistent_wasm
cargo run --example quickstart
cargo run --example sync_quickstart
cargo run --example platform_io
cargo run --example platform_io --features platform-io
cargo run --example platform_io --features platform-io-native
cargo run --example read_versions
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

The publish workflow always runs the forced-process-exit recovery test and a
10,000-operation concurrent mixed-load test. Run the same evidence locally with:

```text
cargo test -q --test production_maturity forced_process_exit_recovery -- --ignored --nocapture --test-threads=1
cargo test -q --test production_maturity concurrent_mixed_load_soak_reopens_cleanly -- --ignored --nocapture --test-threads=1
cargo test -q destructive_ --lib -- --test-threads=1
```

Pull requests that touch production-sensitive paths also run the paired
performance and cross-platform maturity workflow in
`.github/workflows/production-evidence.yml`. Its maturity matrix covers Linux,
macOS, and Windows. A release is not production-evidence complete until that
workflow has passed for the release commit; the Ubuntu publish job does not
substitute for the cross-platform result.

The package list should not include `.github/`, `.phrase/`, `.rust-skills/`,
`.claude/`, or other repository-only workflow directories.

## CI Verification

`.github/workflows/ci.yml` runs the release verification gate on pushes to
`main`, pull requests, and manual dispatch:

- `cargo fmt --check`
- `python3 scripts/check_docs_drift.py`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo check --target wasm32-unknown-unknown --lib`
- `cargo check --target wasm32-wasip1 --lib`
- `cargo test --target wasm32-wasip1 --lib wasi_persistent` under `wasmtime run --dir .`
- `cargo clippy --target wasm32-unknown-unknown --lib -- -D warnings`
- `cargo test --target wasm32-unknown-unknown --test browser_persistent_wasm`
  under the browser test runner
- `cargo run --example quickstart`
- `cargo run --example sync_quickstart`
- `cargo run --example platform_io`
- `cargo run --example platform_io --features platform-io`
- `cargo run --example platform_io --features platform-io-native`
- `cargo run --example read_versions`
- `cargo run --example user_store`
- `cargo run --example event_index`
- `cargo package --list` with a package-content guard
- `cargo package --locked`

The `Windows Platform I/O` and `macOS Platform I/O` jobs additionally check,
test, and run both platform I/O feature modes on their named operating systems.
The separate production-evidence workflow supplies the broader Linux, macOS,
and Windows crash-recovery and mixed-load matrix.

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
--dry-run --locked`, plus forced-exit recovery, deterministic destructive, and
mixed-load evidence. It runs `cargo publish --locked` only when `mode` is
`publish`.

Recommended release flow:

1. Update `Cargo.toml` and `CHANGELOG.md`.
2. Let CI pass on the release branch.
3. Trigger `Publish` with `mode=dry-run`.
4. Create and push the release tag after review.
5. Trigger `Publish` with `mode=publish`.
