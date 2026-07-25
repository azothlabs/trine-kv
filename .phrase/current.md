# Current Phase

## Rust 1.95 toolchain contract

**Status:** Complete.

### Goal

- Make Rust 1.95 the real minimum compiler across package metadata, local
  development, CI, publishing, production evidence, release docs, and the
  dependency graph.
- Move to the `0.6` release line because an MSRV increase is a compatibility
  break for Rust 1.85 users.

### Implemented boundary

- Pin the repository toolchain and immutable GitHub Actions reference to Rust
  1.95.0.
- Remove the separate Rust 1.88 wasm-bindgen installation path.
- Upgrade native platform-I/O dependencies to compio 0.19 and remove the
  transitive unmaintained `paste` macro.
- Update active dependency examples, release policy, changelog, and drift
  checks.
- Run the full Rust 1.95 verification gate and record evidence.

### Discovery result

- Compio 0.19.1 requires both the `MaybeUninit` slice API and `cfg_select!`;
  Rust 1.95 is the first stable toolchain satisfying the full dependency graph.
- The official 0.19 dependency family replaces `paste` with maintained macros,
  so no private fork or compiler escape hatch is required.

### Acceptance gate

- Rust 1.95 native all-target/all-feature tests, strict Clippy, Rustdoc,
  doctests, eight examples, forced-exit recovery, mixed-load recovery, and six
  destructive fault tests pass.
- `wasm32-wasip1` strict compilation and seven persistence tests pass.
  Browser all-feature check/Clippy and 20 Chrome OPFS/Worker tests pass.
- Package content verification, `cargo package --locked`, and
  `cargo publish --dry-run --locked` pass for `trine-kv 0.6.0`.
- The freshly updated RustSec database reports zero vulnerabilities and zero
  warnings; `paste` is absent from the dependency graph.
- Seven script tests, documentation drift, formatting, and diff checks pass.

### Out of scope

- Rust beyond 1.95, private dependency forks, publishing, tagging, committing,
  pushing, storage-format changes, and new public features.
