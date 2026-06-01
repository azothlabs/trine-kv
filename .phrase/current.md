# Current Phase

## Status

Complete

## Goal

Add a feature-gated platform I/O runtime path under Trine's `io` boundary so
native-file reads and WAL append/persist can use OS async file I/O through
`compio` when explicitly selected.

## Scope

- Add optional `platform-io` Cargo feature using the MSRV-compatible `compio`
  release line.
- Add `RuntimeOptions::platform_io()` and platform async I/O runtime
  capabilities.
- Add `PlatformIoDriver` under `src/io.rs` with completion delivery through the
  existing `IoCompletion` type.
- Route native-file length, owned random reads, append, and persist through the
  platform driver when `RuntimeMode::PlatformIo` is selected.
- Keep the default runtime on the existing native-thread blocking adapter.
- Expose platform I/O task counts in stats and keep blocking-adapter reporting
  honest.

## Out Of Scope

- Making platform I/O the default runtime mode.
- Rewriting manifest publish, directory operations, object listing, writer
  lease acquisition, or recovery scanning to platform I/O in this slice.
- Changing public storage format, WAL, MVCC, table, manifest, compaction, or
  recovery semantics.

## Acceptance Gate

- Default builds reject `RuntimeOptions::platform_io()` unless `platform-io` is
  enabled.
- `platform-io` builds can compile and run the compio-backed native-file read,
  append, and persist path.
- Native-file capabilities report `PlatformAsyncIo` only when the platform
  driver is selected.
- Existing blocking-adapter and inline runtime behavior remains covered.
- Formatting, clippy, full tests, `git diff --check`, and forbidden-term scan
  pass.

## Active Task Slice

```text
task457 [x] goal:add platform-io feature and runtime mode | scope:Cargo.toml src/runtime.rs | verify:cargo check --features platform-io
task458 [x] goal:add compio-backed platform I/O driver | scope:src/io.rs | verify:cargo check --features platform-io
task459 [x] goal:route native-file read append persist through platform driver | scope:src/storage.rs | verify:platform storage test
task460 [x] goal:surface platform I/O stats | scope:src/stats.rs src/db.rs | verify:storage stats test
task461 [x] goal:update protocol and evidence | scope:.phrase docs | verify:manual
task462 [x] goal:run final verification | scope:repo | verify:full gate
```

## Known Blockers

- Manifest publish, directory operations, object listing, writer lease
  acquisition, recovery scanning, and some whole-object helpers still use the
  bounded blocking adapter on native files.
- The platform driver is opt-in because it adds a native dependency and is not
  suitable for WASM targets.

## Evidence

- `compio` 0.18 did not satisfy the current Rust 1.85 verification gate; the
  implementation uses `compio` 0.14 with `runtime` and no default features.
- Focused verification passed: `cargo check --features platform-io`,
  `cargo test runtime --lib`, `cargo test storage --lib`, and
  `cargo test platform_io_native_file_read_and_append_use_platform_driver --lib
  --features platform-io`.
- Final verification passed: `cargo clippy --all-targets --all-features -- -D
  warnings`, `cargo test --all-targets --all-features`, `cargo fmt --check`,
  `git diff --check`, forbidden-term scan, and project-name scan.

## Next Recommendation

- Continue with a storage-operation coverage phase that moves manifest publish,
  object writes, WAL rewrite, object reads, and supported metadata operations
  below the same `io` driver boundary where the platform crate can support them.
