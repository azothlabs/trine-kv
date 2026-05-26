# Current Phase

## Status

Complete

## Goal

Restore CI compatibility with the declared Rust 1.85 MSRV without raising the
crate's minimum supported compiler.

## Entry Condition

- Phase 12 benchmark-backed tuning is complete.
- Remote CI reports Rust 1.85 rejecting `const fn` bodies that call `Vec`
  methods not yet stable as const functions.

## Scope

- Keep `rust-version = "1.85"` and keep CI on Rust 1.85.
- Remove `const fn` from the functions that call `Vec::len` or
  `Vec::is_empty`.
- Verify the normal release gate still passes.
- Record the MSRV evidence delta.

## Out Of Scope

- Raising the crate MSRV.
- Changing runtime public API behavior.
- Changing storage formats, recovery, compaction, or durability behavior.
- Publishing the crate.

## Acceptance Gate

- The reported Rust 1.85 const-evaluation errors are removed without changing
  runtime behavior.
- Local verification passes for `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, examples, package list, package
  verification, publish dry-run, and `git diff --check`.

## Active Task Slice

```text
task048 [x] goal:Rust 1.85 CI no longer fails on unstable const Vec method calls | scope:src/blob.rs,src/compaction.rs,.phrase | verify:full local release gate
```

## Known Blockers

- GitHub Actions cannot be executed locally in this environment; remote CI must
  run after push.
- The local default toolchain may be newer than Rust 1.85, so the remote CI run
  remains the final proof for that exact compiler unless Rust 1.85 is available
  locally.

## Evidence To Record

- `ValueRef::len`, `ValueRef::is_empty`, and
  `CompactionTable::has_key_bounds` no longer use `const fn`.
- The full local release gate passed on Rust 1.87; remote CI remains the exact
  Rust 1.85 proof after push.

## Next Recommendation

- Push and let CI run on Rust 1.85. If it passes, continue with the guarded
  publish workflow dry-run before any real publish.
