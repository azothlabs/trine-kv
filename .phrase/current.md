# Current Phase

## Status

Complete

## Goal

Fix the CI regressions exposed after the cross-platform platform-io gate landed.

## Scope

- `cargo clippy --all-targets --all-features -- -D warnings` failures from
  long platform-io test functions.
- Windows `platform_io` example failure caused by cleanup returning
  `PermissionDenied` after the checked database path had already completed.
- Verification for the same commands used by CI where they can run locally.

## Out Of Scope

- New backend architecture.
- Storage format changes.
- Publishing, tagging, pushing, or PR creation.

## Acceptance Gate

- No `too_many_lines` failures under strict all-target clippy.
- `platform_io` example cleanup is best-effort after the database path has
  completed, so Windows file-handle release timing does not fail the example.
- Local Windows target checks still pass.
- Platform-io examples and focused tests still pass.

## Active Task Slice

```text
task758 [x] goal:fix all-target clippy too_many_lines | scope:src/io.rs src/storage.rs tests/async_api.rs | verify:cargo clippy --all-targets --all-features -- -D warnings
task759 [x] goal:make example cleanup tolerate Windows handle timing | scope:examples/platform_io.rs | verify:platform_io examples
task760 [x] goal:record CI regression evidence | scope:.phrase | verify:diff review
```

## Evidence

- The large platform backend matrix test is now operation-row driven through
  helper assertions instead of one monolithic per-platform function.
- The Linux platform-io flush test and native-file management test now keep
  operation flow and counter-class assertions in separate helpers.
- `examples/platform_io.rs` still resets any old directory before opening, but
  the final cleanup is best-effort because Windows can temporarily deny
  directory removal after file-heavy examples.

## Known Residuals

- Windows runtime execution must be confirmed by GitHub Actions because this
  workspace is macOS.
- BSD/Solaris and browser runtime diagnostics remain outside this workspace.

## Next Recommendation

- Commit the CI regression fix and rerun CI.
