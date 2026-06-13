# Current Phase

## Status

Complete

## Goal

Close the remaining cross-platform verification gap for platform-io by adding
Windows CI coverage and running Linux runtime validation in Docker.

## Scope

- GitHub Actions CI for Windows platform-io feature modes.
- Ubuntu CI example coverage for `platform_io`.
- Linux Docker runtime validation for `platform-io` and
  `platform-io-native`.
- Linux-only platform_io test assertions after the feature split.

## Out Of Scope

- New backend architecture.
- Storage format changes.
- Publishing, tagging, pushing, or PR creation.

## Acceptance Gate

- Windows CI checks `platform-io` and `platform-io-native` tests.
- Windows CI runs `examples/platform_io.rs` with both feature modes.
- Linux Docker runs `platform_io` examples and tests with both feature modes.
- Linux tests distinguish `ThreadPoolManagedAsync` under `platform-io` from
  `TruePlatformAsync` under `platform-io-native`.
- Local cross-target Windows checks, formatting, clippy, and diff checks pass.

## Active Task Slice

```text
task754 [x] goal:add Windows platform-io CI gate | scope:.github/workflows/ci.yml | verify:windows target checks
task755 [x] goal:run Linux platform-io runtime gate | scope:docker rust:1.85-bookworm | verify:examples/tests with both features
task756 [x] goal:update stale Linux-only platform_io assertions | scope:tests/async_api.rs | verify:docker gate
task757 [x] goal:record evidence | scope:.phrase | verify:diff review
```

## Evidence

- CI now has a `windows-platform-io` job that checks, tests, and runs the
  `platform_io` example with both `platform-io` and `platform-io-native`.
- Ubuntu CI example coverage now includes `platform_io` without features,
  with `platform-io`, and with `platform-io-native`.
- Linux Docker initially exposed stale Linux-only tests that expected
  `platform-io` to increment true-native counters. After the feature split,
  baseline `platform-io` should increment thread-pool managed counters, while
  `platform-io-native` should increment true native counters on Linux.
- Linux Docker runtime gate passed after updating those assertions.

## Known Residuals

- Windows runtime execution will happen in GitHub Actions after this commit;
  local validation from macOS is limited to Windows target checks.
- BSD/Solaris and browser runtime diagnostics remain outside this workspace.

## Next Recommendation

- Commit the CI/runtime verification update.
