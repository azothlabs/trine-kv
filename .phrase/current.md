# Current Phase

## Status

Complete

## Goal

Fix the Windows platform-io permission failure as a general directory-sync
semantics issue, not as an example-only workaround.

## Scope

- Windows directory sync through native durability helpers.
- Windows directory sync through `platform-io` thread-pool backend.
- Windows directory sync through `platform-io-native` backend.
- `examples/platform_io.rs` error context for future Windows failures.
- Durability docs for Windows directory-sync limits.

## Out Of Scope

- New backend architecture.
- Storage format changes.
- Publishing, tagging, pushing, or PR creation.

## Acceptance Gate

- Windows directory-open or directory-flush `PermissionDenied` is treated as a
  best-effort directory-sync boundary, while file sync and rename still run.
- Platform-io baseline and native Windows backends use the same helper.
- The example reports which operation failed if another Windows permission
  issue appears.
- Local Windows target checks, strict clippy, examples, focused tests, and
  Linux Docker smoke pass.

## Active Task Slice

```text
task767 [x] goal:make Windows directory sync PermissionDenied best-effort | scope:src/durability.rs src/io/platform_threadpool.rs src/io/platform_backend.rs | verify:windows target checks
task768 [x] goal:add platform_io operation context | scope:examples/platform_io.rs | verify:examples
task769 [x] goal:update durability docs and evidence | scope:docs/durability.md .phrase | verify:diff review
```

## Evidence

- Windows directory sync now attempts backup-semantics directory handles, but
  accepts `PermissionDenied` from directory open/flush as the strongest
  available best-effort directory sync on that platform.
- The same Windows directory-sync helper is used by native durability,
  platform-io thread-pool, and platform-io-native code paths.
- `platform_io` now wraps open/write/read/flush/close errors with operation
  context.

## Known Residuals

- Windows runtime execution must be confirmed by GitHub Actions because this
  workspace is macOS.
- BSD/Solaris and browser runtime diagnostics remain outside this workspace.

## Next Recommendation

- Commit the Windows directory-sync permission fix and rerun CI.
