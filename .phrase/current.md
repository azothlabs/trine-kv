# Current Phase

## Status

Complete

## Goal

Fix the remaining CI regressions exposed after the first platform-io CI
regression patch.

## Scope

- Linux native backend matrix assertion that accidentally expected directory
  listing to be true platform async.
- Runtime-enabled native-file read test that depended on exact helper-worker
  task accounting across feature/target combinations.
- Windows `platform_io` example path reuse after a denied cleanup.
- Verification for the same commands used by CI where they can run locally.

## Out Of Scope

- New backend architecture.
- Storage format changes.
- Publishing, tagging, pushing, or PR creation.

## Acceptance Gate

- Linux native matrix expects `ThreadPoolManagedAsync` for directory listing
  and true platform async for the remaining Linux native rows.
- Runtime-enabled native-file read test proves the backend read task completed
  through the blocking adapter without depending on whether the worker holder
  is counted.
- `platform_io` example uses a per-run unique temp directory so stale Windows
  directories from previous runs do not affect the next run.
- Local Windows target checks, Linux Docker focused tests, strict clippy,
  examples, formatting, and diff checks pass.

## Active Task Slice

```text
task761 [x] goal:fix Linux native matrix directory listing class | scope:src/io.rs | verify:docker lib test
task762 [x] goal:relax blocking-adapter stats test to behavior boundary | scope:src/storage.rs | verify:local/docker lib tests
task763 [x] goal:avoid Windows example stale temp path reuse | scope:examples/platform_io.rs | verify:examples/windows target checks
task764 [x] goal:record second CI regression evidence | scope:.phrase | verify:diff review
```

## Evidence

- Linux `platform-io-native` matrix now checks directory listing as
  `ThreadPoolManagedAsync` and the rest of the audited Linux native rows as
  `TruePlatformAsync`.
- The runtime-enabled native-file read test still asserts one backend
  blocking-adapter read task, while allowing submitted/completed runtime task
  counters to include or exclude the helper worker depending on stats baseline.
- `examples/platform_io.rs` now includes a timestamp nonce in its temp
  directory name.

## Known Residuals

- Windows runtime execution must be confirmed by GitHub Actions because this
  workspace is macOS.
- BSD/Solaris and browser runtime diagnostics remain outside this workspace.

## Next Recommendation

- Commit the CI regression fix and rerun CI.
