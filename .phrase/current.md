# Current Phase

## Status

Complete

## Goal

Make platform-io's completed async abstraction understandable and verifiable
from user-facing docs, crate docs, and a runnable example.

## Scope

- README feature selection and verification path.
- Usage guide platform-io section.
- Dedicated `docs/platform-io.md` feature/runtime/matrix guide.
- Crate-level Rustdoc for platform I/O features.
- Runnable `platform_io` example that checks driver and operation counters.
- Local macOS native-first verification for `platform-io-native`.

## Out Of Scope

- New storage format changes.
- New OS backend architecture.
- Publishing, tagging, pushing, or PR creation.

## Acceptance Gate

- README explains the difference between no feature, `platform-io`, and
  `platform-io-native`.
- Docs state that enabling a feature only makes the driver available; callers
  still select it with `RuntimeOptions::platform_io()`.
- Docs explain operation-level classes:
  `TruePlatformAsync`, `PlatformNativeAsyncButPartial`,
  `ThreadPoolManagedAsync`, `BlockingFallback`, and `Unsupported`.
- A checked example validates both `platform-io` and `platform-io-native`.
- Public Rustdoc passes with warnings denied and doctests pass.
- Cross-target feature checks keep browser-style WASM and Windows feature
  shapes honest.

## Active Task Slice

```text
task750 [x] goal:add platform-io runnable example | scope:examples/platform_io.rs | verify:cargo run example with both features
task751 [x] goal:document feature/runtime selection | scope:README.md docs/usage.md docs/platform-io.md src/lib.rs | verify:rustdoc/doctest
task752 [x] goal:fix local macOS native-first verification gap | scope:src/io/platform_backend/apple_dispatch.rs src/io/platform_backend.rs | verify:platform-io-native example/tests
task753 [x] goal:record evidence and roadmap | scope:.phrase | verify:diff review
```

## Evidence

- `platform_io` example writes, reads, and flushes a persistent database. It
  also selects `RuntimeOptions::platform_io()` and asserts platform operation
  counters when the feature/target supports the driver.
- README, usage docs, dedicated platform-io docs, and crate Rustdoc now explain
  both the Cargo feature choice and the runtime selection step.
- Local macOS `platform-io-native` verification exposed a DispatchIO descriptor
  and file-creation reliability gap. The native backend now keeps DispatchIO as
  the preferred path, but retries create/write and descriptor-unavailable sync
  cases through safe blocking file operations inside the same platform-io
  operation.

## Known Residuals

- Real Windows, Linux, BSD/Solaris, and browser runtime diagnostics remain
  outside this macOS workspace unless run through target checks or Docker.
- Native backend rows that are not true native async remain intentionally
  classified as partial or thread-pool managed in the public matrix.

## Next Recommendation

- Commit the verification, documentation, example, and macOS native-first
  reliability fix.
