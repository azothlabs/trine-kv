# Current Phase

## Status

Complete

## Goal

Make platform-io's feature shape match its final abstraction role:
`platform-io` provides a Trine-owned bounded thread-pool async backend, while
`platform-io-native` adds native async backends and falls back to the same
thread-pool backend for unsupported operation rows.

## Scope

- Cargo features:
  - `platform-io`: thread-pool baseline; no native async dependency stack.
  - `platform-io-threadpool`: explicit alias for the baseline.
  - `platform-io-native`: enables native backend crates plus the thread-pool
    baseline.
- Native-thread targets:
  - baseline rows report `ThreadPoolManagedAsync`;
  - native rows report `TruePlatformAsync` or `PlatformNativeAsyncButPartial`
    when native evidence exists;
  - native rows without native support route to the thread-pool backend.
- No-native-thread targets:
  - must not advertise `PlatformAsyncIo`;
  - must not report `ThreadPoolManagedAsync`;
  - `platform-io` and `platform-io-native` must compile without pulling the
    native-only dependency stack.
- Keep KV engine code unaware of OS/backend feature mechanics.

## Backend Boundary Receipt

- Trine operation names remain the acceptance rows, not OS API names.
- Owned internal surface: Cargo feature graph, `PlatformIoDriver` routing,
  bounded platform thread-pool backend, native backend cfg gates, runtime
  capability flags, per-operation stats, public usage docs, ADR/protocol, and
  target-family tests.
- Chosen thread-pool backend: Trine-owned fixed-size bounded worker pool using
  `crossbeam-channel`.
- Chosen native backend: existing compio/DispatchIO-backed path behind
  `platform-io-native`.
- Leak-check scope: no new OS-specific branching in KV engine code.
- Verification gate: baseline and native platform-io tests, Windows target
  checks, wasm checks for both feature shapes, rustdoc/doctest, clippy, fmt, and
  diff checks.

## Out Of Scope

- Making every native backend operation `TruePlatformAsync`.
- Replacing compio as the native backend in this phase.
- Engine revalidation, storage format changes, publishing, tagging, pushing, or
  PR creation.

## Acceptance Gate

- `platform-io` no longer pulls compio/native-only dependencies.
- `platform-io-native` keeps native priority and uses thread-pool completion for
  unsupported native operation rows.
- `platform-io` and `platform-io-native` both compile on `wasm32-unknown-unknown`
  without advertising native-thread platform async I/O.
- Native-thread baseline tests prove all operation rows can complete through
  `ThreadPoolManagedAsync`.
- Native feature tests prove existing native/partial rows still work.
- Phase evidence and docs explain the feature split and remaining limits.
- Full local gate passes and the phase is committed.

## Active Task Slice

```text
task740 [x] goal:split feature graph | scope:Cargo.toml | verify:wasm/native checks
task741 [x] goal:add bounded thread-pool platform backend | scope:src/io/platform_threadpool.rs src/io.rs | verify:platform_io_threadpool test
task742 [x] goal:route native rows to native and unsupported rows to threadpool | scope:src/io.rs src/io/platform_backend.rs | verify:platform-io-native tests
task743 [x] goal:update docs/protocol/evidence | scope:.phrase docs/usage.md | verify:rg stale feature semantics
task744 [x] goal:run validation and commit | scope:repo | verify:full gate plus git commit
```

## Evidence

- The baseline `platform-io` feature now checks without compio and uses
  Trine-owned thread-pool workers for native filesystem operations.
- `platform-io-native` keeps the existing native backend path and class matrix,
  but thread-pool rows are routed to the baseline backend rather than to the
  native worker.
- `platform-io` and `platform-io-native` both compile for
  `wasm32-unknown-unknown` without the previous `socket2` native dependency
  failure.

## Known Residuals

- Windows/macOS/BSD/Solaris complete operations still need future work before
  every row can become `TruePlatformAsync`.
- Real Windows, FreeBSD, illumos, Solaris, and browser runtime diagnostics are
  not run from this macOS workspace.

## Next Recommendation

- After this phase is committed, return to engine revalidation with a stable
  platform-io feature boundary.
