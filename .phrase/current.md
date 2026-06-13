# Current Phase

## Status

Complete

## Goal

Complete platform-io as Trine's cross-platform async abstraction for the current
backend slice by making managed thread-pool async a first-class platform-io
class instead of a vague fallback.

## Scope

- Rename the previous managed-fallback class to `ThreadPoolManagedAsync`
  everywhere the class describes platform-io-owned blocking work that completes
  asynchronously to the caller.
- Keep native async mechanics inside platform-io:
  - Linux keeps true native async rows where the selected backend supports the
    whole Trine operation.
  - Windows keeps IOCP-backed rows as partial until each complete operation is
    fully native async.
  - macOS keeps Apple `DispatchIO` data-path rows as partial until the complete
    operation is fully native async.
  - Native Unix targets without stronger audited support use platform-io's
    managed thread-pool path.
- Classify unsupported/no-native-thread targets as `Unsupported` instead of
  pretending they have managed thread-pool async.
- Update runtime capability, stats, docs, protocol, ADR, tests, and evidence to
  use operation-level truth.

## Backend Boundary Receipt

- Trine operation names are the acceptance rows, not OS function names.
- `PlatformAsyncIo` means the selected native platform driver can return
  asynchronous completions to the caller runtime through true native async,
  partial native async, or platform-io's managed thread-pool path.
- Exact operation health is reported by per-operation counters:
  `TruePlatformAsync`, `PlatformNativeAsyncButPartial`,
  `ThreadPoolManagedAsync`, `BlockingFallback`, or `Unsupported`.
- `BlockingFallback` is reserved for operations that cannot currently move to
  native async or platform-io's managed thread-pool path.
- Browser WASM and other no-native-thread targets must not report
  `ThreadPoolManagedAsync`.
- KV engine code must not gain OS-specific branching.

## Out Of Scope

- Making every Windows/macOS/BSD operation `TruePlatformAsync`.
- Engine revalidation, storage format changes, publishing, tagging, pushing, or
  PR creation.

## Acceptance Gate

- `ThreadPoolManagedAsync` replaces the old platform-managed fallback class in
  code, docs, protocol, ADR, and stats.
- Native directory listing rows are `ThreadPoolManagedAsync` when platform-io
  owns the blocking work.
- Unsupported/no-native-thread targets report `Unsupported`, not managed
  thread-pool async.
- Public stats expose true-or-partial native async counts separately from
  platform-io managed thread-pool counts and blocking fallback counts.
- Target-family matrix tests and platform-io storage tests pass.
- Full local gate passes and the phase is committed.

## Active Task Slice

```text
task730 [x] goal:rename managed fallback class | scope:src .phrase docs | verify:rg stale names
task731 [x] goal:update native/unsupported operation matrix | scope:src/io/platform_backend src/io.rs | verify:matrix test
task732 [x] goal:update runtime/stats semantics | scope:src/runtime.rs src/storage.rs src/stats.rs src/db.rs | verify:platform_io tests
task733 [x] goal:update protocol docs and evidence | scope:.phrase docs/usage.md | verify:rg stale semantics
task734 [x] goal:run validation and commit | scope:repo | verify:full gate plus git commit
```

## Evidence

- Platform-io already owns the blocking lane through the selected runtime, so
  directory listing on native targets should be counted as
  `ThreadPoolManagedAsync`, not as engine-visible blocking fallback.
- Linux true native async rows remain true where the whole Trine operation has
  a complete backend path.
- Windows and macOS have native async substeps, but several complete Trine
  operations remain partial until non-native steps are replaced or split.
- Targets without native threads cannot support managed thread-pool async and
  must use explicit host backends or report unsupported platform-io rows.

## Known Residuals

- Windows/macOS/BSD/Solaris operation rows still need future work before every
  complete operation can become `TruePlatformAsync`.
- Real Windows, FreeBSD, illumos, Solaris, and browser-WASM runtime diagnostics
  are not run from this macOS workspace.
- `wasm32-unknown-unknown` without `platform-io` compiles, but enabling
  `platform-io` still pulls a native dependency stack that does not support
  browser-WASM. The contract now prevents false `ThreadPoolManagedAsync`
  reporting on no-native-thread targets; a future feature-gating phase should
  split native platform-io dependencies from browser host backends.

## Next Recommendation

- After this phase is committed, return to engine revalidation using the
  completed platform-io abstraction matrix rather than a half-finished backend
  diagnosis.
