# ADR 0002: Native Platform IO Backend Matrix

Date: 2026-06-01

## Status

Accepted

## Context

Trine's `io` boundary owns database operations and completions. Native platform
libraries and OS APIs are backend implementations below that boundary.

The current native backend can route many file operations below
`PlatformIoDriver`, but platform support is not uniform:

- Linux can use native async file operations when the backend feature enables
  the Linux native async path.
- Windows can use IOCP for lower-level file read/write primitives, but current
  Trine composite storage operations still include fallback-classified open,
  metadata, sync, rename, directory, or listing steps.
- macOS, BSD, and other Unix targets use the backend fallback path for ordinary
  file work in this phase. Polling readiness is not treated as true native async
  regular-file I/O.
- Directory enumeration has no native async primitive in the selected backend.

## Decision

Trine records native platform I/O by operation class:

- `TrueAsync`: operation is submitted through a native async platform primitive
  for the selected target backend.
- `BackendFallback`: operation is below `PlatformIoDriver` but the selected
  backend cannot honestly provide a native async primitive for that operation.
- `BlockingFallback`: operation is an explicit platform-driver blocking
  fallback, currently used for directory/object listing.

`DbStats` must report:

- true platform async task count;
- platform backend fallback task count;
- platform-driver blocking fallback task count;
- Trine bounded blocking-adapter task count.

`PlatformAsyncIo` capability is advertised only when the selected target has at
least one true Trine-level platform async storage operation. A target whose
current Trine composite operations are all fallback-classified must use the
bounded blocking adapter instead of starting a platform driver only to count
fallback work.

## Consequences

- Linux true async requires the native async backend feature.
- Windows IOCP coverage is partial and must remain per-operation. A composite
  Trine operation is not counted as true platform async until every required
  step has a native async path.
- macOS/BSD must not be described as true native async for regular files until
  a stronger backend is implemented and verified.
- Directory enumeration remains a separately counted blocker until a backend
  exposes a real async directory enumeration operation.
- With the current backend matrix, non-Linux targets do not advertise
  `PlatformAsyncIo` even when the `platform-io` Cargo feature is enabled.
