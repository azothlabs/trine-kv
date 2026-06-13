# ADR 0002: Native Platform I/O Contract And Backend Matrix

Date: 2026-06-01

## Status

Accepted

## Context

Trine's `io` boundary owns database operations and completions. Native platform
libraries and OS APIs are backend implementations below that boundary.

The purpose of `platform-io` is to give the KV engine one async I/O boundary
while each supported platform handles its own mechanics below that boundary.
The engine should not know whether a platform backend uses io_uring, IOCP,
Apple-specific APIs, BSD-specific APIs, polling, or an explicit fallback. The
engine asks for a Trine storage operation and awaits the completion.

The current implementation evidence is strongest on Linux, but Linux is not
the architecture boundary. Platform support is not uniform:

- Linux can use native async file operations when the backend feature enables
  the Linux native async path.
- Windows can use native overlapped/IOCP file primitives for positioned reads
  and writes with the selected backend, but current Trine operations still
  include blocking or helper-managed open, metadata, sync, rename, delete,
  directory, listing, or lease steps before they can be counted as complete true
  platform async operations.
- macOS, BSD, and other Unix targets need platform-specific audits before Trine
  can claim true platform async regular-file operations.
- Directory enumeration has no native async primitive in the selected backend.

## Decision

Trine records native platform I/O by operation capability class:

- `TruePlatformAsync`: the complete Trine operation is submitted through a real
  platform async completion mechanism for the selected target backend.
- `PlatformNativeAsyncButPartial`: the backend has a real platform async
  primitive for one or more lower-level steps, but the complete Trine operation
  still needs fallback-classified steps. This does not advertise
  `PlatformAsyncIo` for that operation.
- `PlatformManagedFallback`: work is owned by `PlatformIoDriver`, but the
  selected backend completes it through polling, an internal helper, inline
  completion, or another backend-managed fallback. The KV engine still sees the
  same platform-io boundary.
- `BlockingFallback`: work is explicitly run through a platform-driver
  blocking fallback. It must be counted separately from Trine's bounded
  blocking adapter.
- `Unsupported`: the target cannot provide the operation through platform-io
  and must reject the mode or route through a different declared backend.

`DbStats` must report:

- whether native storage work is routed through `PlatformIoDriver`;
- true platform async task count;
- platform backend fallback task count;
- platform-driver blocking fallback task count;
- Trine bounded blocking-adapter task count.

`PlatformAsyncIo` capability is advertised only when the selected target has at
least one true Trine-level platform async storage operation. A target whose
current operations are all fallback-classified may still route work through
`PlatformIoDriver` when the user selects `RuntimeMode::PlatformIo`, but it must
report fallback task counts and must not advertise `PlatformAsyncIo`.

### Operation Matrix

The table records current implementation class first, then the target direction
for future backend phases. A later phase may strengthen a cell only with
platform-specific implementation evidence.

| Trine operation | Linux current / target | Windows current / target | macOS current / target | BSD/other Unix current / target | Generic fallback current / target |
| --- | --- | --- | --- | --- | --- |
| Length lookup | `TruePlatformAsync` / keep true async where supported | `PlatformNativeAsyncButPartial` / audit metadata path and make complete operation true async if possible | `PlatformManagedFallback` / audit Apple metadata APIs | `PlatformManagedFallback` / audit target APIs | `PlatformManagedFallback` / stay fallback unless target grows support |
| Random read | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / complete overlapped read path | `PlatformManagedFallback` / audit native async read options | `PlatformManagedFallback` / audit target async read options | `PlatformManagedFallback` |
| Whole-object read | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / complete open plus read path | `PlatformManagedFallback` / audit complete operation | `PlatformManagedFallback` / audit complete operation | `PlatformManagedFallback` |
| Temporary write plus rename publish | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit write, sync, rename, and directory steps | `PlatformManagedFallback` / audit full publish path | `PlatformManagedFallback` / audit full publish path | `PlatformManagedFallback` |
| Append open | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit append handle semantics | `PlatformManagedFallback` / audit append open | `PlatformManagedFallback` / audit append open | `PlatformManagedFallback` |
| Append | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / complete serialized overlapped append or explicit lane strategy | `PlatformManagedFallback` / audit async append feasibility | `PlatformManagedFallback` / audit async append feasibility | `PlatformManagedFallback` |
| Persist/fsync | `TruePlatformAsync` / keep true async when backend completion is real | `PlatformNativeAsyncButPartial` / audit flush completion semantics | `PlatformManagedFallback` / audit fsync/fcntl completion semantics | `PlatformManagedFallback` / audit fsync completion semantics | `PlatformManagedFallback` |
| WAL rewrite | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit complete rewrite and publish steps | `PlatformManagedFallback` / audit rewrite path | `PlatformManagedFallback` / audit rewrite path | `PlatformManagedFallback` |
| Delete | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit delete semantics | `PlatformManagedFallback` / audit delete semantics | `PlatformManagedFallback` / audit delete semantics | `PlatformManagedFallback` |
| Directory create | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit create-directory completion | `PlatformManagedFallback` / audit create-directory completion | `PlatformManagedFallback` / audit create-directory completion | `PlatformManagedFallback` |
| Directory sync | `TruePlatformAsync` / keep true async where supported | `PlatformNativeAsyncButPartial` / audit directory handle sync | `PlatformManagedFallback` / audit directory sync support | `PlatformManagedFallback` / audit directory sync support | `PlatformManagedFallback` |
| Directory listing | `BlockingFallback` / replace only if a real async enumeration operation exists | `BlockingFallback` / replace only with audited async enumeration | `BlockingFallback` / replace only with audited async enumeration | `BlockingFallback` / replace only with audited async enumeration | `BlockingFallback` |
| Writer lease | `TruePlatformAsync` / keep true async where current backend supports it | `PlatformNativeAsyncButPartial` / audit lock/open semantics | `PlatformManagedFallback` / audit lock semantics | `PlatformManagedFallback` / audit lock semantics | `PlatformManagedFallback` or `Unsupported` by target |

### Phase 154 Entry Criteria

Driver cleanup may start only after this contract is in place. It must:

- keep platform async mechanics inside `platform-io`;
- let each backend report operation capability class;
- make the KV engine depend on Trine operations rather than OS async APIs;
- preserve existing Linux write and flush behavior;
- keep fallback accounting visible without treating fallback as the final
  platform-io goal.

## Consequences

- Linux true async remains current evidence, not the definition of
  `platform-io`.
- Windows IOCP or overlapped I/O coverage must remain per-operation. Current
  evidence proves positioned read/write primitives, not whole Trine operations.
  A Trine operation is not counted as true platform async until every required
  step has a correct platform completion path or a Trine-owned async strategy
  that keeps the operation true at the storage boundary.
- macOS/BSD must not be described as true native async for regular files until
  a stronger backend is implemented and verified, but they remain first-class
  platform-io targets.
- Directory enumeration remains a separately counted blocker until a backend
  exposes a real async directory enumeration operation.
- With the current backend matrix, non-Linux targets can use the platform I/O
  driver when the `platform-io` Cargo feature is enabled, but their current
  classes are fallback or partial until audited target implementations land.
