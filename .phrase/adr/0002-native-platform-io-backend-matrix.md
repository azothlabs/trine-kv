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

Cargo features must preserve that boundary:

- `platform-io` is the baseline feature. It uses Trine's own bounded
  thread-pool backend on native-thread targets and does not pull native async
  backend dependencies.
- `platform-io-threadpool` is an explicit alias for the baseline.
- `platform-io-native` enables native backend dependencies and prefers native
  async operation rows where evidence exists. Rows without native support route
  to the baseline thread-pool backend instead of blocking the caller runtime.
- Targets without native threads may compile the feature shape, but they must
  not advertise `PlatformAsyncIo` or report `ThreadPoolManagedAsync`.

The current implementation evidence is strongest on Linux, but Linux is not
the architecture boundary. Platform support is not uniform:

- Linux can use native async file operations when the backend feature enables
  the Linux native async path.
- Windows can use native overlapped/IOCP file primitives for positioned reads
  and writes with the selected backend, but current Trine operations still
  include non-IOCP open, metadata, sync, rename, delete, directory, listing, or
  lease steps before they can be counted as complete true platform async
  operations.
- macOS has an explicit backend row. With the selected backend, macOS file data
  operations use Apple `DispatchIO` for read/write steps, while metadata,
  rename, delete, directory, listing, and remaining durability steps are still
  partial or thread-pool managed at the complete Trine operation boundary.
- FreeBSD and Solaris-family targets have AIO primitive evidence in the
  selected backend for read/write/sync steps, but complete Trine operations
  remain partial while open, metadata, rename, delete, directory, listing, or
  lease steps are still blocking or helper-managed.
- Other Unix targets use platform-io's managed thread-pool path until
  platform-specific audits prove stronger behavior.
- Directory enumeration has no native async primitive in the selected backend,
  so native targets complete listing through platform-io's managed thread-pool
  path. Targets without native threads must not use that class.

## Decision

Trine records native platform I/O by operation capability class:

- `TruePlatformAsync`: the complete Trine operation is submitted through a real
  platform async completion mechanism for the selected target backend.
- `PlatformNativeAsyncButPartial`: the backend has a real platform async
  primitive for one or more lower-level steps, but the complete Trine operation
  still needs non-native-async steps.
- `ThreadPoolManagedAsync`: work is owned by `PlatformIoDriver` and blocking
  file work runs on platform-io's managed thread pool. The KV engine still sees
  the same async platform-io boundary. Browser WASM and other targets without
  native threads must not report this class.
- `BlockingFallback`: work is explicitly run through a platform-driver
  blocking fallback because the platform driver cannot currently move it to
  native async or its managed thread-pool path. It must be counted separately
  from Trine's bounded blocking adapter.
- `Unsupported`: the target cannot provide the operation through platform-io
  and must reject the mode or route through a different declared backend.

`DbStats` must report:

- whether native storage work is routed through `PlatformIoDriver`;
- true or partial native platform async task count;
- platform-io managed thread-pool task count;
- platform-driver blocking fallback task count;
- Trine bounded blocking-adapter task count.

`PlatformAsyncIo` capability is advertised when the selected platform driver can
return asynchronous completions to the caller runtime through true platform
async, partial native async, or platform-io's managed thread-pool path.
Per-operation counters remain the authority for whether a completion was
`TruePlatformAsync`, `PlatformNativeAsyncButPartial`,
`ThreadPoolManagedAsync`, `BlockingFallback`, or `Unsupported`. Browser WASM and
other targets without native threads must reject or use an explicit host backend
instead of reporting thread-pool managed async.

### Operation Matrix

The table records current implementation class first, then the target direction
for future backend phases. Under baseline `platform-io`, native-thread targets
use `ThreadPoolManagedAsync` for every supported row. Under
`platform-io-native`, the following matrix applies. A later phase may strengthen
a cell only with platform-specific implementation evidence.

| Trine operation | Linux current / target | Windows current / target | macOS current / target | BSD/other Unix current / target | Generic fallback current / target |
| --- | --- | --- | --- | --- | --- |
| Length lookup | `TruePlatformAsync` / keep true async where supported | `PlatformNativeAsyncButPartial` / audit metadata path and make complete operation true async if possible | `ThreadPoolManagedAsync` / audit Apple metadata APIs | `ThreadPoolManagedAsync` on other Unix; FreeBSD/Solaris-family managed for metadata | `ThreadPoolManagedAsync` / stay fallback unless target grows support |
| Random read | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / complete overlapped read path | `ThreadPoolManagedAsync` / audit native async read options | FreeBSD/Solaris-family `PlatformNativeAsyncButPartial`; other Unix `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| Whole-object read | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / complete open plus read path | `ThreadPoolManagedAsync` / audit complete operation | FreeBSD/Solaris-family `PlatformNativeAsyncButPartial`; other Unix `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| Temporary write plus rename publish | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit write, sync, rename, and directory steps | `ThreadPoolManagedAsync` / audit full publish path | FreeBSD/Solaris-family `PlatformNativeAsyncButPartial`; other Unix `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| Append open | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit append handle semantics | `ThreadPoolManagedAsync` / audit append open | `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| Append | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / complete serialized overlapped append or explicit lane strategy | `ThreadPoolManagedAsync` / audit async append feasibility | FreeBSD/Solaris-family `PlatformNativeAsyncButPartial`; other Unix `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| Persist/fsync | `TruePlatformAsync` / keep true async when backend completion is real | `PlatformNativeAsyncButPartial` / audit flush completion semantics | `ThreadPoolManagedAsync` / audit fsync/fcntl completion semantics | FreeBSD/Solaris-family `PlatformNativeAsyncButPartial`; other Unix `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| WAL rewrite | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit complete rewrite and publish steps | `ThreadPoolManagedAsync` / audit rewrite path | FreeBSD/Solaris-family `PlatformNativeAsyncButPartial`; other Unix `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| Delete | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit delete semantics | `ThreadPoolManagedAsync` / audit delete semantics | `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| Directory create | `TruePlatformAsync` / keep true async | `PlatformNativeAsyncButPartial` / audit create-directory completion | `ThreadPoolManagedAsync` / audit create-directory completion | `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| Directory sync | `TruePlatformAsync` / keep true async where supported | `PlatformNativeAsyncButPartial` / audit directory handle sync | `ThreadPoolManagedAsync` / audit directory sync support | FreeBSD/Solaris-family `PlatformNativeAsyncButPartial`; other Unix `ThreadPoolManagedAsync` | `ThreadPoolManagedAsync` |
| Directory listing | `ThreadPoolManagedAsync` / replace only if a real async enumeration operation exists | `ThreadPoolManagedAsync` / replace only with audited async enumeration | `ThreadPoolManagedAsync` / replace only with audited async enumeration | `ThreadPoolManagedAsync` / replace only with audited async enumeration | `ThreadPoolManagedAsync` on native-thread targets; `Unsupported` without native threads |
| Writer lease | `TruePlatformAsync` / keep true async where current backend supports it | `PlatformNativeAsyncButPartial` / audit lock/open semantics | `ThreadPoolManagedAsync` / audit lock semantics | `ThreadPoolManagedAsync` / audit lock semantics | `ThreadPoolManagedAsync` or `Unsupported` by target |

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
- macOS must not be described as true native async for regular files with the
  selected backend, but it is now a first-class platform-io target with an
  explicit matrix row.
- FreeBSD and Solaris-family targets may be classified as partial for operations
  that include selected-backend AIO read/write/sync primitives, but they must
  not be described as complete true platform async until remaining blocking
  steps are replaced and verified.
- Other BSD/Unix targets must not be described as true native async for regular
  files until a stronger backend is implemented and verified.
- Directory enumeration remains a separately counted native-async gap until a
  backend exposes a real async directory enumeration operation; on native
  targets it is currently completed by platform-io's managed thread-pool path.
- With the current backend matrix, non-Linux targets can use the platform I/O
  driver when the `platform-io` Cargo feature is enabled, but their current
  classes are thread-pool managed or partial until audited target
  implementations land.
