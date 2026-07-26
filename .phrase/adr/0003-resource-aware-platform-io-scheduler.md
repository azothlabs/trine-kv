# ADR 0003: Resource-Aware Platform I/O Scheduler

Date: 2026-07-26

## Status

Accepted

## Context

ADR 0002 defines Trine's operation-level platform I/O classes and target
matrix. It does not define how accepted work is scheduled.

The first native implementation created one worker per database and executed
`runtime.block_on(task.run())` for every received task. That shape was correct
only as a functional adapter. It allowed at most one native operation to be in
flight for the whole database, so unrelated random reads and different WAL
lanes could not use completion-based concurrency.

Replacing the loop with unconstrained spawning would be incorrect:

- two appends to one WAL file could both observe the same end offset;
- delete could race an admitted read that had not opened its file yet;
- rename publish and directory sync could cross;
- native and thread-pool fallback work could bypass each other's ordering;
- an unbounded ingress queue could move memory pressure into the database;
- detached workers gave database close no drain or join boundary.

The scheduler, not the selected OS library, must own these rules.

## Decision

Each native-file database owns one `PlatformIoDriver` facade with one bounded,
resource-aware `PlatformIoScheduler`. The scheduler exclusively owns both
executor implementations:

```text
KV engine
    |
StorageBackend traits          database operations and durability contract
    |
NativeFileBackend              native paths, append sessions, publish plans
    |
PlatformIoScheduler            admission, ordering, close, control counters
    |-- NativeCompletionExecutor
    `-- BlockingFallbackExecutor
```

The executor types do not own scheduling policy. The database-facing storage
backend does not own executor threads.

### Admission and execution

- The scheduler queue has a hard admission bound.
- The scheduler also caps admitted executor work.
- Queue saturation rejects the new submission with `RuntimeBusy`; it does not
  create a hidden unbounded queue.
- The native executor starts eagerly when the driver is created. Its runtime
  must successfully initialize before the native backend matrix is selected.
  Initialization failure selects the managed thread-pool matrix. On Linux and
  Windows, startup also verifies that Compio actually selected io_uring or
  IOCP respectively; a successfully constructed polling driver does not
  qualify as native file completion.
- One native runtime concurrently polls multiple admitted operation futures.
  It is not recreated per operation and it does not call `block_on` once per
  task.

### Resource ordering

Every task declares resource requests before admission:

- reads take shared access to an object;
- append and persist take exclusive access to the append object;
- publish takes exclusive access to its target, temporary file, and containing
  directory;
- delete takes exclusive access to the object and containing directory;
- directory listing takes shared access to the directory;
- directory creation and sync take exclusive access to the directory;
- writer-lease acquisition takes exclusive access to the lease object and its
  containing directory.

All requested resources are admitted as one unit. No task waits while holding
only part of its resource set.

Shared reads to one object may overlap. A mutation waits for those reads.
Later work may pass a blocked task only when their resource sets do not
conflict, preventing a stream of new readers from starving an earlier writer.
Operations on unrelated objects may overlap. The rules apply across native and
thread-pool executors, so fallback cannot bypass native ordering.

Append open returns a typed append session only after the selected executor has
successfully opened the append path. The storage append object owns that
session, and append/persist submissions require it. The scheduler still keys
exclusion by the underlying object so two sessions cannot mutate one WAL file
concurrently.

Manifest publish, immutable-object publish, and WAL rewrite use explicit
publish plan constructors. The plan remains intact through scheduling and
executor dispatch; callers and executor boundaries do not pass unrelated
boolean flags.

### Completion, failure, and close

The driver has `Running`, `Closing`, `Closed`, and `Failed` states.

- `Closing` rejects new work.
- Work accepted before close is drained to a terminal completion.
- Executor and scheduler threads are joined before close returns.
- A worker task panic completes that task with an error and moves the driver to
  `Failed`; queued work is failed rather than silently retried after an
  uncertain mutation.
- Operation-class counters are recorded at terminal completion only after the
  executor that actually starts the operation assigns its class. Admission
  rejection and executor loss before execution starts do not claim a backend
  class. Unsupported dispatches record `Unsupported`.
- Synchronous adapters park on a real waker. They do not poll on a timer.

The last driver owner also performs the same close path, but database close and
last-user-handle shutdown invoke it explicitly.

### macOS callback integration

DispatchIO callbacks wake Rust futures through async channels. The Compio
runtime thread never waits on a blocking callback receiver. Rare blocking
recovery paths run through the runtime's blocking facility.

## Consequences

- Different WAL lanes and unrelated object operations can be in flight on one
  completion runtime.
- Per-object and directory correctness no longer depends on accidental global
  serialization.
- Fallback work remains honest and safe under the same scheduler.
- Driver startup reports capability from the selected backend matrix after
  executor startup, rather than from a compile-time promise. Native startup
  failure selects the successfully started blocking fallback executor.
- A database owns additional scheduler and executor threads while platform I/O
  is selected, but those threads now have a deterministic shutdown boundary.
- The operation matrix in ADR 0002 remains the source of classification; this
  ADR is the scheduling and lifecycle contract applied to every matrix row.

## Verification Gate

- A resource-table test proves shared-read overlap, same-object mutation
  exclusion, and unrelated-object admission.
- A queue test proves the admission counter cannot exceed its configured
  capacity.
- A close test proves accepted work completes, threads join, and later
  submissions are rejected.
- A native test submits different WAL paths and requires more than one native
  future to be in flight on platforms where native runtime startup succeeds.
- An abandonment test proves a task lost before executor start releases its
  waiter and resources without claiming an execution class.
- WAL tests verify append bytes and durability behavior remain correct.
- Default, `platform-io`, and `platform-io-native` builds pass; strict Clippy,
  Rustdoc, doctests, formatting, and diff hygiene close the slice.
