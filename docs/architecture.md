# Architecture

Trine is organized around one dependency rule:

```text
pure invariants -> state transitions -> database orchestration -> storage adapters
```

Code on the left must not depend on code to its right. Platform adapters may
translate an already-decided operation into filesystem, browser, WASI, or
object-provider calls; they must not independently decide whether a durable
state change is legal.

## Trusted decision core

`src/invariants.rs` contains small, allocation-free predicates for sequence
successors, durable-fence monotonicity, and reachability-safe deletion.
`ManifestState::validate_successor`, `ObjectLeaseState::plan_*`, and
`UploadSessionState::plan_publish_against` add typed domain transitions. These
functions decide legality and idempotency before any irreversible I/O begins.

The trusted core is deliberately small enough for exhaustive Kani harnesses,
mutation testing, and direct review. It does not perform I/O, acquire locks, or
start tasks.

`verification/kani/Cargo.toml` compiles that exact source file as the complete
proof crate. The production crate and proof target therefore cannot silently
drift into two versions of an invariant.

## Orchestration boundaries

- `db/commit.rs` owns commit admission, WAL publication, and visibility order;
  `db/commit_tracker.rs` owns commit visibility and publish barriers.
- `db/engine/maintenance/coordinator.rs` schedules maintenance;
  `registry.rs`, `reads.rs`, `flush.rs`, and `compaction.rs` own distinct work.
- `db/async_api/` separates open/refresh, bucket/checkpoint, default-bucket
  data access, and maintenance entry points. Explicit sync methods adapt the
  same engine rules.
- `branch/` separates registry format, owned state, handle I/O, range merge,
  lifecycle transitions, and public orchestration. Sync operations drive the
  async implementation rather than owning a second branch algorithm.
- `transaction/core.rs` owns generic transaction state. Immutable-content
  extensions live in `content/transaction/`, divided by lifecycle capability.
- `db/content/upload/` separates session, seal, abort, quota, and operator
  maintenance flows.
- `db/content/backend.rs` is the common content-object adapter selected once at
  the storage boundary.
- `content/identity/`, `content/upload/`, and `content/reclaim/` separate
  public values, live upload behavior, lifecycle states, and durable record
  codecs. Reclaim records are divided by the transition that owns them.
- `db/engine/open/` owns backend-specific construction. `db/engine/storage/`
  owns persistence, flush, compaction, and browser maintenance mechanics.
- `storage/` separates object identities, read contracts, capability traits,
  and backend implementations. `substrate/` separates filesystem durability
  from object-store WAL lanes, leases, and chain encoding.
- `object_store/contract.rs` verifies provider semantics,
  `memory.rs` supplies the deterministic implementation, and `backend.rs`
  translates storage traits into object operations.

The synchronous API is a caller-facing adapter over the same database rules.
Browser, native, and object-store paths may differ in mechanics, but durable
transition decisions and garbage-collection eligibility are shared.

## Extension rule

A new storage backend or durable state must arrive in this order:

1. State the invariant and failure outcomes without backend terminology.
2. Add a pure transition or policy type with exhaustive unit tests.
3. Define the smallest capability interface needed by orchestration.
4. Implement the adapter without duplicating transition conditions.
5. Add before/after boundary cases for every irreversible I/O instruction.
6. Add recovery, property/reference, and Gherkin evidence.
7. Document host assumptions that cannot be checked from the data plane.

An adapter is not allowed to weaken immutable-create, conditional-publish,
fencing, reachability, or confirmed-write semantics. Unsupported capabilities
must fail closed.

## Error and stop-write policy

Ordinary request errors leave the database usable. Corruption, fencing loss,
and a durable outcome that cannot be determined close the handle to new work.
`DbStats` exposes the exactly-once stop plus its reason class. The original
typed error remains the operation result and is also recorded by maintenance,
so operators can correlate the stop with the first causal event.

Explicit user close is not counted as an automatic integrity stop.

## Remaining size policy

File length is a diagnostic, not the design rule. A module may be moderately
large when it owns one cohesive algorithm and its local helpers. It should be
split when it mixes policy with mechanics, owns multiple lifecycle stages, or
causes the same rule to appear in more than one backend. Mechanical
one-function-per-file fragmentation is intentionally avoided.
