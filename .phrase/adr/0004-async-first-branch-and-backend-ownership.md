# ADR 0004: Async-First Branches And Owned Backend State

Date: 2026-07-26
Status: Accepted

## Context

Trine's stable dependency rule is:

```text
pure invariants -> state transitions -> database orchestration -> storage adapters
```

The implementation violated that rule in two connected ways:

- durable branch operations had separate synchronous and asynchronous
  orchestration, while branch handles themselves only used synchronous storage;
- `DbInner` stored native, object-store, browser, and memory backends in
  unrelated fields, including invalid combinations and unused placeholder
  backends.

Those shapes made backend capability a repeated runtime question instead of an
owned database state. They also allowed the public branch API to claim
async-first behavior without providing an end-to-end async branch path.

## Decision

### Database backend ownership

One `DatabaseStorage` value owns the selected backend state for a database
handle. Its variants contain only the resources valid for that backend:

- memory owns the content-object memory store;
- filesystem owns the native/WASI file backend;
- object storage owns the data client, WAL client, and canonical database
  prefix;
- browser owns OPFS storage, its writer lease, and its WAL front door.

`DbInner` must not keep backend-specific `Option` fields or an unused native
backend for non-filesystem databases. Callers exhaustively match a
`DatabaseStorageRef`, whose variants expose only the resources valid for that
backend. Object-store and browser callers receive one resource bundle
containing every jointly owned client and coordinate. There is no
wrong-backend accessor and therefore no incompatible capability request that
can become a runtime corruption error.

Capability-specific storage traits remain the operation boundary. This ADR does
not introduce one large trait that pretends every backend supports every
operation.

The backend set is intentionally closed inside the crate because an
implementation participates in WAL, manifest publication, writer fencing,
recovery, and durability contracts together. Supporting third-party backends
would require a separately versioned public protocol and qualification suite;
it must not be introduced by merely making the internal traits public.

### Branch execution

Durable branch state changes are planned once and executed through the selected
storage path. The public branch API follows the crate-wide rule:

- async methods use the operation name without a suffix;
- synchronous native adapters use an explicit `*_sync` suffix;
- async-only backends have a complete create, open, list, read, write, scan, and
  delete path.

The async implementation is the only branch orchestration and range-merge
implementation. Synchronous methods first reject async-only persistent
backends, then drive that same future to completion. `BranchRange` likewise
adapts `AsyncBranchRange`; it does not own a second merge algorithm.

Branch registry encoding, owned handle state, range merging, durable
transitions, and public orchestration live in separate modules.
`BranchCreateRequest`, `BranchDeleteScan`, and `BranchDeletePlan` own lifecycle
decisions inside the single execution path.

### Transaction ownership

The private `transaction::core` module owns snapshot coordinates, staged
writes, conflict tracking, and extension claims. `Transaction` owns database
I/O and commit orchestration. This boundary remains inside the main crate:
transaction state has no independent reuse, version, or release lifecycle that
would justify a second Cargo package. Immutable-content lifecycle extensions
live under the content module even when their methods are implemented on
`Transaction`.
Token/activity, intent, quarantine, grace, sweep, and their protected guards
remain separate content-owned modules.

## Compatibility

This architecture phase does not own the release-version decision. The crate
package identity remains `0.6.0`; a future release process may choose a new
version only as an explicit release action. Storage formats, WAL, manifest,
table encoding, MVCC visibility, and durable branch registry encoding do not
change.

## Verification

- native and object-store branch lifecycle tests exercise the same public
  outcomes;
- async branch handles can read, write, delete, and scan on object storage;
- synchronous branch tests use only explicit `*_sync` methods;
- exhaustive storage enums make invalid resource combinations
  unrepresentable;
- integration tests compile-check the async/sync branch and transaction public
  contracts, while Rust visibility keeps transaction state private;
- all-feature tests, strict Clippy, Rustdoc, doctests, formatting, and diff
  hygiene pass.
