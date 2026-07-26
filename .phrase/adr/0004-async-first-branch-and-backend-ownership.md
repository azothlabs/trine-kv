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
backend for non-filesystem databases. Backend access goes through typed
accessors on `DatabaseStorage`; asking for an incompatible capability fails as
corruption inside the engine rather than silently using a placeholder.

Capability-specific storage traits remain the operation boundary. This ADR does
not introduce one large trait that pretends every backend supports every
operation.

### Branch execution

Durable branch state changes are planned once and executed through the selected
storage path. The public branch API follows the crate-wide rule:

- async methods use the operation name without a suffix;
- synchronous native adapters use an explicit `*_sync` suffix;
- async-only backends have a complete create, open, list, read, write, scan, and
  delete path.

The synchronous adapter may use synchronous storage mechanics, but it must
apply the same typed branch transition and identity checks as the async path.
Duplicated legality decisions are not allowed.

### Transaction ownership

The generic transaction module owns snapshot reads, staged writes, conflict
tracking, and commit. Immutable-content lifecycle extensions live under the
content module even when their methods are implemented on `Transaction`.

## Compatibility

Correcting the branch method names is a pre-1.0 breaking public API change.
Trine therefore increments the minor crate version. Storage formats, WAL,
manifest, table encoding, MVCC visibility, and durable branch registry encoding
do not change.

## Verification

- native and object-store branch lifecycle tests exercise the same public
  outcomes;
- async branch handles can read, write, delete, and scan on object storage;
- synchronous branch tests use only explicit `*_sync` methods;
- compile-time and source-boundary checks reject backend-specific fields on
  `DbInner` and content lifecycle implementations in `transaction.rs`;
- all-feature tests, strict Clippy, Rustdoc, doctests, formatting, and diff
  hygiene pass.
