# Content Access Barrier v1

## Purpose

This protocol irreversibly changes one `StorageDomainId` from compatible
unleased content opens to leased-only opens. It supplies the forward fence that
physical reclamation needs before reader drain can be proved. It does not prove
that handles opened before the fence have drained and does not authorize byte
deletion.

The barrier is per storage domain because deduplication, lease inspection, and
physical lifecycle are scoped by that identity. It is not a File, Principal,
Policy, Branch, or authorization record.

## Identity and public state

`ContentAccessBarrierId` is a versioned 16-byte identity. Byte zero is format
version `1`. `ContentAccessMode` is either `CompatibleUnleased` or
`LeasedOnly { barrier_id }`.

The transition is intentionally one-way. There is no remove, disable, or
compatible-mode restoration API. A later rollback could admit an unleased
handle while physical lifecycle work relies on the fence.

## Backend barrier

Every unleased `open_content` directly reads this content-backend object before
reading the descriptor:

```text
<content-root>/domains/<StorageDomainId hex>/access/leased-only.trinebarrier
```

Value:

```text
"TRNCABR1"[8] | StorageDomainId[16] | ContentAccessBarrierId[16]
```

The direct object read is deliberate. Native and object-store read-only
database handles may retain an old ordinary-KV view; the content backend is the
shared byte authority already used for descriptors and chunks. An absent object
permits the compatible open. A valid object returns `ContentLeaseRequired`.
Malformed, truncated, unknown-version, or path-mismatched bytes fail closed.

Native publication uses temporary-write plus atomic rename and the configured
content durability. Object-store publication completes only after the
`ObjectClient` acknowledges PUT. A concurrent open linearizes at its barrier
read: if it reads absence before publication, it is a pre-barrier handle and
must be covered by later drain proof; after publication, new unleased opens are
rejected.

`open_content_leased` reads the descriptor through the internal leased path,
publishes its exact durable lease and Active content control, and only then
returns. A read-only database cannot use that path because it cannot publish a
lease.

## Protected commit coordinate

After the backend barrier is acknowledged, the writer records the same identity
in the protected `\x01trine-content-control\x01` bucket.

Key:

```text
"access:"[7] | StorageDomainId[16]
```

Value:

```text
"TRNCACO1"[8]
| StorageDomainId[16]
| ContentAccessBarrierId[16]
| enforced_at_commit_seq[u64 big-endian]
```

The sequence slot is filled with the transaction's final local commit sequence.
The record is the stable ordering coordinate used by later lifecycle work; it
is not portable identity and not reader-drain evidence.

## Publication and recovery

`enforce_content_leased_only` serializes transition calls in the active writer:

1. read the backend barrier directly;
2. if absent, publish the caller's requested barrier identity;
3. create/read the protected control bucket and exact coordinate key;
4. if the coordinate exists, require its identity to match the backend object;
5. otherwise publish the coordinate with its final commit sequence.

The backend barrier is always first. A crash between steps 2 and 5 can reject
unleased opens while lacking a coordinate; reclaim intent reports
`LeasedOnlyBarrierUncoordinated`. Retrying with any requested identity adopts
the already-published backend identity and finishes the coordinate. The reverse
unsafe state—coordinate visible before barrier—is not published.

One Trine writer lease/fencing epoch prevents independent writers. The local
transition lock handles concurrent calls inside that writer. Identity mismatch
between backend and KV state is corruption and fails closed.

## Reclaim boundary

`Transaction::stage_content_reclaim_intent` requires both a direct valid
leased-only barrier and its matching protected coordinate. The coordinate point
read joins the optimistic transaction read set. Compatible mode reports
`UnleasedAccessAllowed`; an interrupted publication reports the barrier identity
needed for recovery.

Intent still does not prove drain. Existing unleased `ContentHandle` values are
allowed to finish indefinitely. Automated physical deletion remains disabled
until a separate protocol supplies evidence that every pre-barrier reader
session has ended, or an external coordinator supplies an equivalent
deployment-specific proof.

## Required evidence

- compatible unleased opens work before the barrier;
- the same call returns `ContentLeaseRequired` after publication;
- leased opens continue and create durable leases;
- a pre-barrier handle remains readable, proving this is not drain evidence;
- already-open native and stale object-store read-only handles see the direct
  barrier without ordinary-KV refresh;
- retry adopts the existing identity and returns the original commit coordinate;
- crash state with backend barrier but no coordinate blocks reclaim intent and
  is recoverable;
- malformed or mismatched state fails closed.
