# Content Quarantine v1

## Purpose and safety boundary

This protocol turns an exact accepted reclaim intent into a durable read fence
without deleting, relocating, or hiding the descriptor or bytes. It is the
first per-content state that prevents a new leased read from appearing after the
final lease scan.

Quarantine is not grace and is not deletion authority. V1 records no grace
deadline, runs no delete worker, changes no physical representation, and removes
no local or object-store object. A future sweep requires a new logical proof and
fresh physical checks beyond this protocol.

## Preconditions and layer ownership

The higher layer owns logical reachability. Before calling
`Transaction::stage_content_quarantine`, it must re-read the exact durable
`ReclaimProof`, candidate, liveness coordinate, and retained-root generation in
the same optimistic transaction. This is a second lifecycle validation after
intent acceptance, not trust in an old worker result.

Trine KV owns the physical half. In that transaction it requires:

- the proof is unexpired and valid for the transaction read sequence;
- the direct leased-only barrier and protected coordinate match;
- one valid `ContentReaderDrainAttestation` is bound to that barrier;
- the exact same proof token is already present as accepted reclaim intent;
- the sealed descriptor validates;
- no newer physical activity exceeds the proof sequence;
- no unexpired upload-token authority, read lease, or active physical hold
  exists.

Every protected point/range read joins optimistic conflict validation. A
concurrent logical touch, token, leased open/renewal, or hold either blocks the
check or makes quarantine commit conflict.

## Protected record

Bucket: `\x01trine-content-control\x01`

Key:

```text
"quarantine:"[11] | StorageDomainId[16] | ContentId[33]
```

Value:

```text
"TRNCQRT1"[8]
| StorageDomainId[16]
| ContentId[33]
| ContentReclaimProofToken[49]
| verified_at_commit_seq[u64 big-endian]
| proof_expires_at_unix_ms[u64 little-endian]
| intent_accepted_at_commit_seq[u64 big-endian]
| ContentAccessBarrierId[16]
| barrier_enforced_at_commit_seq[u64 big-endian]
| ContentReaderDrainAttestationId[16]
| quarantined_at_commit_seq[u64 big-endian]
```

`quarantined_at_commit_seq` is filled with the transition transaction's final
commit sequence. The proof expiry remains audit provenance: expiry does not
silently remove a committed quarantine, and it cannot authorize a later sweep.
Unknown formats, mismatched key identity, zero coordinates, or invalid ordering
fail closed.

The existing per-content control record remains in exact `ReclaimIntent` state
while quarantine is present. This preserves its accepted proof and physical
activity high-water. A reviving transaction atomically removes quarantine and
returns the control record to Active.

## Transition algorithm

`Transaction::stage_content_quarantine` performs:

1. validate proof expiry and read-sequence bounds;
2. require the coordinated direct barrier and protected coordinate;
3. read and validate the matching reader-drain attestation;
4. validate the exact descriptor;
5. read the content control record and require the exact accepted intent;
6. recheck the physical activity high-water;
7. exact-range scan token, lease, and hold authority;
8. read the quarantine key;
9. return its original sequence for an exact retry, or stage a new commit-stamped
   quarantine record.

The higher-layer logical recheck and all Trine KV checks use the same
transaction. A commit conflict publishes no quarantine.

## Read fence

`Db::open_content_leased` and durable lease renewal read the exact quarantine
key in the same transaction that would publish lease activity. A valid record
returns `Error::ContentQuarantined { quarantined_at }`; malformed state fails
closed. The lease write is not committed.

Compatible unleased opens are already disabled by the domain barrier. Handles
that predate the barrier are covered only by the trusted drain attestation.
Handles with durable leases must expire before quarantine can pass its exact
lease scan.

Reading the descriptor internally before the transactional fence does not
return a handle and changes no authority. Direct access to object-store keys is
outside this API and must already be controlled by the credential-retirement
contract in `content-reader-drain-attestation-v1.md`.

## Safe revival before deletion

V1 has no deletion state, so new authoritative physical activity must retain
availability rather than strand content behind quarantine:

- upload-token publication or consumption;
- migration, backup, repair, provider, or administrative hold acquisition or
  renewal.

These operations validate any existing quarantine record, delete it, and write
Active control in their same transaction. A race with quarantine conflicts on
the exact quarantine/control key or the token/hold range. After revival, a new
logical proof and intent are required before quarantine can be attempted again.

A leased open is not a revival authority. It is rejected while quarantine is
durable. This distinction prevents a raw read by `ContentId` from defeating the
read fence, while attachment and storage-safety work can conservatively retain
the bytes.

## Crash and retry semantics

- crash before transition commit: no quarantine is visible;
- commit before response: `Db::content_quarantine` discovers the durable record,
  and exact staging retry returns its original commit sequence while the proof
  remains valid;
- crash after commit: native reopen and refreshed remote state retain the read
  fence;
- crash during revival: quarantine deletion and Active publication are one
  transaction, so recovery sees either the old quarantine or fully revived
  state;
- malformed or partially forged protected bytes fail closed.

No `Drop` implementation performs cleanup. Quarantine has no expiry or
best-effort background removal.

## Reclaim-grace and future sweep boundary

`content-reclaim-grace-v1.md` now defines a separately recorded wall-clock
scheduling observation after another logical and physical recheck. It retains
quarantine and deletes nothing. Its deadline is not elapsed-time proof across
clock jumps or restart and is not deletion authority.

A future final-validation protocol must define a trusted clock contract,
provider-version behavior, restart recovery, and another fresh logical and
physical authorization. It may begin physical deletion only after
representation and replica preconditions are also proved.

This v1 record alone, together with intent and reader-drain attestation, still
does not permit deleting any byte.

## Required evidence

- missing drain attestation or exact intent blocks quarantine with typed state;
- logical touch/root change before the recheck invalidates it;
- logical or leased activity after staging makes commit conflict;
- token, lease, and every physical-hold class are freshly rechecked;
- a committed quarantine blocks new leased open and renewal;
- exact retry returns the original commit coordinate;
- native reopen preserves the fence;
- token/hold activity atomically revives content before deletion exists;
- malformed quarantine blocks reads and revival;
- no path records grace or deletes descriptors, chunks, replicas, or provider
  versions.
