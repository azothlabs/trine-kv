# Content Physical Hold v1

## Purpose

This protocol is the common Trine KV fence for storage work that must keep one
sealed immutable `ContentObject` physically readable. Migration, backup,
repair, provider, and administrative work use the same record and transaction
rules. A hold class is operational metadata, not authorization and not a reason
to weaken protection.

Logical File, Version, Policy, and Principal meaning remains above Trine KV.
The higher layer supplies an opaque owner only after its own authority check.

## Identities and classes

- `ContentPhysicalHoldId` is a caller-retained, generated, versioned 16-byte
  identity. Byte zero is format version `1`; the remaining bytes are random.
- `ContentPhysicalHoldOwnerId` is an opaque 16-byte higher-layer workflow or
  authority identity. Trine KV compares but does not interpret it.
- `ContentPhysicalHoldKind` has stable tags: migration `1`, backup `2`, repair
  `3`, provider `4`, and administrative `5`.
- One hold fixes exactly `(StorageDomainId, ContentId, HoldId)`.

## Protected record

Bucket: `\x01trine-content-physical-hold\x01`

Key:

```text
StorageDomainId[16] | ContentId[33] | ContentPhysicalHoldId[16]
```

Value:

```text
"TRNCPHL1"[8]
| ContentPhysicalHoldId[16]
| ContentPhysicalHoldOwnerId[16]
| StorageDomainId[16]
| ContentId[33]
| kind[u8]
| state[u8]
| expires_at_unix_ms[u64 little-endian]
```

State `0` is Active and state `1` is Released. A Released record is a durable
idempotency tombstone: it never blocks intent and the same HoldId can never be
reacquired, resumed, or renewed.

Released and expired records are compact lifecycle history. This version does
not delete them: housekeeping may remove one only after a future protocol can
prove that its HoldId is outside every supported retry/recovery horizon. Merely
observing inactivity is not that proof.

`expires_at_unix_ms = 0` means explicit durable release is required. Any other
value is an exclusive deadline. Identity, length, version, class, key/value
agreement, and content algorithm are validated on every protected read.
Malformed records fail closed and block reclaim intent until repaired.

## Acquire, resume, renew, release

Acquisition validates the sealed descriptor, writes the hold, and advances the
per-content control record to Active in one optimistic transaction. A returned
handle therefore always follows durable publication. The caller retains the
HoldId before acquisition. An exact active retry with the same domain, content,
owner, class, and lifetime form returns the original record; a different owner
or semantics fails. An expired identity cannot be reused or revived. This
closes the commit-before-response boundary even for until-released holds.

Resume is a read-only exact lookup by domain, content, hold identity, and owner.
It neither extends expiry nor revives a released or expired record.

Renewal is valid only for an unexpired expiring hold. It verifies exact owner
and identity, publishes the greater of the old deadline and `now + ttl`, and
advances the per-content control record in the same transaction. Expiry cannot
be revived and explicit-release holds cannot be converted by renewal.

Release verifies exact owner and identity and transactionally advances Active
to Released. Repeating release is successful, while a delayed acquisition
cannot move Released back to Active. Release does not advance physical
activity: removing a blocker cannot invalidate a proof by itself. Drop performs
no asynchronous I/O. Until-released owners must retain a recovery path that can
resume and release after a crash.

Handle clones share local expiry and release observations, but the protected
record is the cross-process source of truth.

## Reclaim-fence participation

`Transaction::stage_content_reclaim_intent` exact-range reads this bucket for
the target content. Every well-formed active hold returns the typed
`ContentReclaimBlocker::PhysicalHold`, including hold identity, class, and its
optional deadline.

Acquire and renew also write the content control key. Concurrent activity
therefore either precedes intent and blocks or supersedes its proof sequence,
or makes the intent transaction conflict. Activity after accepted intent
returns control to Active. Release conflicts with an intent transaction whose
hold-range read observed Active; a retry decodes but ignores Released.

Intent remains coordination state only. This protocol does not start grace,
hide bytes, or authorize representation deletion.

## Unleased compatibility boundary

This hold registry does not make compatible `open_content` handles observable
across processes. Trine KV supports concurrent read-only instances, and a
read-only instance may lack authority to publish a durable lease or heartbeat.
No portable local counter, filesystem handle, or object-store request can prove
that every such handle has drained.

The irreversible leased-only forward fence now exists in
`content-access-barrier-v1.md`, and reclaim intent requires its protected
coordinate. It blocks new unleased opens even from stale read-only database
handles. It does not end handles opened before the barrier: those handles have
no maximum lifetime. Automated physical deletion therefore remains disabled
until a proved reader-drain or external-coordination transition covers every
pre-barrier reader. Grace alone is not that proof; uncertainty retains bytes.

## Required evidence

- every hold class blocks intent with its exact typed coordinates;
- release permits a later intent and is idempotent;
- a Released tombstone rejects resume, renewal, and delayed reacquisition;
- acquire racing staged intent makes one transaction conflict;
- acquire or renew after intent returns control to Active;
- expiry cannot be renewed or resumed;
- explicit holds survive native reopen and resume under the exact owner;
- wrong owner and malformed identity or record fail closed;
- no test or API treats intent or a compatible unleased-read grace period as
  deletion authority.
