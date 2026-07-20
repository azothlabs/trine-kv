# Content Reclaim Intent v1

## Purpose and safety boundary

This protocol is the Trine KV half of exact content reclamation. A verified
higher layer may atomically record short-lived reclaim intent for one sealed
`(StorageDomainId, ContentId)`. Intent is durable coordination state only. It
does not make bytes unavailable, start grace, delete replicas, or authorize
physical deletion.

The higher layer owns logical reachability, liveness touch, retained-root
generation, proof digest semantics, and proof expiry. Trine KV owns the sealed
descriptor, physical activity high-water mark, upload attachment authority,
read leases, and the durable intent record. Both halves are checked through one
optimistic Trine KV transaction.

Uncertainty retains readable bytes. Malformed or missing protected state blocks
intent until repaired.

## Public claims

`ContentReclaimAuthorization` contains:

- exact `StorageDomainId` and `ContentId`;
- an opaque 49-byte `ContentReclaimProofToken`;
- the instance-local `ReadVersion S` used by the higher-layer exact check;
- an exclusive proof deadline in Unix epoch milliseconds.

Trine KV compares and persists the token but does not parse Version, Policy,
root-generation, liveness, or digest meaning from it.

## Protected records

### Per-content control

Bucket: `\x01trine-content-control\x01`

Key:

```text
StorageDomainId[16] | ContentId[33]
```

Value:

```text
"TRNCRCL1"[8]
| StorageDomainId[16]
| ContentId[33]
| prior_activity_commit_seq[u64 big-endian]
| state[u8]
| proof_token[49]
| proof_verified_at[u64 big-endian]
| proof_expires_at_unix_ms[u64 little-endian]
| state_commit_seq[u64 big-endian]
```

`state_commit_seq` is filled with the transaction's final commit sequence.

- `Active`: `prior_activity_commit_seq` and every proof field are zero. The
  physical activity high-water mark is `state_commit_seq`.
- `ReclaimIntent`: `prior_activity_commit_seq` is the last Active high-water
  mark. Proof fields are nonzero and `state_commit_seq` is the exact intent
  acceptance sequence.

An upload-token publication or consumption and every leased open or durable
renewal stages `Active` on this same key. Such activity either occurs before an
intent check and exceeds its `S`, or conflicts with the transaction that is
installing intent. Activity after an accepted intent replaces it with Active.

### Upload authority by content

Bucket: `\x01trine-content-token-index\x01`

Key:

```text
StorageDomainId[16] | ContentId[33] | UploadTokenHash[32]
```

Value:

```text
"TRNCTIX1"[8]
| StorageDomainId[16]
| ContentId[33]
| UploadTokenHash[32]
| expires_at_unix_ms[u64 little-endian]
```

Seal publishes the bearer-token record, this secondary authority record, and
Active control state in one transaction. Token consumption removes the index
record and advances Active state in the same transaction as the caller's
attachment writes. Expired index records are inert and may be cleaned later;
malformed records block intent.

Read leases retain the existing exact-content key and record format defined by
`content-read-lease-v1.md`.

## Acceptance algorithm

`Transaction::stage_content_reclaim_intent` performs these checks at the
transaction's one read sequence:

1. proof deadline is still in the future and `0 < S <= transaction.read_version`;
2. the sealed descriptor exists and validates against the requested identity;
3. protected content control exists, decodes, and has physical activity
   high-water `<= S`;
4. the exact token-authority range contains no unexpired valid record;
5. the exact read-lease range contains no unexpired valid record;
6. the intent write preserves the prior activity high-water and binds the
   opaque token, `S`, expiry, and final acceptance commit sequence.

The point read of control plus the two transaction range reads are part of the
read set. Later token, activity, or lease publication makes commit return
`Conflict`; no intent becomes durable. Exact retry returns the original
acceptance sequence. A different newly verified token may replace an older
intent while preserving the same prior activity high-water.

## Current exclusions

- `open_content` remains a compatible unleased read and is explicitly unsafe
  against future physical deletion. This v1 intent does not block it because no
  deletion occurs in this slice.
- Migration, backup, repair, provider, and administrative physical holds do not
  yet exist in the content API. They must join the same control boundary before
  deletion can be enabled.
- Grace, a second logical/physical recheck, representation fencing, replica
  deletion, provider-version cleanup, and completion audit are later protocols.
- Intent alone is never accepted by a deletion worker as deletion authority.

## Required evidence

- available token and unexpired lease block intent;
- token consumption permits later intent and exact retry is idempotent;
- leased open racing a staged intent causes one transaction to conflict;
- leased open after intent returns control to Active and supersedes the old `S`;
- malformed control, token index, or lease state fails closed;
- intent and acceptance sequence survive native reopen;
- higher-layer touch or root-generation changes conflict with or invalidate the
  same transaction before acceptance.
