# Content Reclaim Sweep v1

## Purpose and capability boundary

This protocol is the first operation allowed to remove immutable content bytes.
It converts an exact quarantined object with completed grace into a durable,
crash-resumable sweep. Physical deletion remains disabled by default and is
enabled independently for each backend.

V1 enables only native filesystem content storage. In-memory, WASI, browser,
and object-store backends fail closed. Ordinary object-store key deletion does
not prove deletion of provider versions, delete markers, locked objects,
retained objects, or legal holds.

## Authority chain

Final staging requires all of the following in one optimistic transaction:

- a fresh higher-layer exact-absence proof whose stable read and publication
  coordinates are at or after the durable grace start;
- the direct leased-only barrier, its protected coordinate, and the same
  trusted reader-drain attestation retained by quarantine;
- the exact quarantine and grace records;
- a trusted clock/restart attestation bound to that grace, with an observation
  no earlier than `not_before_unix_ms`;
- a valid descriptor and its exact `UploadId` and chunk count;
- no physical activity newer than the fresh proof;
- no unexpired upload token, read lease, or active physical hold;
- an enabled and qualified backend capability.

The clock attestation is an explicit trust boundary, not a raw wall-clock
comparison performed by Trine KV. The caller retains the canonical evidence
named by its digest and must establish elapsed-time/restart truth before
attesting. Trine KV validates identity, ordering, and exact binding; it cannot
inspect an external supervisor or time authority.

## Durable state

The sweep record is keyed by `(StorageDomainId, ContentId)` in the protected
content-control bucket. `Prepared` binds the fresh authorization, quarantine,
grace, barrier/drain coordinates, clock attestation, descriptor `UploadId`, and
chunk count. Its commit sequence is the irreversible worker fence.

`Reclaimed` retains the identity and completion sequence as a tombstone until a
later upload of the same bytes publishes new authoritative activity. New
attachment, token, lease, or hold activity cannot revive `Prepared`. Activity
may remove `Reclaimed` and publish Active only after new content bytes and a
descriptor have been established.

Unknown state, malformed coordinates, descriptor disagreement, or a missing
authority record fails closed.

### Protected record layout

Bucket: `\x01trine-content-control\x01`

Key:

```text
"sweep:"[6] | StorageDomainId[16] | ContentId[33]
```

Value:

```text
"TRNCRSW1"[8]
| state[u8]                         # 0 Prepared, 1 Reclaimed
| StorageDomainId[16]
| ContentId[33]
| fresh ContentReclaimProofToken[49]
| fresh_verified_at[u64 big-endian]
| fresh_proof_expiry_unix_ms[u64 little-endian]
| quarantined_at[u64 big-endian]
| grace_started_at[u64 big-endian]
| ContentAccessBarrierId[16]
| barrier_enforced_at[u64 big-endian]
| ContentReaderDrainAttestationId[16]
| ContentReclaimClockAttestationId[16]
| ContentReclaimClockCoordinatorId[16]
| ContentReclaimClockEvidenceDigest[33]
| clock_observed_at_unix_ms[u64 little-endian]
| descriptor UploadId[16]
| descriptor_chunk_count[u64 little-endian]
| prior_prepared_at[u64 big-endian]
| state_commit_at[u64 big-endian]
```

For Prepared, `prior_prepared_at` is zero and the transaction fills
`state_commit_at`; decoding exposes that final value as `prepared_at`. For
Reclaimed, `prior_prepared_at` preserves the Prepared sequence and the final
transaction fills `state_commit_at`, exposed as `reclaimed_at`. Identity,
ordering, state tags, proof/grace order, clock values, and descriptor geometry
are validated. The descriptor itself remains the source of the upload id and
chunk count at staging time; the durable sweep record becomes the source after
Prepared, including after partial deletion.

## Worker order and recovery

After `Prepared` commits, the filesystem worker:

1. re-reads and validates the exact durable record;
2. relies on the durable Prepared key read by descriptor publication so a
   missing descriptor cannot be recreated while the worker is active;
3. deletes every recorded chunk idempotently;
4. deletes the descriptor last;
5. commits `Reclaimed` and removes obsolete control, quarantine, grace, and
   clock-attestation records atomically.

Deletion errors retain `Prepared`. Retry resumes from the recorded manifest;
missing chunks or a missing descriptor are treated as already completed steps,
not permission to guess a different manifest. A crash before `Prepared` removes
nothing. A crash after any object deletion but before completion resumes the
same sweep. `Drop` performs no cleanup.

Disabling reclamation after a crash prevents further worker progress and
retains the remaining bytes. Re-enabling the same qualified backend permits an
explicit resume.

TrineDB exposes this as a bounded caller-driven maintenance run for one exact
content identity. The higher layer discovers durable sweep/grace/quarantine
state before starting a new transition and returns classified outcomes rather
than inventing reader-drain or clock claims. A process exit or lost response
after Prepared is recovered by querying this record and calling resume; no
in-memory queue is required for correctness.

## Reuse of the same ContentId

`ContentId` identifies bytes, not a permanently deleted database entity. Once a
sweep is durably `Reclaimed`, a later independent upload of identical bytes may
publish a new descriptor and activity, atomically remove the tombstone, and
return the content to Active. File and Version history remain the only logical
file history; sweep state is physical lifecycle state only.

## Required evidence

- default configuration cannot stage or run a sweep;
- stale logical proof, early/unbound clock attestation, barrier/drain mismatch,
  token, lease, hold, or newer activity blocks `Prepared`;
- concurrent authoritative activity either commits first or loses to the
  Prepared fence without reviving it;
- each chunk failure and descriptor failure leaves a resumable record;
- kill/reopen after every worker step resumes the exact manifest;
- higher-layer retries discover proof, intent, quarantine, grace, Prepared, and
  Reclaimed commits without duplicating lifecycle state;
- descriptor deletion never precedes chunk deletion;
- completion is durable only after every object deletion succeeds;
- native reopen proves descriptor and bytes are absent after completion;
- a later upload of identical bytes succeeds and reads correctly;
- WASI, browser, and object-store modes return an explicit
  unsupported-capability error and retain bytes.
