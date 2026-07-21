# Content Reclaim Grace v1

## Purpose and non-authority boundary

This protocol records one durable wall-clock scheduling observation while exact
content remains quarantined. It gives a worker a crash-recoverable
`not_before_unix_ms` coordinate without deleting, relocating, or hiding any
descriptor, chunk, replica, or provider version.

V1 grace is not deletion authority. The host wall clock can jump forward or
backward and cannot prove elapsed real time across restart. Reaching the stored
deadline is therefore only a reason to attempt a later, independently specified
final validation. No current method interprets the deadline as permission to
delete.

## Preconditions and transaction ownership

The higher layer repeats its exact proof, candidate, liveness, and retained-root
generation checks in the same optimistic transaction as
`Transaction::stage_content_reclaim_grace`.

Trine KV then freshly requires:

- an unexpired proof valid for the transaction read sequence;
- the coordinated leased-only barrier and matching reader-drain attestation;
- the exact accepted reclaim intent;
- a valid sealed descriptor and no newer physical activity;
- no unexpired upload-token authority, read lease, or physical hold;
- the exact durable quarantine bound to that proof, intent, barrier, and drain
  attestation.

Every protected read joins optimistic conflict validation. A concurrent logical
touch, token, lease, hold, or revival prevents the grace record from committing.

## Protected record

Bucket: `\x01trine-content-control\x01`

Key:

```text
"grace:"[6] | StorageDomainId[16] | ContentId[33]
```

Value:

```text
"TRNCRGR1"[8]
| StorageDomainId[16]
| ContentId[33]
| ContentReclaimProofToken[49]
| quarantined_at_commit_seq[u64 big-endian]
| requested_observation_delay_ms[u64 little-endian]
| observed_at_unix_ms[u64 little-endian]
| not_before_unix_ms[u64 little-endian]
| grace_started_at_commit_seq[u64 big-endian]
```

`not_before_unix_ms` must equal the checked sum of the observation and requested
delay. The observation happens before commit, so the requested delay is not a
minimum interval measured from durable visibility. The final commit sequence is
filled by the transaction. Unknown formats, identity mismatch, zero values,
overflow, or ordering mismatch fail closed.

## Start, retry, and recovery

Starting grace requires a delay of at least one millisecond. The wall-clock
observation and deadline are staged with the record; commit-before-response is
recovered through `Db::content_reclaim_grace`. While the authorization remains
unexpired, an exact retry with the same quarantine and requested delay returns
the original commit sequence. After expiry, the query API is the recovery path.
A different delay or quarantine is rejected rather than silently rewriting
history.

Native reopen and refreshed object-store state preserve the record. There is no
best-effort cleanup and `Drop` performs no asynchronous work.

## Revival

Upload-token publication or consumption and migration, backup, repair,
provider, or administrative hold activity validate the exact quarantine/grace
pair, delete both records, and publish Active content control in one
transaction. Malformed or mismatched protected state blocks revival.

Leased reads are not revival authority. Quarantine remains the read fence for
the entire grace interval.

## Clock and provider limits

- Clock rollback delays a later attempt but does not corrupt the record.
- Clock advancement, NTP steps, VM resume, and restart downtime are not proved
  by this record.
- `ObjectClient` currently exposes key delete, size, and ETag, but no provider
  version id, delete marker, object lock, retention deadline, or legal hold.
- S3/R2 adapter success therefore cannot yet prove that every provider version
  was removed or retained as intended.

A future final-validation protocol must name its trusted clock contract and
provider/version capabilities, obtain a fresh logical proof, repeat every
physical check, and record a separately recoverable deletion state. Until then,
deadline passage changes no authority.

## Required evidence

- missing or mismatched quarantine blocks grace;
- logical activity after staging makes commit conflict;
- exact retry returns the original record and different delay fails;
- native reopen preserves the record;
- refreshed object-store state exposes the same record without deleting any
  content descriptor or chunk;
- token/hold activity removes grace and quarantine atomically;
- malformed grace blocks query and revival;
- an old handle still reads after grace starts;
- no method checks deadline passage to delete content;
- no descriptor, chunk, replica, provider version, or physical byte is removed.
