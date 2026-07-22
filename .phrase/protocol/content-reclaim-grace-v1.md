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
- an original reclaim intent that still exactly binds the continuously durable
  quarantine;
- a valid sealed descriptor and no newer physical activity;
- no unexpired upload-token authority, read lease, or physical hold;
- the exact durable quarantine bound to that original intent, barrier, and
  drain attestation;
- either the original quarantine proof, or a fresh higher-layer proof whose
  stable verification coordinate is at or after `quarantined_at`.

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
recovered through `Db::content_reclaim_grace`. An exact retry with the same
quarantine and requested delay returns the original commit sequence.

If quarantine committed but grace did not, expiry of the original short-lived
proof must not strand the lifecycle. The higher layer issues a fresh exact proof
after re-reading current logical state. In the grace transaction, Trine KV
validates that the original intent and quarantine have remained continuously
bound, that the new proof is no older than quarantine, and that every physical
precondition still holds. Grace remains bound to the original quarantine token
and commit coordinate; neither record is rewritten and no second read-fence
interval is created. A different delay or quarantine is rejected rather than
silently rewriting history.

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
- `ObjectClient` now exposes provider version identity, and sweep v2 rejects any
  object-store qualification probe that observes one. It still cannot enumerate
  provider control-plane locks, retention, legal holds, backup, or replication;
  those remain host-retained external evidence.
- S3-compatible DELETE success is never sufficient by itself. Qualification
  probes protected path families, and the worker checks immediate absence after
  each content-object delete.

`content-reclaim-sweep-v2.md` defines that later final-validation protocol for
qualified native and explicitly qualified unversioned object-store backends. It
adds explicit trusted
clock/restart evidence, a fresh logical proof, repeated physical checks, and a
separately recoverable deletion state. Deadline passage by itself still changes
no authority, and unsupported backends retain bytes.

## Required evidence

- missing or mismatched quarantine blocks grace;
- logical activity after staging makes commit conflict;
- exact retry returns the original record and different delay fails;
- a fresh proof starts grace after quarantine committed without grace, while
  preserving the original quarantine identity and commit coordinate;
- native reopen preserves the record;
- refreshed object-store state exposes the same record without deleting any
  content descriptor or chunk;
- token/hold activity removes grace and quarantine atomically;
- malformed grace blocks query and revival;
- an old handle still reads after grace starts;
- no method checks deadline passage to delete content;
- no descriptor, chunk, replica, provider version, or physical byte is removed.
