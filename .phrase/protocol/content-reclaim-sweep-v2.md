# Content Reclaim Sweep v2

## Capability boundary

Physical deletion is default-off. V2 accepts either the independently
qualified native filesystem or a `QualifiedObjectStoreReclamation` returned by
the mandatory async provider probe. Memory, WASI, browser, versioned object
stores, and unverified object stores fail closed.

Object-store qualification has two independent inputs:

1. The host retains canonical non-secret control-plane evidence identifying the
   provider, bucket, exact database prefix, configuration revision, exclusive
   Trine ownership, disabled versioning/delete markers, and absence of matching
   locks, retention, legal holds, restore automation, replication, or lifecycle
   behavior that preserves or recreates deleted bytes.
2. Trine probes both content deletion path families, `content-v1/chunks` and
   `content-v1/domains`. Each probe uses conditional create and overwrite,
   rejects any provider version identity, deletes twice, and requires immediate
   HEAD, GET, and LIST absence.

The live probe cannot enumerate every provider control-plane rule. Its opaque
evidence digest makes that external trust boundary durable and auditable. The
host must invalidate the evidence before changing provider configuration or
namespace ownership. The returned capability also binds the exact database key
prefix internally and is rejected when used to open a different prefix.
Endpoint matching is never qualification.

## Authority chain

Final staging retains all v1 checks in one optimistic transaction: fresh exact
logical absence after grace, leased-only barrier, old-reader drain attestation,
continuous quarantine, trusted clock/restart evidence, valid descriptor, no
newer physical activity, and no token, lease, or physical hold. Provider
qualification adds no shortcut around those checks.

## Protected record

Bucket and key remain:

```text
bucket = "\x01trine-content-control\x01"
key = "sweep:" | StorageDomainId[16] | ContentId[33]
```

Value:

```text
"TRNCRSW2"[8]
| state[u8]
| StorageDomainId[16]
| ContentId[33]
| fresh ContentReclaimProofToken[49]
| fresh_verified_at[u64 be]
| fresh_proof_expiry_unix_ms[u64 le]
| quarantined_at[u64 be]
| grace_started_at[u64 be]
| ContentAccessBarrierId[16]
| barrier_enforced_at[u64 be]
| ContentReaderDrainAttestationId[16]
| ContentReclaimClockAttestationId[16]
| ContentReclaimClockCoordinatorId[16]
| ContentReclaimClockEvidenceDigest[33]
| clock_observed_at_unix_ms[u64 le]
| UploadId[16]
| chunk_count[u64 le]
| backend[u8]                         # 0 native, 1 object store
| provider_evidence[33]              # zero for native
| prior_prepared_at[u64 be]
| state_commit_at[u64 be]
```

Prepared persists the exact backend evidence. Resume compares it with the
currently configured qualification before any delete. A missing, disabled, or
different capability returns an unsupported-backend error and retains bytes.

## Worker and recovery

The worker re-reads Prepared, deletes each recorded chunk, deletes the
descriptor last, and then commits Reclaimed while removing obsolete lifecycle
records. Native deletion retains its directory durability requirements.
Object-store deletion additionally performs an immediate metadata/read absence
check through HEAD, GET, and LIST for every key. It also rejects a provider
version observed immediately before DELETE. DELETE success without three-way
absence is corruption, not completion.

Missing keys are idempotent completed steps. Any request error, visible object,
malformed state, evidence mismatch, conflict, process exit, or uncertain state
leaves Prepared. Retry resumes the same upload/chunk manifest; it cannot choose
a different provider qualification. Reclaimed remains physical lifecycle state,
not a second File history.

The per-object cloud cost is deliberately higher than an ordinary key delete:
one pre-delete HEAD, one DELETE, and one post-delete HEAD, GET, and LIST. The
bounded higher-layer runner controls how many objects are processed in one
maintenance pass.

## Required evidence

- default-off and backend mismatch retain bytes;
- provider version identity and sticky deletion fail qualification;
- qualification probes both protected path families;
- Prepared persists provider evidence and rejects changed evidence after reopen;
- partial chunk/descriptor deletion resumes without reordering;
- every cloud delete is followed by absence verification;
- MinIO and R2 pass isolated qualification, Prepared close/reopen, full sweep,
  and final content-object absence;
- versioned or control-plane-unverified deployments receive no generic claim.
