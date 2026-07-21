# Content Reader Drain Attestation v1

## Purpose and trust boundary

This protocol durably records that a trusted deployment coordinator has
established the end of every content reader that could have opened before one
irreversible leased-only barrier. It is the missing deployment boundary between
the forward fence in `content-access-barrier-v1.md` and any future grace or
physical sweep.

Trine KV verifies the barrier identity, protected commit coordinates, record
format, idempotency identity, and durable publication. It cannot observe native
process supervisors, remote request streams, credential issuers, or direct
object-store credentials. The attestation therefore proves that Trine KV
durably recorded a trusted coordinator claim; it is not an independently
verified storage proof and is not deletion authority.

Elapsed time, logical absence, lack of new unleased opens, the writer lease, and
the barrier alone are not accepted reader-drain evidence.

## Public identities and claims

`ContentReaderDrainAttestationId` is a caller-retained, versioned 16-byte
identity. Byte zero is format version `1`. It closes commit-before-response:
retrying the same identity and exact options returns the original durable
record. A different identity or claim cannot replace the first attestation for
the barrier.

`ContentReaderDrainCoordinatorId` is an opaque 16-byte coordinator identity.
Trine KV does not derive user, service, tenant, Principal, or authorization
meaning from it.

`ContentReaderDrainEvidenceDigest` is 33 bytes: algorithm tag `1` followed by
SHA-256 over:

```text
"trine-content-reader-drain-evidence-v1" | canonical external evidence bytes
```

The digest is an audit commitment, not a signature. The coordinator retains the
canonical evidence outside Trine KV and should bind at least the deployment,
`StorageDomainId`, `ContentAccessBarrierId`, admitted process or credential
epoch, completion observation, and issuer identity.

`ContentReaderDrainKind` has three v1 claims:

- `DomainBootstrap`: the barrier and protected coordinate completed before the
  domain admitted its first read-capable process, credential, or content read;
- `NativeProcessSetRestarted`: after the barrier, every process capable of
  retaining a pre-barrier handle stopped, and the admitted process set restarted
  under code that enforces the leased-only boundary;
- `RemoteCredentialEpochRetired`: all pre-barrier request streams ended and
  every credential capable of bypassing the current leased-only entry point was
  expired or revoked.

The enum records which deployment proof was used; it does not make the claim
self-verifying.

## Protected record

Bucket: `\x01trine-content-control\x01`

Key:

```text
"drain:"[6] | StorageDomainId[16]
```

Value:

```text
"TRNCRDA1"[8]
| StorageDomainId[16]
| ContentAccessBarrierId[16]
| ContentReaderDrainAttestationId[16]
| ContentReaderDrainKind[u8]
| ContentReaderDrainCoordinatorId[16]
| ContentReaderDrainEvidenceDigest[33]
| barrier_enforced_at_commit_seq[u64 big-endian]
| attested_at_commit_seq[u64 big-endian]
```

`attested_at_commit_seq` is filled with the transaction's final local commit
sequence. Both coordinates must be nonzero and the attestation sequence must be
at or after the barrier sequence. Unknown identity, digest, or kind versions,
wrong domain, malformed length, or invalid coordinates fail closed.

## Publication algorithm

`Db::attest_content_reader_drain` accepts the typed `ContentAccessBarrier`
returned by `Db::enforce_content_leased_only`, a caller-retained attestation id,
and exact options:

1. reject a read-only database;
2. directly read the content-backend barrier and require the same domain and
   barrier identity;
3. read the protected barrier coordinate in one optimistic transaction and
   require the same identity and enforcement sequence;
4. read the one attestation key for the domain;
5. return the original record for an exact retry, reject different claims, or
   stage the new record with its final commit sequence;
6. retry bounded optimistic conflicts.

The direct barrier is already irreversible, so one permanent attestation per
domain is sufficient. There is no release, expiry, rollback, or replacement
API. If an operator cannot support the claim with retained evidence, it must not
call the method.

`Db::content_reader_drain_attestation` reads the protected record. An
object-store read-only handle may need `refresh_object_store` because, unlike
the forward fence, this audit record is ordinary protected KV state and does not
control new read admission.

## Native evidence contract

The existing native `LOCK` file is a writer lease. Read-only database instances
do not hold it, and a `ContentHandle` can outlive the database handle that
opened it. Consequently, acquiring or retaining the writer lock does not prove
reader drain.

For `NativeProcessSetRestarted`, the deployment coordinator must:

1. complete the leased-only barrier and protected coordinate;
2. stop admission of old processes;
3. stop every process that could hold a pre-barrier `ContentHandle`, including
   workers and tools using a read-only `Db`;
4. verify those processes exited rather than merely closing a database handle;
5. admit only the restarted process set that observes the direct barrier;
6. retain the process inventory and observations committed by the evidence
   digest before attesting.

A future native shared-reader-session mechanism may produce stronger internal
evidence, but it cannot retroactively account for binaries that never joined
that mechanism. This v1 protocol does not claim such a mechanism exists.

## Remote and object-store evidence contract

For `RemoteCredentialEpochRetired`, the authority coordinating database/vault
access must:

1. publish the barrier before stopping old admission;
2. end every pre-barrier request stream and background reader;
3. make every old read credential non-renewable, then expire or revoke it;
4. rotate or revoke any direct S3/R2/object-store credential that could bypass
   the authority and continue reading immutable content keys;
5. verify no old session or bypass credential remains usable;
6. retain the session/credential epoch evidence committed by the digest before
   attesting.

An application gateway cannot make this claim when clients can bypass it with
long-lived direct database or bucket credentials. A time-to-live is useful only
when the coordinator proves all relevant credentials are bounded by that
lifetime and cannot be renewed across the barrier.

## Bootstrap evidence contract

`DomainBootstrap` is the preferred path for a new storage domain. The
coordinator establishes the leased-only barrier and protected coordinate before
issuing any read-capable handle or credential for that domain. Its evidence must
bind that ordering. This avoids a legacy reader population; it does not infer
emptiness by listing object-store keys or waiting for a grace duration.

## Future sweep boundary

This protocol does not change `Transaction::stage_content_reclaim_intent`:
intent remains harmless coordination state and may be recorded before external
drain is attested. `content-quarantine-v1.md` now consumes the matching
attestation during a second transactional logical/physical recheck and blocks
new leased reads without deleting bytes. A future sweep must still independently
and freshly require:

- the direct leased-only barrier and matching protected coordinate;
- a valid attestation bound to that exact barrier;
- a second exact logical absence proof;
- no active upload authority, read lease, physical/provider hold, or newer
  content activity;
- completed quarantine and the separately specified grace boundary;
- representation and replica-specific delete preconditions.

No current method consumes the attestation as deletion authority, starts grace,
or deletes bytes.

## Required evidence

- an exact retry returns the original attestation and commit coordinate;
- a different id or claim cannot replace a durable attestation;
- a missing, different, uncoordinated, or malformed barrier/attestation fails
  closed;
- a pre-barrier handle remains readable after a caller records an attestation,
  proving the storage record cannot verify the external claim;
- the record survives native reopen;
- an object-store read-only instance observes the record after ordinary KV
  refresh;
- no test or documentation treats the record, barrier, or elapsed time as
  physical deletion authority.
