# Cloud Content Reclamation Runbook

## Purpose

Enable the default-off content sweep for one exact unversioned S3-compatible
database prefix without treating endpoint compatibility as deletion safety.

## Qualification inputs

Retain a canonical, non-secret evidence document outside the database. It must
identify the provider, account, bucket, exact database prefix, configuration
revision, and Trine ownership boundary, and record the control-plane checks for:

- disabled object versioning and delete markers;
- no bucket lock, retention rule, legal hold, or restore-on-delete automation;
- no replication, backup, or lifecycle process that can preserve or recreate
  keys under the prefix;
- credentials permitted to create, overwrite, HEAD, GET, LIST, and DELETE both
  `content-v1/chunks` and `content-v1/domains`.

Do not include credentials, tokens, or customer content in the evidence
document. Hash its canonical bytes with
`ObjectStoreReclamationEvidenceDigest::for_bytes`, construct an
`ObjectStoreReclamationAttestation`, and call
`qualify_object_store_reclamation` with the exact database prefix. Supply the
returned capability through `ContentReclamationMode::QualifiedObjectStore` when
opening that prefix.

## Change and restart procedure

Before changing bucket versioning, locks, retention, replication, backup,
lifecycle rules, credentials, provider, bucket, or prefix ownership:

1. Stop maintenance workers and wait for in-flight sweep calls to return.
2. Make the provider change.
3. Create and retain a new evidence document and digest.
4. Run qualification again against both protected path families.
5. Reopen with the new capability only after the probe succeeds.

An existing Prepared sweep intentionally refuses to resume with the new digest.
Investigate and explicitly resolve that state; do not copy the old digest into
new evidence.

## Failure handling

Any provider request error, observed version, visible key after DELETE, evidence
mismatch, or process exit leaves the sweep Prepared. Re-run provider and
namespace checks before retrying. Do not manually mark a sweep Reclaimed or
delete its protected control record.

The qualifier cannot enumerate provider-specific control-plane policy through
the generic S3 data plane. If the deployment cannot retain trustworthy evidence
for those controls, keep cloud reclamation disabled.
