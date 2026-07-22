# Current Phase

## Qualified object-store content reclamation for TrineDB Phase 23

**Status:** Complete.

### Implemented boundary

- Physical deletion remains default-off. Native filesystem qualification is
  unchanged; object storage now requires a capability returned by the async
  `qualify_object_store_reclamation` probe.
- Qualification binds a host-retained evidence digest for the exact provider,
  bucket, database prefix, configuration revision, namespace ownership,
  versioning, locks, retention, legal hold, backup/replication, and lifecycle
  boundary. Endpoint names never imply support.
- Live probes exercise both `content-v1/chunks` and `content-v1/domains`, require
  no provider version after create/overwrite, and require immediate HEAD, GET,
  and LIST absence after idempotent delete.
- Prepared sweep v2 stores the backend and provider evidence digest. Missing or
  changed evidence after reopen rejects resume and retains remaining bytes.
- Each cloud delete is followed by an immediate existence check. Chunks remain
  before descriptor in the deletion order; any error retains Prepared.

### Verification evidence

- Deterministic tests reject visible provider versions and successful-but-sticky
  deletes.
- Object-store lifecycle test proves evidence mismatch rejection, injected
  descriptor-delete failure, Prepared persistence, retry, final absence, and
  no revival through leased open.
- MinIO control plane reported an unversioned bucket with locking unsupported;
  the full qualification, Prepared close/reopen, sweep, and provider-prefix
  absence test passed.
- R2 ran the same isolated full lifecycle through Infisical dev credentials and
  passed in 50.25 seconds. Data-plane metadata reported no object versions and
  both protected path probes plus actual chunk/descriptor deletion were
  immediately absent.

### Completion gate

- The final MinIO lifecycle passed in 1.44 seconds and the final real R2
  lifecycle passed in 55.14 seconds after actual deletion was tightened to
  HEAD, GET, and LIST absence.
- All-feature regression passed 515/518 library tests with three intentional
  ignores, every integration target, 31 doctests, strict Rustdoc, and all-target
  wildcard-import Clippy.
- Provider control-plane evidence remains an explicit host responsibility. The
  available R2 credentials are data-plane credentials and cannot enumerate
  Cloudflare bucket-lock configuration.

### Out of scope

- Versioned bucket cleanup, delete-marker traversal, or historical-version
  deletion.
- Bypassing provider locks, retention, legal hold, backup, or replication.
- Automatic qualification from an S3-compatible endpoint.
- WASI and browser reclamation.
