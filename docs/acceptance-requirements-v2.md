# Acceptance Requirements v2

This catalog is the source for the next acceptance-test expansion. It is
written before the feature files and step definitions. Requirements are
derived from public contracts and normative protocols; source coverage reports,
private helper names, storage filenames, and current control flow are not
inputs.

## Interpretation rules

- A requirement describes caller-observable state or a typed public outcome.
- Durable means that the observation is repeated after closing and reopening
  the database, unless the requirement explicitly concerns a live handle.
- Backend-neutral requirements use the same scenario and expected result for
  native files and a qualified S3-compatible object store.
- A capability whose public API is deliberately synchronous is exercised by a
  separate durable-native profile. The profile distinction is part of the
  public API shape, not a test exception.
- Time-dependent requirements use deadlines with a generous separation between
  the "active" and "expired" observations. They do not infer expiry from an
  implementation counter.
- Internal record layouts, WAL/manifest names, fault-hook identifiers, and
  coverage percentages never appear in Gherkin vocabulary.

## Durable branch requirements

Sources: `docs/branching.md`, public `Db`/`Branch` lifecycle documentation, and
the V1 read-version and checkpoint contracts.

| ID | Requirement |
| --- | --- |
| `REQ-BRANCH-001` | A branch reads the root lineage exactly as it existed at the selected fork version even after the root advances. |
| `REQ-BRANCH-002` | Branch writes and deletions affect the branch view without changing the root lineage. |
| `REQ-BRANCH-003` | A named branch, its divergent writes, and its fork view survive database reopen. |
| `REQ-BRANCH-004` | A branch range is ordered and applies leaf writes and deletions over the frozen parent view. |
| `REQ-BRANCH-005` | A child branch reads through its parent and root, while later parent writes remain invisible to the child. |
| `REQ-BRANCH-006` | A branch with a live child cannot be deleted; deleting child before parent succeeds. |
| `REQ-BRANCH-007` | Deleting a branch releases its name and history promise; recreating the name starts a new generation without old divergent data. |
| `REQ-BRANCH-008` | A branch fork remains readable under aggressive root-history retention while the branch exists. |
| `REQ-BRANCH-009` | Listing and lineage inspection expose only active branches and the exact public parent/fork relationship. |

The durable branch API currently exposes synchronous branch data operations, so
these requirements belong to the durable-native acceptance profile. Extending
the public branch data API to remote/browser asynchronous storage requires a
separate API decision and must not be simulated by an in-memory backend.

## Durable upload requirements

Sources: public `ContentUpload`, `ContentUploadResume`,
`ContentUploadOptions`, and upload-maintenance lifecycle documentation.

| ID | Requirement |
| --- | --- |
| `REQ-UPLOAD-001` | An open upload resumes after database reopen at its exact confirmed byte length and can seal the concatenated bytes. |
| `REQ-UPLOAD-002` | A sealed upload identity resumes as the same sealed result and never becomes writable again. |
| `REQ-UPLOAD-003` | An aborted upload cannot be resumed or sealed and never publishes content. |
| `REQ-UPLOAD-004` | Expected length and expected content identity are checked before publication; rejection leaves the future identity unavailable. |
| `REQ-UPLOAD-005` | Physical quota accounts for durable staged bytes, rejects an over-budget write before publication, and releases reservation after completed abort cleanup. |
| `REQ-UPLOAD-006` | Upload maintenance reports and removes only lifecycle states older than the caller-supplied boundary; a current open upload remains resumable. |

## Content read-lease requirements

Source: `.phrase/protocol/content-read-lease-v1.md`.

| ID | Requirement |
| --- | --- |
| `REQ-LEASE-001` | A leased open returns verified immutable bytes only after durable lease publication. |
| `REQ-LEASE-002` | Clones share one lease identity and observe a successful renewal deadline. |
| `REQ-LEASE-003` | Expiry is terminal: reads and renewal fail explicitly and never revive the lease. |
| `REQ-LEASE-004` | A read-only database cannot create a durable read lease. |

## Physical-hold requirements

Source: `.phrase/protocol/content-physical-hold-v1.md`.

| ID | Requirement |
| --- | --- |
| `REQ-HOLD-001` | An until-released physical hold survives reopen and resumes only for its exact identity and owner. |
| `REQ-HOLD-002` | Release is durable and idempotent; a released identity cannot be resumed, renewed, or reacquired. |
| `REQ-HOLD-003` | A wrong owner cannot resume or release another owner's hold. |
| `REQ-HOLD-004` | An expiring hold can extend but not shorten its deadline; an expired hold cannot be resumed or renewed. |
| `REQ-HOLD-005` | Every public hold class supplies the same physical-retention guarantee. |

## Leased-only access requirements

Source: `.phrase/protocol/content-access-barrier-v1.md`.

| ID | Requirement |
| --- | --- |
| `REQ-ACCESS-001` | Before enforcement, compatible unleased content opens return verified bytes. |
| `REQ-ACCESS-002` | After enforcement, every new unleased open is rejected with `ContentLeaseRequired`, including after reopen. |
| `REQ-ACCESS-003` | Leased opens remain available after enforcement. |
| `REQ-ACCESS-004` | A handle opened before enforcement remains fixed to its immutable bytes; the barrier is not reader-drain evidence. |
| `REQ-ACCESS-005` | Enforcement is irreversible and an exact retry returns the already-established barrier identity. |

## Recovery and API-parity requirements

Sources: V1 §§4, 10, 11, 25; the public asynchronous API contract; and
`docs/durability.md`.

| ID | Requirement |
| --- | --- |
| `REQ-RECOVERY-001` | Confirmed cross-bucket commits recover atomically after close/reopen without requiring flush. |
| `REQ-RECOVERY-002` | A failed or rejected operation cannot become visible after reopen. |
| `REQ-RECOVERY-003` | Maintenance followed by repeated reopen preserves the same accepted latest and retained historical views. |
| `REQ-ASYNC-001` | Asynchronous point, batch, range-delete, snapshot, flush, and compaction operations have the same durable observable contract as their synchronous native counterparts. |
| `REQ-ASYNC-002` | Dropping an unpolled asynchronous mutation has no durable effect. |
| `REQ-ASYNC-003` | Once an asynchronous mutation is accepted by polling, dropping the caller future cannot turn a confirmed result into partial publication. |

## Executable scenario set

The implementation intentionally chooses behavior spanning distinct state
transitions rather than maximizing line counts:

1. branch fork isolation and ordered overlay;
2. durable branch reopen and generation replacement;
3. nested branch freeze and parent-delete constraint;
4. upload resume and sealed-idempotency across reopen;
5. upload abort non-publication and physical-quota release;
6. leased-only enforcement with pre-barrier and leased handles;
7. lease clone renewal and terminal expiry;
8. physical-hold reopen/resume, wrong owner, renewal, terminal expiry, and
   durable release;
9. upload maintenance across the exclusive time boundary and every lifecycle
   class;
10. accepted and rejected cross-bucket recovery, repeated maintenance reopen,
    and an unpolled asynchronous mutation.

`REQ-ASYNC-001` is traced across the existing backend-neutral point, batch,
range-delete, snapshot, flush, and compaction scenarios. `REQ-ASYNC-003` is
exercised by the persistent asynchronous API integration test because its
single-poll boundary cannot be expressed as application vocabulary without
introducing an implementation-level scheduling hook.
