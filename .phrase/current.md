# Current Phase

## Consumer-driven non-authorizing reclaim grace — 2026-07-21

Quarantine closes new leased reads, but a crash-recoverable worker also needs a
durable earliest-time scheduling coordinate. This slice records the host
wall-clock observation, requested delay, derived `not_before`, exact quarantine,
and final commit sequence only after the higher layer and Trine KV repeat all
logical and physical checks in one optimistic transaction.

This record is deliberately not elapsed-time proof. The observation happens
before commit, wall clocks can jump, and no local monotonic clock survives
restart. Passing `not_before` changes no authority and invokes no delete path.
Upload/token and physical-hold activity removes grace and quarantine while
returning control to Active atomically; malformed or mismatched state fails
closed.

Backend boundary receipt: the owned operation is `content_reclaim_grace`.
Native storage and `ObjectClient` are backends. `ObjectClient` currently exposes
key delete, size, and ETag but no provider version id, delete marker, object
lock, retention, or legal hold. Provider-version deletion therefore remains
outside this slice and outside the current authority boundary.

Stable layout, retry/recovery rules, clock limits, revival semantics, and future
delete exclusions are in `.phrase/protocol/content-reclaim-grace-v1.md`.

Acceptance result: five focused grace tests pass. The complete all-feature
suite passes with 507 library tests (505 passed, two ignored) plus every
integration target. Strict all-target/all-feature Clippy with wildcard imports
denied, strict Rustdoc, 29 doctests, formatting, explicit-import, and diff gates
pass.

## Consumer-driven crash-safe content quarantine — 2026-07-21

Reader drain now has a durable deployment attestation, but intent and
attestation alone still allowed a new leased read to appear after the last lease
scan. This slice adds one exact-content quarantine record that is staged only
after the higher layer repeats its logical proof/liveness/root-generation checks
in the same optimistic transaction as Trine KV's barrier, attestation,
descriptor, activity, token, lease, and physical-hold rechecks.

A committed quarantine blocks new leased opens and renewal with
`ContentQuarantined { quarantined_at }`. It keeps the descriptor and all bytes
intact. Token/attachment or migration/backup/repair/provider/administrative hold
activity validates and removes quarantine while returning control to Active in
one transaction; a leased read is not revival authority. Concurrent logical or
physical activity either blocks the transition or makes its commit conflict.

The record binds the accepted proof, intent sequence, leased-only barrier, and
reader-drain attestation, and carries its final commit sequence. Exact retry is
idempotent; native reopen retains the fence; malformed state fails closed.

This slice records no grace deadline and invokes no physical deletion. Future
grace/sweep work must obtain a fresh logical proof and repeat every physical
check, then separately prove representation, replica, provider-hold, clock, and
crash behavior. Stable layout and transition semantics are in
`.phrase/protocol/content-quarantine-v1.md`.

Acceptance result: six focused quarantine tests pass. The complete all-feature
suite passes with 502 library tests (500 passed, two ignored) plus every
integration target. Strict all-target/all-feature Clippy with wildcard imports
denied, strict Rustdoc, 28 doctests, formatting, explicit-import, and diff gates
pass.

## Consumer-driven reader-drain attestation boundary — 2026-07-21

The leased-only barrier closes new unleased opens but cannot observe native
read-only processes, old `ContentHandle` values, remote request streams, or
direct object-store credentials. The native writer `LOCK` is not reader
coordination, and a timeout is not proof for an unbounded handle.

This slice adds one permanent, barrier-bound
`ContentReaderDrainAttestation` per `StorageDomainId`. A trusted deployment
coordinator selects `DomainBootstrap`, `NativeProcessSetRestarted`, or
`RemoteCredentialEpochRetired`, supplies an opaque coordinator identity and an
algorithm-tagged digest of externally retained evidence, and keeps a
caller-generated id for exact retry. Trine KV verifies the direct barrier and
protected coordinate, persists the exact claim with its final commit sequence,
and rejects conflicting replacement or malformed state.

This is deliberately an attestation, not an internal drain proof. A focused
test records a claim while retaining an old handle and proves that handle still
reads. Native evidence therefore requires every capable old process to exit;
remote evidence requires every old session and bypass credential, including
direct S3/R2 credentials, to end or be revoked. Bootstrap before first read is
the preferred new-domain path.

Reclaim intent remains unchanged because it does not hide or delete bytes. A
future sweep must freshly require the matching attestation plus exact logical
absence, no token/lease/activity/hold blocker, quarantine, grace, and
representation-specific checks. No grace or physical deletion is implemented.

The stable byte layout, trust boundary, native/remote evidence requirements,
and future sweep gate are in
`.phrase/protocol/content-reader-drain-attestation-v1.md`.

Acceptance result: three focused reader-drain tests plus the object-store
visibility test pass. The complete all-feature suite passes with 496 library
tests (494 passed, two ignored) plus every integration target. Strict
all-target/all-feature Clippy with wildcard imports denied, strict Rustdoc, 27
doctests, formatting, and diff checks pass.

## Consumer-driven persisted leased-only forward fence — 2026-07-20

Trine KV now has one irreversible leased-only access barrier per
`StorageDomainId`. Compatible `open_content` checks a small record directly in
the shared content backend before reading a descriptor. That bypasses stale
ordinary-KV views: already-open native read-only handles and object-store
readers that have not called `refresh_object_store` observe the barrier on their
next content open. `open_content_leased` bypasses only this compatibility check
and still commits its exact durable lease and Active control state before
returning.

The writer publishes the backend barrier first, then records the same versioned
identity and final commit sequence in the protected content-control bucket. A
crash between those writes is restrictive but recoverable: new unleased opens
fail, reclaim intent returns `LeasedOnlyBarrierUncoordinated`, and retry adopts
the backend identity and completes the coordinate. A protected coordinate is
never visible before the backend fence. The API exposes no reversal.

Reclaim-intent staging now requires a valid direct barrier and matching
coordinate. Compatible mode returns `UnleasedAccessAllowed`; the coordinate
point read joins the transaction read set. Existing token, lease, physical-hold,
activity, proof, and descriptor checks remain unchanged.

Safety result: this barrier proves only that no new unleased open linearizes
after it. A pre-barrier handle remains readable indefinitely, so reader drain,
grace, and physical deletion remain unimplemented. Native and remote
deployments need an explicit reader-session or external-coordination proof next.

Acceptance result: 30 focused content tests pass, including native and stale
object-store read-only visibility, compatible and uncoordinated reclaim
blockers, interrupted recovery, identity adoption, malformed fail-closed state,
old-handle survival, and leased access after fencing. The complete all-feature
suite passes with 492 library tests (490 passed, two ignored) plus every
integration target. Strict Clippy/import, Rustdoc, 26 doctests, formatting, and
diff gates pass. Stable ownership, record layouts, ordering, and exclusions are
in `.phrase/protocol/content-access-barrier-v1.md`.

## Consumer-driven unified physical content holds — 2026-07-20

Migration, backup, repair, provider, and administrative work now uses one
durable exact-content `ContentPhysicalHold` registry. Holds have a generated,
caller-retained versioned identity, opaque owner, typed class, and either an
exclusive expiry or explicit-release lifetime. Acquisition and renewal publish
Active content control in the same optimistic transaction; release advances
the exact record to a durable Released tombstone; explicit holds survive native
reopen through owner-checked
resume. Drop performs no asynchronous I/O.

Acquisition is idempotent by caller-retained HoldId. An exact retry after a lost
response returns the original active record; owner/class/lifetime mismatch
fails and an expired identity cannot be revived. Until-released holds therefore
do not become undiscoverable permanent blockers at commit-before-response.
Released HoldIds also cannot be reacquired by a delayed request.

Reclaim-intent acceptance exact-range reads the hold registry and returns a
typed blocker containing hold id, class, and optional deadline. Concurrent
hold acquisition conflicts with staged intent; later acquisition or renewal
supersedes older intent by returning the control record to Active. Expired
records are inert, cannot be revived, and malformed protected state fails
closed.

Safety result: the known storage actors now share one complete durable hold
mechanism, but compatible unleased `open_content` remains fundamentally
unobservable across concurrent read-only processes. Automated physical
deletion therefore remains disabled wherever that path is permitted. A future
delete protocol first needs a persisted leased-only mode plus proved reader
drain or external coordination; grace alone cannot bound an unleased handle.

Acceptance result: 25 focused content tests pass, including all five hold
classes, exact acquisition retry, Released non-revival, release/idempotency,
expiry/renewal, owner mismatch, native resume, intent races, post-intent
supersession, and malformed-state failure. The full
all-feature suite passes with 487 library tests (485 passed, two ignored) and
all integration targets. Strict Clippy/import, Rustdoc, 25 doctests,
formatting, and diff gates pass. Stable ownership and byte layouts are in
`.phrase/protocol/content-physical-hold-v1.md`.

## Consumer-driven atomic content reclaim intent — 2026-07-20

TrineDB now produces exact, short-lived ReclaimProofs, but Trine KV previously
had no atomic physical boundary for accepting one. The existing exact-content
lease scan was deliberately only a conservative precheck: a new lease could
race immediately after it, UploadToken authority was keyed by bearer hash
rather than ContentId, and no per-content physical activity high-water existed.

Current slice: add durable ReclaimIntent coordination without making bytes
unavailable. A protected per-content control record holds Active or intent
state. Seal, token consumption, leased open, and durable renewal write Active
with their final commit sequence. Seal also publishes a ContentId-ordered token
authority index; consumption removes it. `Transaction::stage_content_reclaim_intent`
checks descriptor, proof expiry and `S`, physical high-water, available token
authority, and exact read leases before staging intent in the caller's same
optimistic transaction.

Concurrency result: the control point read and token/lease range reads join the
transaction read set. Concurrent token or lease activity either appears before
the check and blocks it, or makes commit conflict. Later leased/token activity
replaces old intent with Active. Exact retry returns the original acceptance
sequence. Typed `ContentReclaimBlocker` values distinguish expired proof,
newer physical activity, upload authority, and read lease.

Safety boundary: intent neither starts grace nor deletes or hides content.
Compatible unleased `open_content` remains unsafe against future deletion, and
additional migration/backup/repair/provider/administrative holds do not yet
exist. They must join the fence before a deletion protocol is possible.

Acceptance result: 20 content tests pass, including five new lifecycle tests
for native reopen, idempotency, token/lease blockers, lease conflict, later
lease/token activity, proof expiry, and malformed records. The complete suite
passes with 482 library tests (480 passed, two ignored) and every integration
target. Strict all-target/all-feature Clippy, Rustdoc, 24 doctests, formatting,
and diff checks pass. Stable ownership and byte layouts are recorded in
`.phrase/protocol/content-reclaim-intent-v1.md`.

## Consumer-driven Branch metadata symmetry — 2026-07-20

TrineDB's protected logical Branch-root publication needs the physical fork and
checkpoint established before its own logical record becomes visible. Root
Branch creation and deletion already had async metadata paths, but nested
Branch creation and lineage lookup did not. The upper layer also had to decode
Trine KV's private registry bytes to read lineage asynchronously.

Current slice: add `create_branch_from_async` and `branch_info_async` with the
same fork, parent, retention, and error semantics as their synchronous peers.
The async methods use async checkpoint and registry operations throughout, so
object-storage and browser callers do not cross into a synchronous metadata
path. A checkpoint-only interrupted create remains invisible and may be retried.

Audit correction: an in-memory existing-name checkpoint write used
`HashMap::insert` before returning `CheckpointAlreadyExists`, which replaced the
old pin despite the error. Checkpoint creation now checks existence before
insertion. Branch creation verifies that an orphan checkpoint pins the exact
requested fork before publishing a registry entry.

Acceptance result: 17 focused Branch tests pass, including nested lineage and a
regression proving a mismatched orphan checkpoint neither changes its old pin
nor publishes a Branch. The complete all-feature suite passes with 477 library
tests (475 passed, two ignored) and every integration target. Strict Rustdoc, 23
doctests, locked all-target/all-feature check, Clippy/import, formatting, and
diff gates pass. This slice changes no registry, checkpoint, WAL, manifest, or
Branch data format.

## Consumer-driven content read lease — 2026-07-20

TrineDB's accepted FileHandle contract fixes one immutable content identity but
the current Trine KV `ContentHandle` only caches a descriptor. It carries no
lease identity or expiry, so future relocation or reclamation would have no
authoritative way to distinguish an active read from an unreachable object.

Current slice: add a compatible leased-open path without changing existing
unleased content reads or physical layout. A content lease has an opaque caller
owner, generated lease identity, explicit Unix-millisecond expiry, and an exact
`(StorageDomainId, ContentId)` binding. Lease records are durable protected KV
state keyed for bounded per-content inspection. Handle clones share one lease;
reads fail closed after expiry; renewal is explicit and cannot revive an expired
lease. Drop performs no asynchronous I/O and simply allows the durable lease to
expire.

Out of scope: physical relocation, content reclamation, automatic renewal,
higher-layer Principal/Policy decisions, background expired-record cleanup, and
changing the existing `open_content` behavior.

Acceptance gate: leased open returns a verified handle with stable content and
lease identities; clone shares expiry; valid renewal extends it; wrong identity,
expired renewal/read, malformed state, and zero/overflow TTL fail with typed
errors; an internal active-lease probe sees only unexpired exact-content leases;
memory and persistent reopen tests pass; strict Rustdoc/doctest and the existing
full gate remain green.

Result: complete. `open_content_leased` publishes a versioned durable record for
an exact content identity before returning. Handle clones share an atomic local
deadline; renewal validates owner, content, lease, durable expiry, and bounded
optimistic conflicts before advancing that deadline. Expired leases cannot be
revived, unleased handles cannot renew, and malformed records fail closed.

The stable record format and lifecycle are documented in
`.phrase/protocol/content-read-lease-v1.md`. Its concurrency boundary is
explicit: exact-prefix inspection is a conservative precheck, not deletion
authority. Future reclamation must install and revalidate a per-content
quarantine/fence around lease recheck and grace. Existing `open_content` and
physical content layout are unchanged.

Memory and native-reopen tests cover leased reads, clone renewal, expiry,
missing lease, zero TTL, exact active inspection, persistence, and malformed
state. The all-feature suite passes with 476 library tests (474 passed, two
ignored) and every integration target. Strict Rustdoc, doctests, locked check,
all-target/all-feature Clippy with wildcard imports denied, formatting, and diff
checks pass.

## Consumer-driven compatible deltas — 2026-07-19

TrineDB's protected logical Version catalog required the exact accepted local
commit sequence inside metadata written by that same transaction. Added
`Transaction::put_bucket_with_commit_sequence`, a compatible API that fills one
fixed eight-byte big-endian value slot only after conflict validation and commit
slot assignment. WAL and memtable publication receive the same final bytes;
conflicts publish nothing; persistent reopen is covered. This does not change
the production-evidence phase status below and does not expose sequence
prediction or reservation.

TrineDB's protected FileState also needs to persist algorithm-tagged ContentId
without parsing display text or depending on private ContentDescriptor layout.
`ContentId::to_bytes` and `ContentId::from_bytes` now expose its fixed 33-byte
portable identity; unknown algorithm tags fail closed. This is a compatible
identity-codec addition only and does not change hashing, descriptor, upload,
deduplication, or object-storage behavior.

## Status

Phase 191 implementation and local verification are complete. Remote Linux,
macOS, and Windows execution of the new production-evidence workflow remains
the phase-closing gate.

## Goal

Create repeatable production evidence for performance, native platforms, and
real process lifecycle behavior without changing Trine's storage or public API
contracts.

## Evidence Boundary

- Performance: compare representative benchmark medians on the same runner and
  preserve raw CSV reports. Timing is a regression signal, not a universal SLA.
- Cross-platform: run target-native tests on Linux, macOS, and Windows; compile
  success alone does not count as runtime evidence.
- Process lifecycle: terminate a child process without running Rust destructors,
  then reopen and verify every confirmed write across repeated rounds.
- Soak: use a reproducible seed, concurrent disjoint writers, reads, snapshots,
  maintenance, flush, compaction, close, and reopen verification.
- Destructive I/O: inject one deterministic failure at a named native-storage
  boundary, then verify the returned error, file state, retry or repair path,
  and reopened contents.

## Current Task Slice

- Added a bounded `production-gate` benchmark profile and paired CSV comparator.
- Added configurable forced-exit and mixed-load production-maturity tests.
- Added scheduled/manual/PR Linux, macOS, and Windows evidence jobs and
  machine-readable artifacts.
- Added macOS platform-I/O runtime coverage to normal CI.
- Fixed commit completion returning before its sequence became continuously
  visible.
- Bound compaction plans to their source LSM version and serialized flush with
  active table rewrites; compaction and Blob GC replacement are serialized per
  bucket while different buckets retain independent compaction progress.
- Documented local commands, evidence meaning, deployment gaps, and the updated
  `0.5` dependency line.
- Added an executable documentation-drift gate that ties active dependency
  examples and release documentation to the commands enforced by CI and the
  publish workflow.
- Added the forced-exit and 10,000-operation mixed-load evidence harness to the
  publish workflow while retaining the Linux/macOS/Windows workflow as the
  phase-closing cross-platform gate.
- Reworked README adoption guidance around production status, a five-minute
  verification path, evidence-backed capabilities, and explicit deployment
  boundaries; its maturity claims and local links are now checked in CI.
- Added a path-scoped test-only storage fault model and a six-scenario matrix
  for WAL append/persist, table/manifest publish, directory sync, WAL rewrite,
  and delete retry; the matrix now runs in cross-platform and publish gates.
- Prepared the compatible `0.5.12` patch release metadata and changelog, and
  passed the local Cargo package and publish dry-run gates.
- Reduced the consumer package from 206 files and 548 KiB compressed to 140
  files and 415.7 KiB by excluding repository-only tests, benchmarks, and
  extended documentation; the package rebuilds successfully from the archive.
- Corrected the production-evidence job-level report expressions to use the
  available matrix context, and isolated the `wasm-bindgen-cli 0.2.122` tool
  install on Rust 1.88 while keeping Trine verification on Rust 1.85.
- Removed native/browser-only CommitTracker waker state from production WASI
  builds while retaining its unit-test coverage, and made WASI rustc warnings a
  CI and publish failure.
- Corrected the browser platform split so DedicatedWorker uses synchronous OPFS
  handles while SharedWorker preserves the async multi-tab path, and replaced
  the nested Blob-worker tests with native wasm-bindgen Worker targets.
- Released browser compaction's internal input-table handles before its awaited
  cleanup pass, preventing successful compaction from leaving old tables that
  fail the next read-only reopen.
- Updated every GitHub workflow checkout step to `actions/checkout@v7` so its
  declared Node.js 24 runtime matches the hosted-runner transition, and made the
  release drift gate reject legacy checkout versions.
- Prepared `0.5.13` metadata from the seven commits after the `v0.5.12` tag and
  moved post-tag fixes out of the `0.5.12` changelog section so published
  release history matches the code contained in each tag. The 140-file,
  approximately 417-KiB package rebuild and crates.io publish dry-run pass
  locally.

## Out Of Scope

- Optimizing code before this phase identifies a measured retained hotspot.
- Claiming sudden-power-loss proof from forced process exit.
- Claiming deterministic returned-I/O-error injection is equivalent to real
  kernel, filesystem, controller, quota, or physical-device failure.
- Changing public API, storage formats, durability semantics, or provider
  adapters.

## Acceptance Gate

- The bounded benchmark profile emits all required rows in grouped CSV form.
  Met.
- The comparator passes controlled samples and fails a deliberate regression.
  Met.
- Forced-exit recovery and mixed-load soak pass locally. Met: four forced-exit
  rounds, one 100,000-operation run, and five 50,000-operation seeds passed.
- CI definitions cover Linux, macOS, and Windows runtime evidence and preserve
  reports as artifacts. Implemented; remote execution pending.
- Full local verification passes and evidence delta records remaining gaps.
  Met.
- Active release documentation, CI, and publish automation pass the automated
  drift contract. Met.
- Deterministic destructive scenarios cover pre-write failure, post-write
  persistence failure, pre-rename publish failure, post-rename directory-sync
  failure, atomic rewrite retry, and delete retry. Met locally; remote matrix
  pending with the rest of Phase 191.

## Known Blockers

- The Rust 1.88 wasm-bindgen tool install, strict WASI warning gate, Window
  suite, and all four DedicatedWorker tests passed on GitHub-hosted CI. The
  SharedWorker test then reproduced Chrome's `SecurityError` for OPFS under the
  wasm-bindgen-test runner's default COOP/COEP headers. CI and publish now
  disable those optional test-server headers; a fresh remote run is pending.
- The corrected Linux/macOS/Windows workflow has not run remotely in this
  local-only session; Phase 191 cannot close until those jobs and artifacts are
  inspected.
- The currently running workflow was created from the preceding commit and will
  retain its `actions/checkout@v4` warning; the Node.js 24 action update needs a
  fresh remote run.
- `v0.5.13` is intentionally not created until the release-preparation commit
  passes the hosted browser and Linux/macOS/Windows production-evidence gates.
- GitHub-hosted timing varies; the paired gate uses broad relative limits plus
  an absolute noise floor and is not a hardware SLA.
- Process exit validates application-crash recovery, not sudden power loss.
- Real disk-full/quota exhaustion, sanitizer, and long-duration fleet evidence
  remain follow-up candidates.
