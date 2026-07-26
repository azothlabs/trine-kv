# Current Phase

## Architecture clarity and layered assurance

**Status:** In progress: hosted release gates pending.

### Goal

- Remove duplicated durable-state decisions from platform adapters.
- Divide object storage, content storage, and maintenance by owned
  responsibility.
- Make legal durable transitions and irreversible I/O plans explicit.
- Turn the six critical engine invariants into executable evidence.
- Add complete Gherkin acceptance coverage and repeatable deep-verification
  gates.

### Architecture remediation slice (complete locally)

- Replace backend-specific optional fields on `DbInner` with one owned
  `DatabaseStorage` state as accepted in ADR 0004.
- Make the complete durable branch lifecycle async-first and retain only
  explicit `*_sync` native adapters.
- Keep branch legality and generation decisions shared between both execution
  paths.
- Move immutable-content lifecycle extensions out of the generic transaction
  module.
- Split open, durability-substrate, and platform-I/O files at responsibility
  boundaries without changing storage formats or durability semantics.
- Add compile-checked architecture contracts and object-store branch
  acceptance before closing the slice.

The slice is complete. The engine now owns exactly one `DatabaseStorage`
variant, branch APIs are async-first with explicitly named native sync
adapters, and immutable-content transaction behavior lives outside the generic
transaction core. Responsibility-focused modules and executable architecture
checks prevent the retired mixed boundaries from returning.

The follow-up refinement is also complete locally. Branch registry, state,
handle I/O, range merge, transitions, and API orchestration now have separate
modules. Sync Branch APIs drive the same async orchestration and range merge,
so there is only one lifecycle execution path. Content transaction extensions
are divided by lifecycle capability. Transaction state has its own private
module without inventing a separately published crate. Backend-specific
clients and coordinates are exposed only through exhaustive typed resource
bundles.

### Evidence collected

1. Native all-target/all-feature suite: every executed target passed; provider
   live tests remain intentionally ignored in the default run.
2. The backend-neutral requirement-driven Gherkin corpus contains 10 features,
   52 scenarios, and 352 steps. It passed unchanged against isolated native
   directories and isolated prefixes in real R2. A separate durable-native
   branch profile adds 6 scenarios and 55 steps.
3. Strict native and browser Clippy, WASI warning-denied check, Rustdoc, 31
   doctests, formatting, documentation drift, workflow YAML, and diff hygiene
   passed.
4. Coverage passed at 77.32% lines, 75.43% functions, and 75.95% regions;
   dependency audit scanned 368 locked dependencies with no advisory.
5. Kani verified 5/5 harnesses; focused Miri transition checks passed; Loom
   enumerated the small commit/waiter schedule.
6. Six decoder fuzz targets completed local short campaigns without a crash;
   the scheduled workflow extends each campaign.
7. Mutation check evaluated 20 trusted-core variants: 19 were detected and one
   was compile-invalid, leaving no surviving variant.
8. Upload-maintenance mutation analysis initially exposed two missing
   distinctions. The expanded Feature and explicit lifecycle decision table
   now reject every viable focused mutation.
9. The hosted WASI test exposed that unconditional native test harness
   dependencies pulled `proptest`'s Unix/Windows-only process-timeout chain
   into a WASI build.
10. The host-only harnesses are now target-scoped. From a new target directory,
    the exact WASI CI command compiled without that dependency chain and all
    seven persistence tests passed.
11. Native, WASI, and browser CI checks are independent jobs. The documentation
    drift guard rejects collapsing them back into one failure domain, while a
    result-only `Verify` job preserves the aggregate required check.
12. The first independent Browser Verify run exposed two remaining tests that
    expected the superseded `RuntimeBusy` writer-lock category. Browser runtime
    behavior already returned the accepted `LeaseUnavailable`; the tests now
    assert that cross-backend contract. Local Chrome 150 with its matching
    ChromeDriver passed all 20 DedicatedWorker, persistent, and SharedWorker
    tests after the correction.
13. Platform I/O now uses the resource-aware scheduler accepted in ADR 0003.
    Local macOS evidence observed more than one native future in flight; both
    native and managed full library suites, the all-feature suite, strict
    Clippy, Rustdoc, and doctests passed.
14. The architecture remediation slice passed a clean-build all-feature run.
    Its refinement passed 606 core tests with 4 intentionally ignored, the 10-feature
    acceptance corpus passed all 52 scenarios and 352 steps, the durable branch
    profile passed all 6 scenarios and 55 steps, and all 31 doctests passed.
15. Exhaustive storage types enforce singular backend ownership and prevent
    wrong-backend capability access. Integration contracts compile-check the
    public async/sync branch and transaction APIs. Rust module declarations make
    every responsibility module a compile dependency; no source-text or
    file-tree architecture assertions remain. Strict native/browser Clippy and
    the warning-denied WASI library check also passed.

### Residual scope

- Real R2 is qualified for the acceptance and request-measurement suites. Other
  live providers, real browser/WASI hosts, kernel/filesystem/controller
  failures, and hardware power loss remain deployment evidence.
- Linux, Windows, FreeBSD, and Solaris-family platform-I/O qualification plus
  device-specific queue-depth benchmarks remain target evidence; the scheduler
  no longer globally serializes them.
- Local Chrome 150 passes all 20 browser tests. The hosted Chrome job remains
  the browser release evidence for the exact corrected commit.
- The native 17-boundary catalog covers every represented atomic hook, not
  hidden syscalls inside dependencies or remote provider internals.
- The guarantee layers reduce residual risk and make it observable; they do not
  create a logically valid claim that unknown defects are impossible.

### Backend boundary receipt

- Trine operations: immutable object create/read/range/list, conditional
  metadata publish, verified delete, native/browser manifest publish, WAL
  append/persist/rewrite, content chunk/session mutation.
- Owned interfaces: `ObjectClient`, storage read/write/delete/list traits,
  manifest stores, durability substrate, and explicit transition plans.
- Backends: native filesystem, in-memory object client, S3-compatible object
  client, WASI host filesystem, and browser OPFS.
- Known limits: provider conditionals and deletion visibility remain external
  assumptions and must be qualified; browser/WASI runners are host-provided;
  power-loss guarantees cannot exceed the selected durability mode.
- Leak-check scope: temporary objects, retired upload chunks, obsolete
  table/blob files, queued WAL completions, and publish guards.
- Verification gate: focused transition/property tests, Gherkin acceptance,
  systematic fault matrix, all-feature tests, strict Clippy, Rustdoc/doctests,
  supported WASM checks, and deep scheduled tools.

### Platform I/O root-correction slice

- Observation: one native worker executed one `block_on` per task, globally
  serializing unrelated database I/O. The same accidental serialization also
  hid unsafe same-file append offsets. Native startup, bounded admission,
  cross-executor ordering, and driver shutdown were not owned by one boundary.
- Trine operations: random/whole-object read, typed append session
  open/append/persist, explicit manifest/object/WAL publish plan, delete,
  directory create/list/sync, and writer lease.
- Owned interface: `PlatformIoDriver` is the native-file facade;
  `PlatformIoScheduler` owns bounded admission, resource ordering,
  native-versus-managed routing, terminal accounting, failure state, drain, and
  join. Storage traits keep database semantics; platform executors only execute
  admitted plans.
- Chosen backends: `NativeCompletionExecutor` owns one concurrently polled
  Compio runtime where eager startup succeeds; `BlockingFallbackExecutor` owns
  the bounded managed thread pool for fallback matrix rows. The scheduler owns
  both executor lifecycles.
- Known limits: the operation matrix remains target-specific; directory
  listing and writer lease are managed on targets without a qualified native
  operation. Hardware queue-depth benefit still requires benchmark evidence.
- Leak-check scope: accepted completions, scheduler queue entries, resource
  grants, native futures, managed tasks, worker threads, temporary publish
  files, append sessions, and close-time references.
- Verification gate: resource-order test, hard queue-bound test, native
  multi-in-flight test, close/drain/reject test, WAL append regressions, default
  and feature builds, strict Clippy, Rustdoc/doctests, formatting, and diff
  hygiene.

### Acceptance gate

- Gherkin scenarios are derived from stable public requirements, use only
  durable backends, and run with identical expected outcomes on native storage
  and real R2.
- No public behavior, storage format, MVCC rule, or durability guarantee
  changes silently.
- Architecture documentation identifies the trusted core and adapter
  assumptions.
- The exact WASI persistence command passes without compiling host-only test
  harness dependencies.

### Out of scope

- New end-user features, weakened safety defaults, publishing, tagging, or
  pushing changes.
