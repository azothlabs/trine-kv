# Current Phase

## Architecture clarity and layered assurance

**Status:** Complete after requirements-first acceptance expansion.

### Goal

- Remove duplicated durable-state decisions from platform adapters.
- Divide object storage, content storage, and maintenance by owned
  responsibility.
- Make legal durable transitions and irreversible I/O plans explicit.
- Turn the six critical engine invariants into executable evidence.
- Add complete Gherkin acceptance coverage and repeatable deep-verification
  gates.

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

### Residual scope

- Real R2 is qualified for the acceptance and request-measurement suites. Other
  live providers, real browser/WASI hosts, kernel/filesystem/controller
  failures, and hardware power loss remain deployment evidence.
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

### Acceptance gate

- Gherkin scenarios are derived from stable public requirements, use only
  durable backends, and run with identical expected outcomes on native storage
  and real R2.
- No public behavior, storage format, MVCC rule, or durability guarantee
  changes silently.
- Architecture documentation identifies the trusted core and adapter
  assumptions.

### Out of scope

- New end-user features, weakened safety defaults, publishing, tagging, or
  pushing changes.
