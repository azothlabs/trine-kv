# Decision Framework

## Principles

- Use minimal default context.
- Let evidence choose the next phase.
- Keep roadmaps at phase granularity.
- Keep tasks local to the current phase.
- Record only decision-relevant memory.
- Verify before claiming completion.

## Evidence Rules

Accepted evidence may include tests, traces, benchmarks, audits, user
observations, incident facts, data checks, manual verification, and prototype
results.

Evidence notes should separate:

- observation
- interpretation
- recommended next action

## Durable Boundaries

- Do not silently change stable contracts; update ADR or protocol docs.
- Do not pre-split future roadmap phases into tasks.
- Do not read archive files by default.
- Do not maintain mechanical per-file changelogs when the git diff is enough.
- Write comments for non-obvious Rust engine invariants, especially lock order,
  MVCC visibility, batch atomicity, and storage-format assumptions.
- Treat public Rustdoc as user documentation, not lint filler. Public API docs
  must explain purpose, parameters, return semantics, errors, user-observable
  behavior, adjacent API differences, and doctest examples for core or easily
  misused entry points.
- Prefer concrete storage-engine language in explanations and comments.
- For platform I/O and storage backend work, Trine's own boundary must be the
  design subject. External crates, OS APIs, and libraries are implementation
  choices only; they must not drive public naming, phase goals, protocol
  wording, or acceptance gates.
- Before implementing or extending a backend, record a backend boundary receipt
  in `current.md` or the active protocol: Trine operation names, owned
  interface, chosen backend, known backend limits, leak-check scope, and
  verification gate.

## Trine KV V1 Source Of Truth

- V1 engine decision: `.phrase/adr/0001-v1-lsm-mvcc-engine.md`.
- Native platform I/O backend matrix:
  `.phrase/adr/0002-native-platform-io-backend-matrix.md`.
- V1 protocol and storage contract: `.phrase/protocol/trine-kv-v1-spec.md`.
- Trine specs, ADRs, tests, and local design notes are the source of truth.
- Do not implement Trine by depending on another storage engine.
- V1 compression uses only `none` and `lz4_flex`-backed fast block compression;
  do not add zlib/DEFLATE or `flate2` for v1.
- Public crate versions use Semantic Versioning. Before `1.0.0`, breaking
  public API or storage-contract changes should increment the minor version;
  compatible fixes should increment the patch version.
- Do not change MVCC, WAL, SSTable, manifest, compaction, transaction,
  prefix-filter, compression, or search-policy behavior without updating the
  protocol spec or adding a follow-up ADR.
- Trine's primary database API and storage boundary are async-first. Sync
  native APIs are adapters over the primary async engine and use explicit
  `*_sync` public names.
- Public database open is path-first and persistent by default:
  `Db::open(path)` and `Db::open_sync(path)` open a persistent database, while
  in-memory mode requires explicit `DbOptions::memory()`.
- `PlatformAsyncIo` means the selected platform-io backend can complete at
  least one current Trine storage operation asynchronously without blocking the
  caller runtime. The operation may be `TruePlatformAsync`,
  `PlatformNativeAsyncButPartial`, or `ThreadPoolManagedAsync`; exact class
  counters remain the source of truth for whether the operation used native OS
  completion or platform-managed thread-pool completion. Targets without native
  threads, such as browser WASM, must not advertise thread-pool managed async.
- `platform-io` is the Cargo baseline for platform async I/O and uses Trine's
  bounded thread-pool backend on native-thread targets. `platform-io-native`
  enables native backend dependencies and falls back to the same thread-pool
  backend for operation rows without native support. Targets without native
  threads may compile the feature shape, but must not advertise
  `PlatformAsyncIo`.
- Platform-io completion must be judged before engine revalidation. The
  platform-io responsibility is to be Trine's cross-platform async file I/O
  abstraction; engine phases must not treat a half-complete backend matrix as
  the final async boundary.
- Platform-io backend acceptance is per complete Trine operation, not per OS
  primitive. The required operation rows are random read, whole-object read,
  temporary write plus rename publish, append open, append, persist, WAL
  rewrite, delete, directory create, directory sync, directory listing, and
  writer lease. Each row must report whether the complete operation is true
  platform async, partial native async, thread-pool managed async, blocking
  fallback, or unsupported on the selected platform.
- Persistent storage behavior is governed by backend capabilities, including
  writer lease, manifest publish, durability strength, and background-work
  support.
- Native persistent constructors default to safety-first durability for
  confirmed writes. Lower durability modes such as `Buffered` are explicit
  advanced choices for data that can tolerate losing recent confirmed writes.
- WASI and browser persistence must be selected through explicit host backend
  options. WASI persistence may use the host-preopened filesystem on WASI
  targets; browser persistence must fail as `UnsupportedBackend` until its
  required capabilities are implemented.
- WASM readiness is a design constraint for public API, runtime boundary, and
  storage backend boundaries.
- Titan and other external storage engines may be used as design references,
  but Trine must keep its own code, file formats, tests, and recovery contract.

## Phase Gate Rules

A phase can close only when:

- acceptance gate is checked
- verification evidence exists
- remaining blockers are recorded
- next phase recommendation is written
- durable decisions are updated if needed

## Rejected Paths

- Full-history loading as the default agent behavior.
- Static spec/plan/task/change bookkeeping for every session.
- Treating stale plans as current truth after fresh evidence contradicts them.
- Abstract jargon when plain engine terminology is clearer.
- Rustdoc comments that merely silence `missing_docs` without teaching users
  how to understand or call the API.
- Letting an external backend crate become the architecture boundary instead
  of Trine's `io` and storage contracts.
