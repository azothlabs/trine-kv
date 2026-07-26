# Phase 194 File-by-File Progress

This ledger records completed review/implementation units and their executable
evidence. A checked file has been read in full for the stated concern, changed
where needed, formatted, and covered by at least the focused gate shown here.

| Status | File or module | Result | Focused evidence |
| --- | --- | --- | --- |
| complete | `src/invariants.rs`, `verification/kani/Cargo.toml` | One shared minimal core for runtime and proof builds | Kani harnesses; unit/property users |
| complete | `src/manifest.rs`, `src/manifest/store.rs`, `src/manifest/format.rs` | One successor validator before durable publication | manifest transition tests |
| complete | `src/substrate.rs`, `src/substrate/lease_state.rs` | Explicit lease observation and WAL-head transition plans | lease transition tests |
| complete | `src/content/upload.rs` | Explicit upload publication/retirement plans | upload transition tests |
| complete | `src/object_store.rs` | Reduced to public types and module boundary | compile and object tests |
| complete | `src/object_store/contract.rs` | Provider contract and qualification only | 18 object tests |
| complete | `src/object_store/memory.rs` | Deterministic conditional object implementation only | 18 object tests |
| complete | `src/object_store/backend.rs` | Storage-trait translation only | 18 object tests |
| complete | `src/db/content/backend.rs` | Shared memory/native/browser/object content adapter | all-feature check |
| complete | `src/db/content/storage.rs` | Uses shared adapter; retains only object naming, CAS, and fences | content tests |
| complete | `src/db/content/upload/session.rs` | Begin/resume lifecycle | content-upload tests |
| complete | `src/db/content/upload/seal.rs` | Seal lifecycle | content-upload tests |
| complete | `src/db/content/upload/abort.rs` | Abort and chunk cleanup lifecycle | content-upload tests |
| complete | `src/db/content/upload/quota.rs` | Physical quota accounting | content tests |
| complete | `src/db/content/upload/maintenance.rs` | Operator listing/reaping/pruning | content tests |
| complete | `src/db/sync_api/maintenance/coordinator.rs` | Worker coordination only | maintenance tests |
| complete | `src/db/sync_api/maintenance/registry.rs` | Bucket registry only | maintenance tests |
| complete | `src/db/sync_api/maintenance/reads.rs` | Read and scan orchestration only | read tests |
| complete | `src/db/sync_api/maintenance/flush.rs` | Flush planning/publication only | maintenance tests |
| complete | `src/storage/fault_injection.rs` | Unique 17-boundary native catalog with phase | destructive tests |
| complete | `src/storage/native_file/helpers.rs` | Before/after hooks on atomic I/O instructions | destructive tests |
| complete | `src/db.rs`, `src/stats.rs`, fatal callers | Exactly-once automatic stop and reason metrics | destructive unknown-outcome test |
| complete | `tests/property_model.rs` | Generated reference-model operation sequences | 32 generated cases |
| complete | `tests/concurrency_model.rs` | Exhaustive small commit/waiter schedules | Loom test |
| complete | `tests/acceptance.rs` | Backend-neutral Cucumber lifecycle and mandatory cleanup | native and real-R2 runs |
| complete | `tests/acceptance_support/world.rs` | Durable native/object-store profiles with identical expectations | isolated directory/prefix cleanup |
| complete | `tests/acceptance_support/steps_database.rs` | Durable commit, reopen, lifecycle, and typed ownership steps | native and real-R2 runs |
| complete | `tests/acceptance_support/steps_history.rs` | Snapshot, checkpoint, retention, and range-delete steps | native and real-R2 runs |
| complete | `tests/acceptance_support/steps_namespace.rs` | Namespace generation and cross-bucket atomicity steps | native and real-R2 runs |
| complete | `tests/acceptance_support/steps_query.rs` | Ordered snapshot-consistent cursor steps | native and real-R2 runs |
| complete | `tests/acceptance_support/steps_transaction.rs` | Point/range conflict and atomic commit steps | native and real-R2 runs |
| complete | `tests/acceptance_support/steps_content.rs` | Staging visibility, seal durability, and identity steps | native and real-R2 runs |
| complete | `tests/features/*.feature` | Requirement-traced public behavior contract | 8 features, 30 scenarios, 167 steps |
| complete | `src/error.rs` and writer-lease adapters | One public `LeaseUnavailable` outcome across native, WASI, browser, and object storage | focused ownership tests and live R2 |
| complete | `src/substrate/lease_state.rs`, `src/object_store/{contract,memory,backend}.rs`, `src/s3.rs` | Mutable control reads retry typed object-version races; immutable reads still fail closed | deterministic race test, object contract tests, full real-R2 Gherkin |
| complete | `src/s3.rs` live suite | Measures the real async group-commit path and exact immutable/CAS requests | real R2: 12 concurrent writes, 1 WAL segment, 2 conditional writes |
| complete | `src/fuzzing.rs`, `fuzz/fuzz_targets/` | Six real persistent decoder entry points | short local campaigns; scheduled CI |
| complete | `.github/workflows/deep-assurance.yml` | Pinned audit, coverage, Kani, Miri, fuzz, sanitizer, mutation gates | workflow/document drift checks |
| complete | `docs/architecture.md`, `docs/assurance-case.md` | Dependency rule and exact proof/test/assumption boundary | documentation review |

## Baseline Evidence

- CI-equivalent library/test coverage: 77.22% lines, 75.37% functions, 75.84%
  regions; the 75% line gate passes.
- Local dependency advisory scan: 368 locked packages, no advisory.
- Local six-target short fuzz campaign: no crash.
- Local Miri manifest, lease-owner, and upload publication transitions: pass.
- Local Kani 0.67.0: 5/5 invariant harnesses verified with no failed
  properties.
- Local cargo-mutants 27.0.0: 20 variants, 19 detected by focused invariant
  tests, 1 compile-invalid, 0 surviving variants.
- Durable Gherkin corpus: 8 features, 30 scenarios, and 167 steps passed
  unchanged on native storage and real R2.
- Real R2 measurement/fault suite: 12 sequential writes produced 12 immutable
  WAL segments; 12 concurrent async writes were grouped into one segment with
  one immutable create and one head CAS; flush cleanup and reopen checks passed.
- Final native/all-feature suite: 598 library tests passed and four live or
  production-evidence tests were intentionally ignored; every integration,
  acceptance, example, and benchmark target passed.
- Strict native and browser Clippy, warning-denied WASI check, Rustdoc, 31
  doctests, documentation drift, workflow parsing, formatting, and diff hygiene
  passed.

## Explicit Residual Scope

- Linux sanitizers and mutation testing are encoded as scheduled/manual CI
  jobs; they were not available as equivalent local macOS evidence in this
  phase.
- The 17-entry fault catalog is exhaustive for the represented native atomic
  hooks, not for syscalls hidden inside third-party libraries or remote
  provider internals.
- Live R2 has repository evidence. Live browser, WASI, other S3-compatible
  providers, filesystem, kernel, controller, and hardware behavior remains
  deployment evidence.
