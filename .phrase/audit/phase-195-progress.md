# Phase 195 Requirements-First Test Progress

This ledger records the test-expansion work in the order it is performed.
Requirements are recorded before Feature files; Feature files are recorded
before step implementations.

| Status | Unit | Result | Evidence |
| --- | --- | --- | --- |
| complete | `docs/acceptance-requirements-v2.md` | Independent public requirement catalog for branch, upload, lease, hold, access barrier, recovery, and async parity | source-to-requirement review |
| complete | `tests/features_native/branches.feature` | 6 durable-native branch scenarios, 55 steps | native persistent execution |
| complete | expanded backend-neutral Features | 52 scenarios, 352 steps covering upload/lease/hold/access/recovery | native and real R2 execution |
| complete | acceptance step implementations | Shared native/R2 world with verified per-scenario cleanup; no volatile fallback | strict Clippy and traceability checks |
| complete | focused property and transition tests | Persistent randomized branch reference model plus complete upload-maintenance decision table | property execution and focused mutation analysis |
| complete | coverage and test-effectiveness audit | 77.32% lines, 75.43% functions, 75.95% regions; upload-maintenance mutations 12/12 caught after excluding one compile-invalid variant | llvm-cov and cargo-mutants |

## Findings from independent execution

- Three initially skipped Gherkin steps exposed keyword and singular/plural
  registration mistakes; the suite now has zero skipped steps.
- Real R2 rejected a 200 ms "active" hold assumption because legitimate remote
  latency consumed the deadline. The contract now separates active and expired
  observations by 60 seconds and validates renewal to 120 seconds.
- Real R2 returned two unknown DELETE outcomes after HTTP timeouts. Cleanup now
  relists the isolated prefix, retries only objects still visible, and still
  fails unless final absence is verified.
- Mutation analysis found that sealed-state pruning was not checked against an
  old open upload. The Feature now cross-checks both maintenance operations.
- The remaining concurrent cutoff comparison was made an explicit four-state,
  two-operation decision table. All three mutations of that decision are
  rejected.

## Rules

- No scenario is derived from an uncovered line.
- No acceptance scenario uses `DbOptions::memory`.
- Expected values are fixed in Feature text or the public requirement catalog.
- Native and S3 profiles may not disagree on a backend-neutral expectation.
- A failed scenario changes product code or the declared public contract; step
  code may not translate the failure into success.
