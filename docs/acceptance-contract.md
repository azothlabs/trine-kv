# Acceptance Contract

The Gherkin files in `tests/features/` are an executable view of Trine's public
contract. Their expected outcomes come from normative specification sections,
documented public lifecycle promises, and the public error model—not from
current control flow, private storage layout, or assertions copied out of unit
tests.

## Design rules

- Every scenario uses a durable backend. Volatile memory mode is deliberately
  excluded because it cannot establish recovery, ownership, or publication
  behavior.
- Feature language describes caller-observable state and typed outcomes.
  Manifest records, WAL filenames, lock implementation, helper functions, and
  fault hooks are not acceptance vocabulary.
- Stable `@REQ-*` tags identify promises rather than code paths. A refactor may
  change every internal module without changing a feature.
- One feature corpus runs against the native-file backend on every test run and
  against an isolated prefix in a real S3-compatible service when
  `TRINE_ACCEPTANCE_BACKEND=s3` is selected.
- The runner may translate backend setup and cleanup, but it may not vary an
  expected result by backend. An unsupported or divergent outcome is a failed
  acceptance scenario.
- Scenario data is fixed by the contract. The runner does not compute expected
  values by asking Trine or by mirroring Trine's algorithms.
- Feature review happens at the requirement layer: a feature must still make
  sense if storage filenames, task topology, locks, and internal state structs
  are all replaced.

## Requirement sources

| Tags | Normative source | Observable promise |
| --- | --- | --- |
| `REQ-DURABLE-*`, `REQ-OWNER-*` | V1 §§4.1, 10, 11, 25 | confirmed mutations recover; one durable writer owns the scope |
| `REQ-LIFECYCLE-*` | V1 §§4, 27 and public `Error` contract | closed/read-only/unsupported operations fail explicitly |
| `REQ-VERSION-*`, `REQ-HISTORY-*` | V1 §8 | read versions, snapshots, retention, and checkpoints preserve or reject an exact historical view |
| `REQ-BATCH-*`, `REQ-TXN-*` | V1 §9 | batches are atomic across buckets; failed optimistic transactions publish nothing |
| `REQ-RANGE-*`, `REQ-CURSOR-*` | V1 §§8, 21, 22 | half-open deletion and ordered snapshot-consistent cursors |
| `REQ-NS-*` | V1 §§5, 23 plus the public generation-fenced `Bucket` contract | durable isolation and rejection of stale namespace authority |
| `REQ-CONTENT-*` | public `ContentUpload`/`ContentId` lifecycle contract and Phase 194 content acceptance gate | staging is invisible, seal publishes immutable bytes, equal bytes have stable identity |

Low-level format corruption, syscall crash points, provider request accounting,
and internal transition exhaustiveness remain separate fault, format, model,
and proof suites. They are not rewritten as Gherkin because their vocabulary is
not an application-facing acceptance contract.

## Backend profiles

Native profile:

```text
cargo test --test acceptance
```

Real S3-compatible profile:

```text
TRINE_ACCEPTANCE_BACKEND=s3 cargo test --features s3 --test acceptance
```

The S3 profile requires `TRINE_S3_BUCKET`, `AWS_REGION`, optional
`AWS_ENDPOINT_URL`, and the normal AWS credential environment. Every scenario
uses a unique `trine-gherkin/...` prefix and the after-scenario hook verifies
that its objects were removed.

## Coverage boundary

These 30 scenarios are the durable cross-backend acceptance contract, not a
second spelling of every lower-level test. Arbitrary malformed bytes, exact
crash points, provider response ambiguity, bounded concurrency schedules, and
proof predicates remain in fuzz, fault, model, and formal suites because
expressing those mechanisms as application prose would make the Feature files
less independent—not more complete.
