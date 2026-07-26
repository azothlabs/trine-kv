# Assurance Case

This document states what repository evidence establishes, what remains an
assumption, and what must be established in a deployment. No finite test suite
proves the absence of every unknown defect.

## Invariant map

| Invariant | Runtime enforcement | Machine evidence |
| --- | --- | --- |
| Commit sequences advance by one and never wrap | checked sequence successor and ordered commit tracker | Kani successor proof, property/reference sequences, commit tests |
| Manifest durable fences never regress | `ManifestState::validate_successor` before every publish | transition tests and Kani order proof |
| Immutable objects are create-only | conditional create; equal bytes make a retry idempotent; differing bytes are corruption | Kani retry-policy proof, object contract tests, provider qualification |
| Retained snapshots remain reachable | snapshot pins and checkpoint manifest entries constrain retention | Kani interval proof, property/reference tests, Gherkin checkpoint scenario |
| Lease epochs fence stale writers | explicit lease observation and commit-head transition plans | lease transition tests and object-writer tests |
| Garbage collection never deletes a reachable object | shared `gc_delete_allowed` predicate before deletion | Kani proof, cleanup tests, reopen checks |

The Kani harnesses prove the pure predicates for all values in their finite Rust
types. They do not prove filesystem, browser, network, cloud-provider, kernel,
or hardware behavior.

## Layered evidence

Fast pull-request gates:

- formatting and strict Clippy;
- all-target/all-feature tests and supported WASM checks;
- 30 requirement-traced Gherkin scenarios with 167 steps against durable native
  storage on every run;
- dependency advisory scan;
- a 75% line-coverage floor.

Scheduled and manually dispatched deep gates:

- Kani proofs for the trusted invariant core;
- Miri checks for manifest, lease, and upload transitions;
- six persistent-format fuzz targets: manifest, WAL, table, blob, upload, and
  object-control records;
- address and thread sanitizers on the library suite;
- mutation testing of `src/invariants.rs`.

The deterministic property test generates writes, point deletes, half-open
range deletes, snapshots, flushes, compactions, and reopens, and compares every
observable step with a `BTreeMap` reference model. The Loom model enumerates
the small commit-publisher/waiter schedule instead of relying only on timing
stress.

## I/O boundary matrix

The native atomic-publication catalog currently contains 17 supported
before/after boundaries:

- WAL append before/after, partial append before, and WAL persist before/after;
- immutable object publish before/after;
- manifest rename and manifest-directory sync before/after;
- WAL rewrite rename before/after;
- general directory sync before/after;
- object delete before/after.

Each boundary reports whether the operation is known not to have happened,
known to have happened, or has an unknown durable outcome. Recovery checks
validate earlier confirmed data and the allowed result set.

This is a complete enumeration of the native hooks represented by
`StorageFaultBoundary::ALL`; it is not yet a proof over every syscall issued by
dependencies or by remote providers. Browser transaction aborts, object-store
CAS ambiguity, listing delay, and provider-side deletion are covered by
contract fakes and targeted tests, but still require live-host qualification.

The same 30-scenario corpus and the object request/fault suite also pass
against real R2. That is evidence for the tested R2 account, bucket policy,
endpoint, and run; it does not generalize to every provider or future
configuration.

## Commands

```text
cargo test --all-targets --all-features
cargo test --test acceptance
TRINE_ACCEPTANCE_BACKEND=s3 cargo test --features s3 --test acceptance
cargo test --test property_model
cargo test --test concurrency_model
cargo audit
cargo llvm-cov --all-features --lib --tests --fail-under-lines 75
cargo +nightly miri test --lib manifest_successor
cargo kani --manifest-path verification/kani/Cargo.toml
cargo mutants -f src/invariants.rs -E 'proofs::' --timeout 60 -- --lib invariants::tests
cargo +nightly fuzz run manifest -- -dict=fuzz/dictionaries/formats.dict
```

The Kani manifest compiles the exact runtime `src/invariants.rs` as a minimal
standalone proof target because Kani 0.67's verifier toolchain predates the
main crate's Rust 1.95 floor. It does not copy or fork the invariant logic.

The pinned versions and the remaining fuzz targets are encoded in
`.github/workflows/deep-assurance.yml`.

## Evidence limits

Coverage proves execution, not assertion quality. Fuzzing samples a large input
space but does not exhaust unbounded byte strings. A bounded concurrency model
does not enumerate every production task graph. Sanitizers and Miri cover only
executed paths. Dependency scans know only published advisories. Formal proofs
are only as strong as their specifications and environmental assumptions.

Residual risk is reduced by making these layers independent: a defect must
escape the type/transition boundary, direct tests, randomized reference model,
concurrency schedule, fault matrix, decoder fuzzing, and production detection.
