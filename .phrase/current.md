# Current Phase

## Status

Complete

## Goal

Add runnable integration examples that show Trine KV behind realistic
application boundaries.

## Entry Condition

- Phase 7 release packaging is complete.
- User chose integration examples after release packaging.

## Scope

- Runnable examples under `examples/` using only public Trine KV APIs.
- Documentation links from README or usage docs.
- Verification that examples compile and run.

## Out Of Scope

- New storage-engine behavior.
- New public API helpers unless examples expose a concrete API blocker.
- Heavy external dependencies.

## Acceptance Gate

- Integration examples run with `cargo run --example`.
- README or usage docs points users to the examples.
- `cargo fmt --check`, `cargo clippy`, `cargo test`, and `git diff --check`
  pass.

## Active Task Slice

```text
task043 [x] goal:repository-pattern and event-index integration examples are runnable and documented | scope:examples,README.md,docs,.phrase | verify:cargo run --example user_store + cargo run --example event_index + cargo fmt --check + cargo clippy + cargo test + git diff --check
```

## Known Blockers

- None recorded for Phase 8.
- The examples did not expose a public API blocker.

## Evidence To Record

- Example run results.
- Any API friction discovered while writing examples.

## Next Recommendation

- Choose the next phase from CI/release verification, publishing workflow,
  more targeted hardening, or user-requested API changes.
