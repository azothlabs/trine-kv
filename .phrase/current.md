# Current Phase

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
- GitHub-hosted timing varies; the paired gate uses broad relative limits plus
  an absolute noise floor and is not a hardware SLA.
- Process exit validates application-crash recovery, not sudden power loss.
- Real disk-full/quota exhaustion, sanitizer, and long-duration fleet evidence
  remain follow-up candidates.
