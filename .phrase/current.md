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

## Out Of Scope

- Optimizing code before this phase identifies a measured retained hotspot.
- Claiming sudden-power-loss proof from forced process exit.
- Simulating kernel, filesystem, controller, or physical-device failure.
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

## Known Blockers

- The new Linux/macOS/Windows workflow has not run remotely in this local-only
  session; Phase 191 cannot close until those jobs and artifacts are inspected.
- GitHub-hosted timing varies; the paired gate uses broad relative limits plus
  an absolute noise floor and is not a hardware SLA.
- Process exit validates application-crash recovery, not sudden power loss.
- Disk-full, I/O fault injection, sanitizer, and long-duration fleet evidence
  remain follow-up candidates.
