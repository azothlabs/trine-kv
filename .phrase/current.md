# Current Phase

## Status

Complete

## Goal

Give BSD and other Unix targets explicit platform-io classification instead of
leaving every non-Linux/non-macOS Unix target in one vague fallback bucket.
Where the selected backend exposes AIO primitives, classify complete Trine
operations as partial until their remaining blocking steps are replaced.

## Scope

- Audit the selected `compio-driver 0.7.1` Unix backend for FreeBSD,
  Solaris-family targets, and other Unix targets.
- Split backend classifications where the selected backend has materially
  different async support.
- Keep complete Trine operation classes honest: AIO read/write/sync primitives
  do not make operations true platform async if open, rename, delete, directory,
  or lease steps remain blocking.
- Record validation limits for targets not installed in this environment.

## Backend Boundary Receipt

- Trine operation names: length lookup, owned random read, whole-object read,
  temporary write plus rename publish, append open, append, persist/fsync, WAL
  rewrite, delete, directory create, directory sync, directory listing, and
  writer lease.
- Owned interface: `PlatformIoBackendKind`, backend matrix modules,
  `PlatformIoTaskClass`, platform I/O stats, ADR 0002, and async storage
  protocol.
- Chosen backend: selected `compio` Unix polling/AIO path. FreeBSD and
  Solaris-family targets have AIO hooks for some regular-file read/write/sync
  primitives; other Unix targets remain platform-managed fallback.
- Known backend limits: FreeBSD/Solaris-family complete Trine operations are
  partial, not true platform async, because open, stat, rename, delete,
  directory, listing, and lease steps still include blocking/direct syscall
  work.
- Leak-check scope: no BSD/Unix-specific branching in KV engine code; only
  platform backend matrices and diagnostics differ by target.
- Verification gate: local compile/tests for unaffected targets, source audit,
  target checks where installed, and explicit evidence for any unavailable
  target runtime.

## Out Of Scope

- Implementing new BSD, Solaris, or Unix-specific async file primitives.
- Implementing macOS or Windows upgrades.
- Rewriting compaction, maintenance, cleanup, close, or cooperative
  maintenance.
- Changing manifest, WAL, SSTable, MVCC, transaction, or recovery formats.
- Publishing, tagging, pushing, or creating a GitHub release.

## Acceptance Gate

- FreeBSD/Solaris-family async primitive evidence is recorded separately from
  other Unix fallback evidence.
- Backend matrix code exposes the distinction without changing KV engine code.
- Other Unix targets remain `PlatformManagedFallback` unless audited otherwise.
- Directory listing remains `BlockingFallback`.
- Verification limits are recorded for targets unavailable in this environment.
- Phase completion is committed before starting the next phase.

## Active Task Slice

```text
task694 [x] goal:start BSD/other Unix phase | scope:current roadmap | verify:phase brief
task695 [x] goal:audit selected Unix AIO/fallback paths | scope:cargo registry source | verify:audit notes
task696 [x] goal:split BSD/Solaris-family backend classification | scope:src/io src/io/platform_backend | verify:local compile plus cfg review
task697 [x] goal:update docs/evidence for BSD/other Unix limits | scope:ADR protocol evidence | verify:docs diff
task698 [x] goal:verify and commit Phase 157 | scope:tests docs git | verify:available target checks
```

## Evidence

- `compio-driver 0.7.1` build aliases set `aio` for FreeBSD and
  Solaris-family targets only.
- On non-AIO Unix targets, regular-file read/write/sync operations use blocking
  decisions before direct syscalls.
- FreeBSD/Solaris-family AIO primitive evidence still needs to be mapped to
  complete Trine operation classes.
- `FreeBsdNative` and `SolarishNative` backend kinds now distinguish selected
  backend AIO primitive targets from other Unix fallback targets.
- FreeBSD and illumos target checks passed with `platform-io`, including test
  compilation.

## Known Residuals

- Real runtime validation on BSD/Solaris-family hosts remains external to this
  local host; this phase verified target compilation only.

## Next Recommendation

- Commit Phase 157, then start Phase 158: Public Platform-I/O Diagnostics.
