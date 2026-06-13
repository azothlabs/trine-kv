# Current Phase

## Status

Complete

## Goal

Make platform-io behavior visible through public diagnostics so callers can see
which Trine storage operations completed as true platform async, partial native
async, platform-managed fallback, blocking fallback, or unsupported.

## Scope

- Improve the public `DbStats` platform-io diagnostics so users do not need to
  hand-sum operation class fields.
- Keep diagnostics at the Trine storage operation boundary, not OS syscall
  names.
- Explain class meanings in public docs and rustdoc.
- Verify diagnostics on Linux and at least one non-Linux target where this
  environment can compile or run the target-specific checks.

## Backend Boundary Receipt

- Owned public surface: `DbStats::storage_platform_io_operations`,
  `PlatformIoClassCounters`, `PlatformIoOperationStats`, and the public usage
  guide.
- Operation names remain: length lookup, owned random read, whole-object read,
  temporary write plus rename publish, append open, append, persist/fsync, WAL
  rewrite, delete, directory create, directory sync, directory listing, and
  writer lease.
- Class meanings remain: true platform async, partial native async,
  platform-managed fallback, blocking fallback, and unsupported.
- Diagnostics must describe current backend behavior without promising that a
  fallback target is already true async.
- Verification gate: rustdoc with warnings denied, focused diagnostics tests,
  Linux platform-io diagnostics, non-Linux diagnostics, and full local gates.

## Out Of Scope

- Changing storage behavior, file formats, WAL/manifest/table semantics, or
  durability semantics.
- Implementing additional OS-specific async primitives.
- Revalidating compaction, maintenance, cleanup, or close paths.
- Publishing, tagging, pushing, or creating a GitHub release.

## Acceptance Gate

- Public stats expose per-operation platform-io classes and a clear aggregate
  summary API.
- Public docs explain how to interpret true async, partial native async,
  platform-managed fallback, blocking fallback, and unsupported counters.
- Tests assert diagnostics for Linux and at least one non-Linux target.
- Phase completion is recorded in evidence and committed before starting the
  next phase.

## Active Task Slice

```text
task699 [x] goal:start public diagnostics phase | scope:current roadmap | verify:phase brief
task700 [x] goal:add public aggregate helpers | scope:src/stats.rs src/lib.rs | verify:unit tests rustdoc
task701 [x] goal:document diagnostics interpretation | scope:docs usage protocol | verify:rustdoc/docs diff
task702 [x] goal:verify Linux and non-Linux diagnostics | scope:tests/docker/local targets | verify:quiet gates
task703 [x] goal:record and commit Phase 158 | scope:evidence roadmap git | verify:commit
```

## Evidence

- `DbStats::storage_platform_io_operations` already exposes per-operation class
  counters from Phase 154.
- Linux tests assert true platform async operation counters for read, append,
  flush, and WAL rewrite paths.
- The local macOS/non-Linux test asserts platform driver fallback accounting for
  random reads.
- `PlatformIoClassCounters` and `PlatformIoOperationStats` now expose public
  aggregate helpers with doctested examples.
- Linux Docker platform-io tests and local non-Linux platform-io tests passed.

## Known Residuals

- Real Windows, FreeBSD, and illumos runtime diagnostics remain external to this
  local host.

## Next Recommendation

- Commit Phase 158, then start Phase 159: engine path revalidation on the
  corrected cross-platform platform-io contract.
