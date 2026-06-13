# Current Phase

## Status

Active

## Goal

Finish Linux platform-io as a complete Trine-operation backend before returning
to engine revalidation.

## Scope

- Audit Linux operation rows at the Trine operation boundary:
  random read, whole-object read, temporary write plus rename publish, append
  open, append, persist, WAL rewrite, delete, directory create, directory sync,
  directory listing, and writer lease.
- Remove remaining Linux fallback rows where the selected backend/kernel
  contract can support a complete true async operation.
- Treat directory listing as the first known Linux gap.
- Keep other platforms out of this phase except for ensuring the shared
  operation table and diagnostics remain portable.

## Backend Boundary Receipt

- Trine operation names are the acceptance rows, not OS syscall names.
- Owned internal surface: Linux platform backend matrix, platform task
  submission for directory/listing operations, and platform-io diagnostics.
- Chosen backend: selected Linux platform-io backend behind Trine's
  `PlatformIoDriver`.
- Known backend limit entering the phase: directory listing is currently
  `BlockingFallback`; other Linux rows are currently reported as true platform
  async by existing tests but must be rechecked as complete Trine operations.
- Leak-check scope: no Linux-specific branching in KV engine code.
- Verification gate: Docker Linux platform-io tests asserting every Linux
  operation row and local cross-platform checks for unaffected fallback targets.

## Out Of Scope

- Windows partial-operation completion.
- macOS backend selection or Apple-specific implementation.
- Other Unix upgrades beyond preserving fallback classification.
- Engine compaction, maintenance, cleanup, close, or broader engine
  revalidation.
- Storage format changes, publishing, tagging, pushing, or PR creation.

## Acceptance Gate

- Linux operation rows are all asserted by tests at the complete Trine operation
  boundary.
- Directory listing is no longer an unexamined blocking fallback; it is either
  true platform async or explicitly proven unavailable for the selected
  backend/kernel contract.
- Public protocol/evidence records any hard Linux backend limit.
- Phase completion is recorded in evidence and committed before starting
  Windows.

## Active Task Slice

```text
task710 [x] goal:correct roadmap back to platform-io backend completion | scope:decision roadmap current | verify:docs diff
task711 [ ] goal:audit Linux directory listing backend support | scope:kernel/backend/source | verify:source evidence
task712 [ ] goal:update Linux backend implementation or classification | scope:src/io src/io/platform_backend | verify:Docker Linux platform-io tests
task713 [ ] goal:assert every Linux operation row | scope:tests stats | verify:Docker Linux matrix tests
task714 [ ] goal:record and commit Phase 160 | scope:evidence roadmap git | verify:commit
```

## Evidence

- User correction on 2026-06-13: platform-io must first complete its role as
  the cross-platform async abstraction before engine revalidation resumes.
- Phase 158 made per-operation diagnostics available.
- Phase 159 is retained as a diagnostic checkpoint, but its recommended next
  action is superseded by the platform-io backend completion sequence.
- Linux directory listing is the known remaining Linux fallback row.

## Known Residuals

- Windows remains partial for complete operations that still include open,
  metadata, sync, rename, delete, directory, or lease fallback steps.
- macOS needs a newly selected or implemented Apple-side async file path before
  it can move beyond managed fallback.
- Other Unix remains fallback unless later evidence proves more.

## Next Recommendation

- Audit Linux directory listing support in the selected backend/kernel path,
  then either implement true async listing or record a hard backend limit with
  tests.
