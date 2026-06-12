# Current Phase

## Status

Complete

## Goal

Remove `Sequence` from the public API before the `0.3.0` release.

## Scope

- Stop exporting `Sequence` and `SnapshotSequence`.
- Make sequence accessors and constructors crate-internal where they only serve
  engine code.
- Update public tests, docs, protocol, changelog, and release evidence to use
  `ReadVersion`.

## Out Of Scope

- Changing internal commit ordering, WAL, manifest, MVCC, compaction, or
  transaction behavior.
- Publishing, tagging, pushing, or opening a PR.
- Branch, merge, rebase, time-based retention, checkpoint replacement, or
  lineage mapping.

## Acceptance Gate

- Public exports no longer include `Sequence` or `SnapshotSequence`.
- External tests and docs use `ReadVersion` for historical-read boundaries.
- Internal engine code keeps typed sequence semantics.
- Rustdoc, doctests, focused tests, clippy, full tests, diff checks, and scans
  pass.

## Active Task Slice

```text
task632 [x] goal:remove public Sequence surface | scope:lib types db snapshot transaction | verify:rg public surface
task633 [x] goal:update public tests/docs | scope:tests docs protocol changelog | verify:focused tests/doctests
task634 [x] goal:run full gate and commit | scope:rustdoc clippy tests scans | verify:all pass
```

## Evidence

- User clarified there is no real old-caller burden to preserve.
- `0.3.0` has not been released, so this breaking cleanup can be included in
  the same pre-`1.0` minor release.
- The long-term API direction is that callers should use `ReadVersion`, not
  internal commit-number terminology.

## Known Residuals

- None for this phase.

## Next Recommendation

- Public cleanup is complete. Keep further historical-read extensions deferred
  until there is explicit evidence for them.
