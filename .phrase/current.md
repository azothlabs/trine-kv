# Current Phase

## Status

Complete

## Goal

Make `ReadVersion` the documented user-facing historical-read boundary while
keeping existing `Sequence` APIs as lower-level compatibility hooks.

## Scope

- Public Rustdoc and examples that currently steer users toward `Sequence`.
- Transaction read-boundary helper parity with `Snapshot` and `CommitInfo`.
- Protocol wording for the `ReadVersion` / `Sequence` boundary.

## Out Of Scope

- Removing `Sequence` from the public API.
- Storage format, manifest format, WAL, MVCC, compaction, or transaction
  conflict behavior changes.
- Branch, merge, rebase, retention, or checkpoint semantics.

## Acceptance Gate

- New user-facing examples prefer `ReadVersion` for historical-read cursors.
- Existing `Sequence` APIs are documented as lower-level / diagnostics-oriented
  rather than the primary application cursor.
- No breaking API removal.
- Rustdoc, doctests, focused checks, clippy, diff checks, and scans pass.

## Active Task Slice

```text
task625 [x] goal:document Sequence boundary | scope:types db transaction docs | verify:rustdoc
task626 [x] goal:add transaction read_version helper | scope:Transaction | verify:doctest/focused test
task627 [x] goal:update protocol/evidence | scope:.phrase/protocol current evidence roadmap | verify:doc review
task628 [x] goal:verify and commit | scope:docs tests clippy scans | verify:all pass
```

## Evidence

- Phase 146 completed `ReadVersion`, checkpoints, and configurable retention.
- Audit found user-facing docs and examples still using `CommitInfo::sequence`
  and `Transaction::read_sequence` as primary-looking entry points.
- The long-term design says callers should not need internal commit-number
  mechanics for historical reads.

## Known Residuals

- Public `Sequence` remains exported for compatibility and lower-level
  diagnostics in this phase.

## Next Recommendation

- With the public sequence boundary cleaned up, keep larger historical-read
  extensions deferred until there is explicit evidence for them.
