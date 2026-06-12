# Current Phase

## Status

Complete

## Goal

Implement named checkpoints and configurable recent read-version retention.

## Scope

- Checkpoint public APIs and typed errors.
- Manifest-backed checkpoint metadata for durable storage modes.
- In-memory checkpoint metadata with the same public semantics.
- `DbOptions::with_keep_last_read_versions` and retained-floor integration.
- Compaction cleanup boundary updated to use the effective retained floor.

## Out Of Scope

- Writable branches, merge, or rebase behavior.
- Time-based retention.
- Checkpoint replacement APIs.
- Replication or lineage mapping.

## Acceptance Gate

- `.phrase/protocol/read-version-public-api.md` and
  `.phrase/protocol/trine-kv-v1-spec.md` record the checkpoint and retention
  boundary.
- `snapshot_at` rejects unavailable versions and never falls back to latest.
- Active snapshots, checkpoints, and configured recent retention all participate
  in the effective retained floor.
- Manifest v8 decodes with no checkpoints and v9 persists checkpoint pins.
- Focused MVCC/read-version and manifest tests pass.
- Rustdoc, doctests, clippy, full tests, diff checks, and forbidden-term scans
  pass before closing.

## Active Task Slice

```text
task621 [x] goal:define durable checkpoint boundary | scope:manifest v9 + memory map | verify:protocol review
task622 [x] goal:add retention option | scope:DbOptions retained floor | verify:focused MVCC test
task623 [x] goal:add checkpoint APIs | scope:Db manifest errors | verify:focused checkpoint test
task624 [x] goal:run full quality gate | scope:rustdoc doctest clippy full tests scans | verify:all pass
```

## Evidence

- User chose `ReadVersion` as the public term and asked to finish the remaining
  checkpoint/retention problems.
- Current implementation keeps public `ReadVersion` separate from internal
  commit-number mechanics.
- Checkpoints are stored in manifest metadata for durable storage modes and in
  process-local metadata for in-memory databases.
- The retained floor is the oldest read version required by active snapshots,
  named checkpoints, or the configured recent-retention window.

## Backend Boundary Receipt

- Trine operation names: `create_checkpoint`, `delete_checkpoint`,
  `checkpoint_read_version`, `snapshot_at`, `oldest_retained_read_version`.
- Owned interface: `Db` public API, `DbOptions`, `ManifestState`.
- Chosen backend: manifest metadata for native/object/browser persistent
  databases; in-memory map for `DbOptions::memory`.
- Known backend limits: checkpoint names are database-local, non-empty, unique,
  and not lineage-portable.
- Leak-check scope: public docs and protocol must not expose internal commit
  allocation as the user model.
- Verification gate: focused checkpoint/retention tests plus full Rust gate.

## Known Residuals

- Decide later whether existing public `Sequence` methods become documented
  lower-level aliases or are moved out of user-facing docs before `1.0`.

## Next Recommendation

- Avoid extending historical-read scope unless user evidence calls for
  checkpoint replacement, time-based retention, or lineage mapping.
