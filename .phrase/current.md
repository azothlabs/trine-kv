# Current Phase

## Status

In Progress

## Goal

Separate the internal LSM core from database-wide coordination without changing
public API behavior or storage formats.

## Entry Condition

- Phase 20 iterator merge and background maintenance passed locally.
- User identified that the current tree data structure and database layer are
  too tightly mixed.
- Current evidence shows `db.rs` still owns tree state, read visibility,
  tombstone checks, flush input selection, and compaction retention helpers.

## Scope

- Use `.phrase/protocol/lsm-core-boundary-spec.md` as the source of truth for
  the extraction.
- Keep WAL, manifest publish, process lock, recovery, background worker
  lifecycle, and cross-keyspace batch coordination in the database layer.
- Move one-keyspace tree state and tree-local rules behind an internal
  `LsmTree` boundary.
- Keep MVCC read visibility, range tombstone checks, scan grouping, transaction
  conflict checks, flush planning, and compaction retention inside LSM core.
- Preserve in-memory mode as the same logical engine over volatile storage.
- Keep public API and storage format unchanged.

## Out Of Scope

- Public API redesign.
- Storage format changes.
- WAL or manifest format changes.
- New compression codecs.
- Replacing the background worker model.
- Cross-keyspace compaction.

## Acceptance Gate

- The boundary spec is written and linked from the v1 protocol.
- `src/lsm/` exists and owns the first tree-local state boundary.
- `Db` keeps database-wide coordination but no longer owns the first extracted
  tree rules for the task slice.
- Full local Rust verification passes after each code slice.
- Evidence records what moved and what remains in DB for the next phase.

## Active Task Slice

```text
task068 [x] goal:write complete LSM core boundary spec | scope:.phrase/protocol,.phrase/current.md,.phrase/roadmap.md | verify:protocol link and doc diff checks
task069 [ ] goal:create internal LSM module and move tree state boundary | scope:src/lsm,src/db.rs,src/lib.rs | verify:cargo test --all-targets --all-features
task070 [ ] goal:move point read visibility into LsmTree | scope:src/lsm,src/db.rs,tests | verify:point read, tombstone, transaction, persistent tests
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.
- `AGENTS.md` has a pre-existing unstaged edit outside this phase.
- Later slices still need range/prefix scans, flush, compaction, and transaction
  conflict checks moved into LSM core after the first boundary is stable.

## Evidence To Record

- Boundary spec path and protocol link.
- First code slice diff proving DB delegates tree-owned behavior to `LsmTree`.
- Full local verification for each implementation slice.

## Next Recommendation

- Start task069 by introducing `src/lsm/` and moving tree state behind
  `LsmTree` without changing behavior.
