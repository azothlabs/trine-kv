# Current Phase

## Status

In Progress

## Goal

Harden memtable and flush scheduling so one hot keyspace does not force every
tree to move together, and so write-buffer pressure is tracked without scanning
whole memtables.

## Entry Condition

- Phase 22 completed the versioned LSM level layout.
- User review identified P3 as the next LSM tree improvement after P0/P1/P2:
  memtable bytes, keyspace-local freeze, clearer immutable queue pressure,
  foreground/background flush boundaries, and in-memory mode parity.

## Scope

- Maintain memtable byte estimates incrementally on write and freeze.
- Keep freeze decisions keyspace-local.
- Make immutable memtable queue pressure and write backpressure explicit.
- Keep active memtable and active range tombstones freezing as one unit.
- Keep in-memory mode on the same logical LSM path.
- Add focused tests before behavior changes.

## Out Of Scope

- Public API redesign.
- Storage format changes.
- WAL or manifest format changes.
- SSTable hash indexes, fd cache, partitioned metadata loading, blob GC, and
  full compaction picker rewrite.
- Replacing the current background worker model beyond what this phase needs to
  clarify flush scheduling.

## Acceptance Gate

- Memtable byte accounting no longer needs whole-map scans on normal writes.
- A hot keyspace freeze/flush decision does not freeze unrelated keyspaces.
- Immutable memtable queue pressure has tested behavior when the configured
  limit is reached.
- In-memory mode follows the same memtable/freeze/read path as persistent mode.
- Existing public API and storage formats remain unchanged.
- Full local Rust verification passes.

## Active Task Slice

```text
task080 [ ] goal:audit current memtable byte and freeze path | scope:src/lsm/write.rs,src/lsm/flush.rs,src/db.rs,tests | verify:evidence note with exact blockers
task081 [ ] goal:incrementally maintain memtable byte estimates | scope:src/memtable.rs,src/lsm/write.rs,tests | verify:unit tests plus persistent write-buffer tests
task082 [ ] goal:make immutable queue pressure explicit | scope:src/lsm/write.rs,src/db.rs,tests | verify:pressure/backpressure regression tests
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.
- Current background maintenance error boundaries must stay compatible with
  existing public methods.

## Evidence

- Phase 22 introduced `LsmVersion`/`LevelState`, version-swap publish, one
  read-held version handle for table layout, and old compacted-table file
  lifetime protection for lazy readers.
- User review identified `write_buffer_bytes`, `max_immutable_memtables`, and
  background flush behavior as the next P3 risk area.

## Next Recommendation

- Start task080 with a small code audit and evidence note before changing write
  or flush behavior.
