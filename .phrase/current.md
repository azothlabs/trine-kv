# Current Phase

## Status

Complete

## Goal

Complete Phase F from `.phrase/protocol/guard-aware-lsm-strategy.md`: decide
whether guard information should stay derived in memory at open or become
persisted metadata. Phase F is a decision gate, not a default implementation
phase; "complete" means making the evidence-backed decision and recording it.

## Design Assessment

Phase F is resolved as **keep guards derived**. Guards are simply table key
bounds, which the manifest table record already persists. On open, recovery
reads `manifest.tables()` (properties with bounds) and `LsmVersion::new` only
sorts the tables into the level layout. There is no separate guard derivation
pass and no extra I/O for guards, so guards are already implicitly durable.

Adding a separate persisted guard structure would duplicate the manifest's
bounds while adding format-version, migration, and recovery-validation burden.
The Phase F entry condition (deriving guards became a measured open/recovery
cost) is not met, so no format change is made.

## Scope

- Evidence: confirm guard bounds are manifest-persisted and measure open cost.
- Decision recorded in `decision.md` (durable boundary) and
  `guard-aware-lsm-strategy.md` (Phase F resolved).
- No Rust behavior or storage-format change.

## Out Of Scope

- Any manifest/SSTable/blob format change.
- A new persisted guard-metadata structure.
- Manifest-side pre-sorted level cache (only revisited if `LsmVersion` build is
  later measured to dominate open).

## Acceptance Gate

- No format change occurs without a protocol update and migration/recovery
  tests. Met: the decision is to make no format change.
- The decision is evidence-backed and recorded in the durable docs.

## Evidence

- Manifest table record already encodes/decodes `smallest_user_key` and
  `largest_user_key` (`src/manifest.rs`), so guard bounds are persisted.
- `LsmVersion::new` (`src/lsm/version.rs`) only groups and sorts tables into
  levels; recovery sources bounds from `manifest.tables()` (`src/recovery.rs`).
- Benchmark startup-recovery: cold table open ~6.7 ms writable / ~2.0 ms
  read-only, WAL replay open well under 1 ms; open is dominated by metadata I/O
  and WAL replay, not the `LsmVersion` sort.

## Known Risks

- The decision relies on the current open path; if table counts grow very large
  and `LsmVersion` build becomes a measured open bottleneck, a manifest-side
  pre-sorted level cache (not a new guard format) would be the first response,
  still gated by a protocol update and migration/recovery tests.

## Next Recommendation

- The guard-aware LSM strategy (Phases A-F) is complete. Pick the next KV engine
  phase from fresh benchmark evidence, e.g. single-key sync-write fsync cost or
  compaction/blob rewrite throughput.
