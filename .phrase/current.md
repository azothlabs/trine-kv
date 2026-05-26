# Current Phase

## Status

In Progress

## Goal

Harden filter strategy behavior after the SSTable read path grew sharper block
lookup, cache keys, cache replacement, and file-handle reuse.

## Entry Condition

- Phase 24 completed data-block point lookup indexing, richer cache key
  classes, cache hit promotion, and persistent table file-handle reuse.
- User review identified P5 as the next LSM tree improvement after SSTable
  read-path detail hardening.

## Scope

- Audit table-level and per-block whole-key filters, prefix filters, and current
  stats boundaries.
- Make filter hit/miss behavior observable enough to tune later with benchmark
  evidence.
- Preserve current table format unless the audit proves a format change is
  necessary and protocol docs are updated first.
- Keep prefix scans able to skip unrelated tables/blocks when the configured
  prefix extractor matches.

## Out Of Scope

- Public API redesign.
- WAL or manifest format changes.
- Compaction picker rewrite.
- Blob GC.
- Benchmark-driven policy defaults without new benchmark evidence.
- Format changes without a protocol update.

## Acceptance Gate

- Filter stats distinguish table/block filter hits and misses for point and
  prefix reads.
- Prefix filter tests prove nonmatching prefixes skip data-block reads when the
  extractor matches.
- False positives are counted only when a filter allows a candidate but the
  checked block/table yields no matching user key.
- Existing public API and storage formats remain unchanged unless protocol docs
  are updated first.
- Full local Rust verification passes.

## Active Task Slice

```text
task086 [ ] goal:audit filter read-path and stats gaps | scope:src/filter.rs,src/table.rs,src/stats.rs,tests | verify:evidence note with exact blockers
task087 [ ] goal:add filter hit/miss/false-positive counters | scope:src/cache.rs,src/table.rs,src/stats.rs,tests | verify:stats-focused point/prefix tests
task088 [ ] goal:strengthen prefix filter skip behavior | scope:src/table.rs,tests | verify:nonmatching prefix avoids data-block reads
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.
- Any on-disk format change must update protocol docs first; current phase
  should avoid that unless evidence proves it necessary.

## Evidence

- Phase 24 full local verification passed.
- Existing Bloom implementations are real bitset filters; the next gap is
  observability and stronger skip-path proof, not replacing fake filters.
- Table-level filters and per-block filters already exist; stats do not yet
  expose filter hit/miss/false-positive behavior.

## Next Recommendation

- Start task086 with a focused filter read-path audit before changing stats or
  prefix-scan behavior.
