# Current Phase

## Status

Complete

## Goal

Harden the table read path so point lookups use a real data-block hash index,
unsupported search-policy names disappear, and large tables no longer load the
full block index/filter metadata at open.

## Entry Condition

- Phase 39 completed automatic blob maintenance policy.
- User requested the remaining table read-path hardening before release:
  block hash index, no fake Eytzinger/Galloping switches, and partitioned
  index/filter loading.

## Scope

- Encode and decode a data-block hash index that maps user-key hash to record
  ranges and uses key comparison only to resolve hash collisions.
- Remove `Eytzinger` and `GallopingWithHint` from the public search-policy
  surface unless fresh benchmark evidence justifies a real implementation.
- Keep old manifest tags readable by mapping retired search policies to `Auto`.
- Store table block index/filter metadata in partition blocks behind a small
  top-level index.
- Load index partitions lazily for point/range/prefix reads.
- Update protocol, usage docs, benchmarks, tests, roadmap, and evidence.

## Out Of Scope

- New public tuning knobs for index partition size.
- A separate index-block cache shared across tables.
- Changing WAL, MVCC, blob, or compaction semantics.

## Acceptance Gate

- Point reads inside a decoded data block do not fall back to binary search for
  normal key lookup.
- Retired search-policy manifest tags remain readable.
- Benchmarks no longer advertise Eytzinger/Galloping rows.
- Persistent open reads only footer/properties/top-level index metadata, not
  every block filter/index partition.
- Filter misses can skip data blocks using lazily loaded partition filters.
- Full local Rust verification passes.

## Active Task Slice

```text
task135 [x] goal:on-disk block hash lookup | scope:src/table.rs tests | verify:data_block_point_lookup_uses_hash_index
task136 [x] goal:remove fake search policies | scope:options manifest benches docs tests | verify:manifest legacy tag test + bench rows
task137 [x] goal:lazy partitioned index/filter | scope:src/table.rs persistent tests | verify:filter miss skips data block and partition metadata loads lazily
task138 [x] goal:update evidence and release gate | scope:.phrase docs | verify:full Rust verification
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.

## Evidence

- Rust skill, SPEC-AGENTS context, and the coding module were read before
  implementation.
- Data blocks now encode a checked user-key hash index. Point lookup uses the
  hash index to find candidate record ranges and compares keys only to handle
  hash collisions.
- `Eytzinger` and `GallopingWithHint` were removed from the public
  `IndexSearchPolicy` surface. Retired manifest tags `2` and `3` decode to
  `Auto`.
- Persistent table open now reads footer, properties, and the small top-level
  index. Per-partition block index/filter metadata is loaded on demand.
- Full table point/prefix filters are not kept in persistent table handles;
  per-block filters inside lazily loaded index partitions still skip data
  blocks on misses.
- `cargo bench --bench v1_bench` reports only linear, binary, and auto search
  policy rows.
- `cargo test --all-targets --all-features`, `cargo clippy --all-targets
  --all-features -- -D warnings`, `cargo fmt --all --check`,
  `cargo bench --bench v1_bench`, `git diff --check`, and the
  forbidden-term scan pass locally.

## Next Recommendation

- Commit Phase 40, then use remote CI as the final external release signal
  after push.
