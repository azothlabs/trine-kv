# Current Phase

## Status

Complete

## Goal

Replace eager range/prefix result building with a lazy seek cursor that merges
memtable and SSTable records under MVCC visibility.

## Entry Condition

- Phase 13 Rust 1.85 CI compatibility fix is complete.
- User review identifies eager range iteration as the wrong engine shape:
  range iterators should seek to the start and advance row by row instead of
  prebuilding a `Vec<KeyValue>`.

## Scope

- Change `Iter` so range/prefix scans can own lazy source cursors.
- Add memtable and SSTable point-record cursors with forward and reverse scan
  support.
- Merge source groups by user key, then apply MVCC point and range-delete
  visibility before returning each row.
- Keep point reads on the existing focused lookup path.
- Add a focused test proving range construction does not touch table blocks.

## Out Of Scope

- Changing public scan semantics.
- Changing SSTable, manifest, WAL, blob, or recovery file formats.
- Reworking compaction planning or transaction conflict detection.
- Publishing the crate.

## Acceptance Gate

- Range and prefix scans no longer call the eager visible-range builder.
- The lazy cursor preserves ordering, snapshot visibility, point deletes, range
  deletes, blob values, and reverse scans.
- A focused test shows table block-cache access starts on `next()`, not at
  iterator construction.
- Local verification passes for `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-targets --all-features`, examples, and `git diff --check`.

## Active Task Slice

```text
task049 [x] goal:range/prefix iterator advances lazily over merged source cursors | scope:src/iterator.rs,src/db.rs,src/table.rs,tests,.phrase | verify:focused lazy test + full Rust gate
```

## Known Blockers

- GitHub Actions cannot be executed locally in this environment; remote CI must
  run after push.
- The local default toolchain is Rust 1.87; remote CI remains the exact Rust
  1.85 proof after push.
- Tables are currently fully loaded into memory, so this phase makes scan
  advancement lazy over table records and blob value reads, not an on-demand
  file-block decoder redesign.

## Evidence To Record

- `Iter` owns lazy source cursors for range/prefix scans; point reads keep the
  focused lookup path.
- `persistent_range_iterator_defers_table_block_reads_until_next` proves range
  construction does not touch table blocks.
- The full local Rust gate passed on Rust 1.87; remote CI remains the exact
  Rust 1.85 proof after push.

## Next Recommendation

- If this phase passes, consider adding a CI stable-matrix job while keeping the
  Rust 1.85 MSRV gate.
