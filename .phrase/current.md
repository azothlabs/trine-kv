# Current Phase

## Status

Complete

## Goal

Replace exact-set table filters with real Bloom bitsets for point-key and
prefix filters while preserving the Phase 17 metadata/data-block read boundary.

## Entry Condition

- Phase 17 file-backed table reader passed locally.
- Evidence shows table-level and block-level filters still store complete key or
  prefix sets, so filter memory cost does not match the advertised Bloom
  policy.

## Scope

- Implement a compact Bloom bitset in `src/filter.rs`.
- Make `bits_per_key` and `bits_per_prefix` control filter bit count and hash
  count.
- Encode/decode Bloom bitsets in table filter/index blocks.
- Keep table-level and block-level point-key filters.
- Keep table-level and block-level prefix filters based on the configured
  prefix extractor.
- Keep filters advisory: false positives are allowed; false negatives fail
  closed during block validation.
- Preserve in-memory mode behavior.

## Out Of Scope

- Changing public filter policy names.
- Level scoring or streaming compaction.
- Range tombstone query indexes.
- Background worker scheduling.
- Rewriting iterator merge strategy.

## Acceptance Gate

- Filter objects no longer store every full key/prefix as a searchable set.
- Filter bitset size changes with `bits_per_key` / `bits_per_prefix`.
- Existing point/range/prefix reads continue to pass.
- A block filter missing one of its own keys/prefixes is rejected by validation.
- Formatting, clippy, and targeted/full tests pass.

## Active Task Slice

```text
task057 [x] goal:replace exact point-key filters with Bloom bitsets | scope:src/filter.rs,src/table.rs,tests | verify:filter sizing and round-trip tests
task058 [x] goal:replace exact prefix filters with Bloom bitsets | scope:src/filter.rs,src/table.rs,tests | verify:prefix filter read-path tests
task059 [x] goal:update docs and evidence for Bloom filter semantics | scope:.phrase | verify:phase evidence and protocol notes
```

## Known Blockers

- Compaction still builds complete input record lists and does not split output
  by target table size.
- Range tombstones still use a table-level on-demand list instead of a query
  structure.
- GitHub Actions cannot be executed locally; remote CI must run after push.

## Evidence To Record

- `point_filter_bit_count_tracks_bits_per_key` proves `bits_per_key` controls
  Bloom bit count and hash count.
- `point_filter_round_trips_from_parts` and
  `prefix_filter_uses_extractor_prefixes` prove point and prefix Bloom filters
  keep their own keys/prefixes across encode/decode.
- `data_block_filter_false_negative_fails_closed` and
  `prefix_block_filter_false_negative_fails_closed` prove block validation
  rejects filters that miss their own data.
- `persistent_filter_miss_does_not_read_corrupt_data_block` still passes with
  Bloom filters.
- Full local gate passed: `cargo check --all-targets --all-features`,
  `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D
  warnings`, and `cargo test --all-targets --all-features`.

## Next Recommendation

- If Bloom filters pass, move to compaction output sizing and level scoring, or
  range tombstone query structures if delete-heavy behavior is the sharper risk.
