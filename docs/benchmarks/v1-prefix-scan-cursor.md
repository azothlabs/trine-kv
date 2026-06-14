# V1 Prefix Scan Cursor Metadata Reuse

Date: 2026-06-14

## Scope

This note records Phase 178. The phase measured range/prefix scan behavior and
kept one table-prefix cursor optimization that does not change storage formats,
prefix-filter semantics, MVCC visibility, or range-delete behavior.

## Measurement

The persistent prefix table-partition benchmark was the clearest scan-path
target in this phase.

Before the change:

- `prefix scan table partitions matching`: 3052 us median.
- `read pruning prefix matching block metadata probes`: 472.
- `read pruning prefix matching data block reads`: 152.

After the change:

- `prefix scan table partitions matching`: 2880 us median.
- `read pruning prefix matching block metadata probes`: 320.
- `read pruning prefix matching data block reads`: 152.

## Change

The table prefix cursor already inspected block metadata and the block prefix
filter before deciding to read a data block. After loading the block, it used
to inspect block metadata again and scan the decoded block only to decide
whether to record a block-prefix-filter false positive.

The cursor now carries the block prefix-filter state across that boundary. It
marks whether the loaded block actually returned a matching record, then records
a false positive when the cursor leaves a block with a prefix filter that
allowed the scan but produced no matching records.

## Interpretation

The optimization removes redundant metadata and decoded-block checking from
persistent table prefix scans. It does not reduce the number of data blocks
needed for the matching prefix workload, and it does not apply to the general
in-memory `prefix scan` benchmark.

## Verification

- `cargo fmt --check`
- `cargo test -q prefix --lib`
- `cargo test -q range --lib`
- `cargo clippy --lib -- -D warnings`
- `cargo test -q --lib`
- `cargo test -q persistent_prefix`
- `cargo test -q persistent_async_range_and_prefix_advance_flushed_tables`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench`
- `git diff --check`
