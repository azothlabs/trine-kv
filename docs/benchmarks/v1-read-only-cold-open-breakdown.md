# Trine KV V1 Read-Only Cold Open Breakdown

Date: 2026-06-03

Command:

```text
cargo bench --bench v1_bench > /tmp/trine-v1-bench-phase144-breakdown.txt
cargo bench --bench v1_bench > /tmp/trine-v1-bench-clean-wal.txt
```

Harness inputs:

- rows: 1024
- ops: 2048
- build profile: Cargo bench release profile
- storage: temporary persistent directories under the OS temp dir

## Change Measured

This phase split cold reopen diagnostics into:

- total reopen plus first point read,
- open phase before the first point read,
- first read phase after open.

The benchmark harness keeps the existing writable and read-only cold table read
rows, then adds diagnostic rows for both phases. This is diagnostic coverage
only; no public API, storage behavior, recovery behavior, WAL format, manifest
format, or table format changed.

## Release Benchmark Rows

After adding split diagnostics:

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| cold table read | 32 | 202052 | 158 | 640 |
| cold table read-only | 32 | 94942 | 337 | 640 |
| read pruning cold read-only open phase storage read object bytes requests | 1 | 0 | 0 | 128 |
| read pruning cold read-only open phase storage current manifest requests | 1 | 0 | 0 | 32 |
| read pruning cold read-only open phase storage acquire writer lease requests | 1 | 0 | 0 | 0 |
| read pruning cold read-only first read phase storage read owned requests | 1 | 0 | 0 | 64 |
| read pruning cold read-only first read phase point data block reads | 1 | 0 | 0 | 32 |
| read pruning cold read-only first read phase storage read object bytes requests | 1 | 0 | 0 | 0 |

After the clean-WAL read-only open change:

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| cold table read | 32 | 211282 | 151 | 640 |
| cold table read-only | 32 | 91081 | 351 | 640 |
| read pruning cold read-only open phase storage open read requests | 1 | 0 | 0 | 32 |
| read pruning cold read-only open phase storage len requests | 1 | 0 | 0 | 32 |
| read pruning cold read-only open phase storage read owned requests | 1 | 0 | 0 | 32 |
| read pruning cold read-only open phase storage read object bytes requests | 1 | 0 | 0 | 0 |
| read pruning cold read-only open phase storage current manifest requests | 1 | 0 | 0 | 32 |
| read pruning cold read-only open phase storage list directory files requests | 1 | 0 | 0 | 32 |
| read pruning cold read-only first read phase storage read object bytes requests | 1 | 0 | 0 | 0 |

## Interpretation

- Read-only cold reopen remains faster than writable cold reopen because it
  still avoids writer-lease acquisition.
- Before the clean-WAL change, open phase carried 128 whole-object reads across
  32 read-only opens. One whole-object read per open was the current manifest;
  code inspection mapped the remaining whole-object reads to WAL shard reads.
- Clean-WAL read-only open now uses the directory listing's file length when it
  is available and skips WAL shard content reads only when every discovered WAL
  shard has zero bytes. The measured read-only open phase now reports zero
  whole-object reads without increasing open-read or length request counts.
- First read phase does not add whole-object reads. It adds table probes, block
  metadata probes, one data-block read per operation, and positioned reads for
  the lazy table data block.
- The release row moved from `cold table read-only` at 94942 us in the split
  diagnostic run to 91081 us after clean-WAL read-only open. Local cold-read
  elapsed time remains noisy, so the request-count reduction is the stronger
  evidence.
- The async native open path now reuses the same directory file list for safe
  temporary-file repair, unreferenced-file checks, WAL shard discovery, and
  clean-WAL length proof. Focused async tests cover both zero whole-object
  reads after a clean flush and normal replay when WAL shards are non-empty.
- Table data-block work is still visible, but it belongs to first point read
  rather than open-time fixed cost.

## Verification

- `cargo fmt --check`
- `cargo test -q read_only --lib`
- `cargo test -q --bench v1_bench`
- `cargo bench --bench v1_bench`, with output redirected and only key rows
  inspected.
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test -q --all-targets --all-features`
- `git diff --check`
- Forbidden-term and source-name scans over touched source, benchmark,
  documentation, and phase files.
