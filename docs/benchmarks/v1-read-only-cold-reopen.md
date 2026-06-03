# Trine KV V1 Read-Only Cold Reopen

Date: 2026-06-03

Command:

```text
cargo bench --bench v1_bench > /tmp/trine-v1-bench-phase143-read-only.txt
```

Harness inputs:

- rows: 1024
- ops: 2048
- build profile: Cargo bench release profile
- storage: temporary persistent directories under the OS temp dir

## Change Measured

Native persistent read-only open already existed through
`DbOptions::persistent_read_only(path)` and `DbOptions::read_only()`. This phase
made that path visible in the benchmark harness by adding a read-only cold table
read row and matching cold-read diagnostic rows.

No storage format, WAL, manifest, table, blob, compaction, transaction, or
writable-open behavior changed. Writable opens still acquire the writer lease.
Read-only opens skip writer-lease acquisition, do not create a WAL writer, do
not start background workers, and reject writes.

## Release Benchmark Rows

After adding the read-only benchmark rows:

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| cold table read | 32 | 215780 | 148 | 640 |
| cold table read-only | 32 | 93188 | 343 | 640 |
| read pruning cold storage acquire writer lease requests | 1 | 0 | 0 | 32 |
| read pruning cold read-only storage acquire writer lease requests | 1 | 0 | 0 | 0 |
| read pruning cold storage acquire writer lease micros | 1 | 0 | 0 | 101923 |
| read pruning cold read-only storage acquire writer lease micros | 1 | 0 | 0 | 0 |
| read pruning cold storage list directory files requests | 1 | 0 | 0 | 32 |
| read pruning cold read-only storage list directory files requests | 1 | 0 | 0 | 32 |
| read pruning cold storage read object bytes requests | 1 | 0 | 0 | 128 |
| read pruning cold read-only storage read object bytes requests | 1 | 0 | 0 | 128 |

## Interpretation

- Read-only cold reopen removed writer-lease acquisition from the measured
  read-only path.
- The benchmark row improved from 215780 us for writable cold reopen/get to
  93188 us for read-only cold reopen/get in this local release-profile run.
- Request-count evidence is stronger than elapsed time because cold filesystem
  timings are noisy.
- Remaining read-only cold cost is shared with writable open: manifest read,
  table open, directory listing, WAL replay into memory, and one lazy table
  data-block read for the point lookup.

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
