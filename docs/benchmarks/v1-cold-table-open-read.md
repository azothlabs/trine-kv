# Trine KV V1 Cold Table Open Read

Date: 2026-06-03

Command:

```text
cargo bench --bench v1_bench > /tmp/trine-v1-bench-phase139-release-2.txt
```

Harness inputs:

- rows: 1024
- ops: 2048
- build profile: Cargo bench release profile
- storage: temporary persistent directories under the OS temp dir

## Change Measured

Phase 138 diagnostics showed that cold point reads were not filter-pruning
bound: each reopen/get performed one table probe, one block metadata probe, one
data-block read, and one cache miss.

This phase extended the cold-read diagnostic rows with storage operation
counts. Before the table-open change, 32 reopen/get operations performed 288
positioned owned reads. That is 9 positioned owned reads per reopen/get.

The kept change adds a sync table-open fast path for small table files. When
the table file is at or below 256 KiB, sync open reads the whole file into a
temporary buffer and decodes table metadata, filters, and index metadata from
that buffer. The opened table still keeps its native file handle and leaves
data blocks lazy, so point/range reads and block-cache behavior remain the same
shape as the normal persistent table path.

## Release Benchmark Rows

Second release-profile run after the change:

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| cold table read | 32 | 167430 | 191 | 640 |
| read pruning cold point table probes | 1 | 0 | 0 | 32 |
| read pruning cold point block metadata probes | 1 | 0 | 0 | 32 |
| read pruning cold point data block reads | 1 | 0 | 0 | 32 |
| read pruning cold point filter skips | 1 | 0 | 0 | 0 |
| read pruning cold point cache misses | 1 | 0 | 0 | 32 |
| read pruning cold storage read owned requests | 1 | 0 | 0 | 96 |
| read pruning cold storage read owned micros | 1 | 0 | 0 | 236 |
| read pruning cold storage current manifest micros | 1 | 0 | 0 | 493 |

The first release-profile run after the change recorded `cold table read` at
174933 us and the same 96 positioned owned read requests.

## Interpretation

- The table-open fast path reduced positioned owned read requests from 288 to
  96 for the 32 reopen/get diagnostic workload.
- The second release-profile run recorded `cold table read` at 167430 us,
  compared with the `0.1` baseline's 183876 us.
- The first after-change release run was 174933 us, so the local timing is
  noisy; request-count reduction is the stronger evidence for keeping the
  change.
- The remaining cold-read cost still includes current-manifest reads, table
  open/read work, and one data-block read per reopened database.

## Verification

- `cargo fmt --check`
- `cargo test -q table --lib`
- `cargo test -q --bench v1_bench`
- `cargo bench --bench v1_bench` twice, with output redirected and only key
  rows inspected.
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test -q --all-targets --all-features`
- `git diff --check`
- Forbidden-term and source-name scans over touched design and benchmark
  records.
