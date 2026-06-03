# Trine KV V1 Cold Manifest/Open Reopen

Date: 2026-06-03

Command:

```text
cargo bench --bench v1_bench > /tmp/trine-v1-bench-phase140-snapshot-after.txt
```

Harness inputs:

- rows: 1024
- ops: 2048
- build profile: Cargo bench release profile
- storage: temporary persistent directories under the OS temp dir

## Change Measured

Phase 139 reduced small table-open positioned reads, leaving reopen/open work as
the next cold-read target. This phase first extended the cold-read diagnostic
rows to report writer-lease, directory-listing, object-listing, whole-object
read, and per-operation latency totals.

The fresh before-change diagnostic run showed that 32 reopen/get operations
performed:

- 32 writer-lease acquisitions.
- 96 directory file-list requests.
- 64 object-list requests.
- 128 whole-object reads.

The kept change reads the native persistent directory once during sync open and
reuses that snapshot for safe temporary-file repair, WAL path discovery, and
unreferenced table/blob checks. It does not change writer-lease acquisition,
manifest decoding, WAL frame reads, table loading, or recovery failure
conditions.

## Release Benchmark Rows

Before-change discovery run:

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| cold table read | 32 | 177971 | 179 | 640 |
| read pruning cold storage acquire writer lease requests | 1 | 0 | 0 | 32 |
| read pruning cold storage list directory files requests | 1 | 0 | 0 | 96 |
| read pruning cold storage list objects requests | 1 | 0 | 0 | 64 |
| read pruning cold storage read object bytes requests | 1 | 0 | 0 | 128 |
| read pruning cold storage acquire writer lease micros | 1 | 0 | 0 | 72826 |
| read pruning cold storage list directory files micros | 1 | 0 | 0 | 2056 |
| read pruning cold storage list objects micros | 1 | 0 | 0 | 1087 |

After-change run:

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| cold table read | 32 | 168137 | 190 | 640 |
| read pruning cold storage acquire writer lease requests | 1 | 0 | 0 | 32 |
| read pruning cold storage list directory files requests | 1 | 0 | 0 | 32 |
| read pruning cold storage list objects requests | 1 | 0 | 0 | 0 |
| read pruning cold storage read object bytes requests | 1 | 0 | 0 | 128 |
| read pruning cold storage acquire writer lease micros | 1 | 0 | 0 | 78852 |
| read pruning cold storage list directory files micros | 1 | 0 | 0 | 919 |
| read pruning cold storage list objects micros | 1 | 0 | 0 | 0 |

## Interpretation

- The directory snapshot reduced directory scans from five to one per
  reopen/get.
- Object-list requests disappeared from the cold-read diagnostic path because
  table/blob unreferenced checks now consume the same directory snapshot.
- Writer-lease acquisition remains the largest measured fixed cost for writable
  cold reopen. The change intentionally leaves that safety boundary intact.
- Local elapsed time remains noisy, so the request-count reduction is the
  stronger evidence for keeping the change.

## Verification

- `cargo fmt --check`
- `cargo test -q recovery --lib`
- `cargo test -q wal --lib`
- `cargo test -q persistent --lib`
- `cargo test -q --bench v1_bench`
- `cargo bench --bench v1_bench`, with output redirected and only key rows
  inspected.
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test -q --all-targets --all-features`
- `git diff --check`
- Forbidden-term and source-name scans over touched design and benchmark
  records.
