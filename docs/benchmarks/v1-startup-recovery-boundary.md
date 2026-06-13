# V1 Startup And Recovery Benchmark Boundary

Date: 2026-06-13

This phase revisited the startup, reopen, and recovery rows after the grouped
benchmark refresh.

## Measurement Fix

The previous `cold table read`, `cold table read-only`, `WAL replay`, and
`WAL replay read-only` rows included setup work inside the measured closure:

- cold table rows created a database, wrote 1024 rows, and flushed before the
  repeated reopen/read loop;
- WAL replay rows created the WAL test directory before opening it.

Those rows were useful for broad end-to-end smoke checks, but they overstated
startup/recovery cost and could push later optimization work toward setup
costs instead of open/replay/read costs.

The benchmark now prepares the database or WAL directory before calling
`measure`, so the startup/recovery rows measure the reopen/replay/read path.
The harness also reports wall-time diagnostics for cold open, first read, and
close.

## Local 3-Run Result

Command:

```text
TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
```

Selected grouped medians after the measurement fix:

| row | median_us |
| --- | ---: |
| WAL replay | 852 |
| WAL replay read-only | 581 |
| cold table read | 17668 |
| cold table read-only | 3772 |
| cold table open wall micros | 6996 |
| cold table first read wall micros | 1489 |
| cold table close wall micros | 9918 |
| cold table read-only open wall micros | 2432 |
| cold table read-only first read wall micros | 1531 |
| cold table read-only close wall micros | 112 |

Selected storage-operation medians for 32 cold writable opens:

| diagnostic | median_value |
| --- | ---: |
| read pruning cold open phase storage acquire writer lease micros | 1937 |
| read pruning cold open phase storage list directory files micros | 1042 |
| read pruning cold open phase storage read object bytes micros | 1225 |
| read pruning cold open phase storage current manifest micros | 342 |

## Rejected Optimization

The writable close/drop path still has visible cost because the writer lease
drop path reads the `LOCK` owner text before removing the file. A Unix-only
unlink-while-open fast path was tested, but the existing
`native_file_writer_lease_does_not_remove_changed_marker` regression test
correctly rejected it: if another actor changes the marker, Trine must not
remove it on drop.

That safety rule is more important than this close-path micro-optimization, so
no writer-lease behavior change was retained.

## Interpretation

Startup/recovery is no longer the next largest optimization area after the
measurement boundary fix. The corrected `WAL replay` rows are below 1 ms in
this local benchmark, and read-only cold reopen plus first read is below 4 ms.

Writable cold reopen remains higher mostly because it includes writer-lease
acquire and close/drop behavior. That cost is real, but the obvious shortcut
would weaken the tested fail-closed writer-lease contract, so it should not be
kept without a different safety proof.

## Verification

- `cargo fmt --check`
- `cargo test -q writer_lease --lib`
- `cargo test -q read_only --lib`
- `cargo clippy --bench v1_bench -- -D warnings`
- `TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench`
