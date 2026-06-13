# V1 Benchmark Baseline Refresh

Date: 2026-06-13

This phase refreshed the benchmark harness so future performance work can use
grouped, multi-run evidence instead of a single local run.

## Commands

Default single-run output remains compatible with the existing CSV shape:

```text
cargo bench --bench v1_bench
```

Set `TRINE_BENCH_RUNS` to emit a grouped summary with min, median, and max
values:

```text
TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench
```

The summary columns are:

```text
group,name,runs,iterations,elapsed_us_min,elapsed_us_median,elapsed_us_max,units_per_sec_median,value_min,value_median,value_max
```

For normal benchmark rows, `value_*` is the checksum. For diagnostic rows,
`value_*` is the diagnostic counter or timing value carried by that row.

## Local 3-Run Baseline

Selected median rows from the local grouped run:

| group | row | median_us |
| --- | --- | ---: |
| compaction | compaction throughput | 178000 |
| blob-large-values | blob level merge | 170524 |
| blob-large-values | blob GC rewrite | 169310 |
| blob-large-values | separated blob values | 109257 |
| startup-recovery | cold table read | 83592 |
| startup-recovery | cold table read-only | 71677 |
| writes-flush | flush throughput | 65977 |
| startup-recovery | WAL replay read-only | 27622 |
| startup-recovery | WAL replay | 27562 |
| blob-large-values | blob range scan | 23197 |
| blob-large-values | blob point read | 23146 |
| mvcc-transactions | snapshot read under concurrent writes | 16181 |
| writes-flush | single-key put | 11472 |
| scans | prefix scan | 7630 |
| cache-decode | inline runtime block decode read | 6833 |
| cache-decode | native runtime block decode read | 6369 |
| scans | bounded range scan | 3991 |
| point-reads | batched point read memory | 2590 |
| point-reads | batched point read persistent | 1459 |
| cache-decode | block cache warm read | 1159 |
| point-reads | localized batched point read persistent | 1131 |
| point-reads | random get | 1059 |

Selected diagnostic medians:

| diagnostic | median_value |
| --- | ---: |
| WAL replay writable open wall micros | 21372 |
| WAL replay read-only open wall micros | 17448 |
| WAL replay writable open storage acquire writer lease micros | 2371 |
| WAL replay writable open storage read object bytes micros | 1936 |
| localized point diagnostic batch 16 wall micros | 1126 |
| read pruning prefix matching block metadata probes | 472 |
| read pruning prefix matching data block reads | 152 |

## Interpretation

The largest grouped costs are in compaction and blob maintenance:

- `compaction throughput`: 178000 us.
- `blob level merge`: 170524 us.
- `blob GC rewrite`: 169310 us.
- `separated blob values`: 109257 us.

Startup and recovery remain important, especially cold table open/read, but the
current 3-run median puts `WAL replay` below the maintenance-heavy rows after
the writer-lease fix.

Point-read rows, including localized batched point reads, are not the next
largest target in this baseline. Range and prefix scans are still worth future
work, but they are also below compaction/blob and cold startup costs in this
run.

## Recommendation

Start the next optimization phase by decomposing compaction and blob
maintenance write amplification. The phase should measure output table/blob
write time, manifest publish time, directory sync time, obsolete cleanup, blob
GC candidate selection, and blob level-merge rewrite cost before changing
behavior.
