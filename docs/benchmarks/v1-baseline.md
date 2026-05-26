# Trine KV V1 Benchmark Baseline

Date: 2026-05-25; large-value read rows extended on 2026-05-26

Command:

```text
cargo bench --bench v1_bench
```

Harness inputs:

- rows: 1024
- ops: 2048
- build profile: Cargo bench release profile
- storage: in-memory and temporary persistent directories under the OS temp dir

The numbers below are a local baseline for comparing future engine changes on
the same machine. Checksums make each workload do observable work and help catch
accidental no-op rewrites of the harness.

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| single-key put | 2048 | 1528 | 1340167 | 40599 |
| batch write | 1024 | 489 | 2091930 | 1024 |
| random get | 2048 | 8496 | 241041 | 40230 |
| missing get | 2048 | 7739 | 264616 | 0 |
| bounded range scan | 128 | 1638 | 78128 | 77253 |
| prefix scan | 128 | 2329 | 54949 | 160952 |
| prefix scan table partitions matching | 128 | 1900 | 67349 | 160952 |
| prefix scan table partitions nonmatching | 128 | 17 | 7331462 | 0 |
| snapshot read under concurrent writes | 2048 | 19590 | 104539 | 40238 |
| optimistic transaction commit | 512 | 6253 | 81872 | 9879 |
| optimistic transaction conflict | 512 | 6633 | 77182 | 512 |
| WAL replay | 1024 | 22764 | 44982 | 20 |
| flush throughput | 1024 | 26069 | 39279 | 29965 |
| compaction throughput | 1024 | 51923 | 19721 | 29972 |
| large inline values | 256 | 537 | 476463 | 4194304 |
| separated blob values | 256 | 38302 | 6683 | 4194304 |
| blob point read | 256 | 13976 | 18316 | 4194304 |
| blob range scan | 32 | 13719 | 2332 | 4194304 |
| blob GC rewrite | 128 | 153518 | 833 | 4194304 |
| block cache warm read | 2048 | 1443 | 1418322 | 40960 |
| cold table read | 32 | 156116 | 204 | 640 |
| index seek policy linear small | 2048 | 1334 | 1535040 | 37696 |
| index seek policy binary small | 2048 | 1347 | 1519851 | 37696 |
| index seek policy eytzinger small | 2048 | 1384 | 1479411 | 37696 |
| index seek policy galloping small | 2048 | 1376 | 1488281 | 37696 |
| index seek policy auto small | 2048 | 1329 | 1540526 | 37696 |
| index seek policy linear medium | 2048 | 1560 | 1312435 | 40238 |
| index seek policy binary medium | 2048 | 1621 | 1262865 | 40238 |
| index seek policy eytzinger medium | 2048 | 1607 | 1273994 | 40238 |
| index seek policy galloping medium | 2048 | 1613 | 1269028 | 40238 |
| index seek policy auto medium | 2048 | 1557 | 1314541 | 40238 |
| index seek policy linear large | 2048 | 2425 | 844434 | 42020 |
| index seek policy binary large | 2048 | 1726 | 1185986 | 42020 |
| index seek policy eytzinger large | 2048 | 1774 | 1154073 | 42020 |
| index seek policy galloping large | 2048 | 1730 | 1183416 | 42020 |
| index seek policy auto large | 2048 | 1700 | 1204498 | 42020 |
| iterator advance_to near targets | 2048 | 56 | 36408888 | 2098176 |
| iterator advance_to far targets | 2048 | 6 | 303407407 | 16041075 |
| iterator advance_to random targets | 2048 | 3 | 564965517 | 16740817 |
| codec none Trine data blocks | 2048 | 278 | 7355872 | 16777216 |
| codec fast block compression Trine data blocks | 2048 | 4628 | 442511 | 8464384 |
| codec none Trine index blocks | 2048 | 181 | 11309668 | 8388608 |
| codec fast block compression Trine index blocks | 2048 | 2514 | 814557 | 4255744 |
| codec none Trine range tombstone blocks | 2048 | 182 | 11206566 | 8388608 |
| codec fast block compression Trine range tombstone blocks | 2048 | 956 | 2141605 | 4265984 |
