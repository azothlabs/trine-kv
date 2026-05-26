# Trine KV V1 Benchmark Baseline

Date: 2026-05-25; updated for partitioned table indexes on 2026-05-26

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

Only linear, binary, and auto index seek policy rows are reported. Earlier
Eytzinger/Galloping labels were removed because they did not have distinct
implemented search layouts.

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| single-key put | 2048 | 2992 | 684291 | 40599 |
| batch write | 1024 | 843 | 1213510 | 1024 |
| random get | 2048 | 1488 | 1376112 | 40230 |
| missing get | 2048 | 665 | 3077575 | 0 |
| bounded range scan | 128 | 3166 | 40429 | 77253 |
| prefix scan | 128 | 6728 | 19023 | 160952 |
| prefix scan table partitions matching | 128 | 4522 | 28302 | 160952 |
| prefix scan table partitions nonmatching | 128 | 82 | 1542930 | 0 |
| snapshot read under concurrent writes | 2048 | 3300 | 620590 | 40238 |
| optimistic transaction commit | 512 | 1661 | 308209 | 9879 |
| optimistic transaction conflict | 512 | 2107 | 242941 | 512 |
| WAL replay | 1024 | 39429 | 25970 | 20 |
| flush throughput | 1024 | 50759 | 20173 | 33627 |
| compaction throughput | 1024 | 129002 | 7937 | 33635 |
| large inline values | 256 | 704 | 363206 | 4194304 |
| separated blob values | 256 | 77385 | 3308 | 4194304 |
| blob point read | 256 | 13995 | 18291 | 4194304 |
| blob range scan | 32 | 13332 | 2400 | 4194304 |
| blob range lazy keys | 32 | 248 | 129032 | 3072 |
| blob GC rewrite | 128 | 125602 | 1019 | 4194304 |
| blob level merge | 128 | 139850 | 915 | 2097152 |
| block cache warm read | 2048 | 2856 | 716888 | 40960 |
| cold table read | 32 | 174357 | 183 | 640 |
| index seek policy linear small | 2048 | 5630 | 363765 | 37696 |
| index seek policy binary small | 2048 | 5461 | 375002 | 37696 |
| index seek policy auto small | 2048 | 5647 | 362648 | 37696 |
| index seek policy linear medium | 2048 | 20510 | 99851 | 40238 |
| index seek policy binary medium | 2048 | 16731 | 122401 | 40238 |
| index seek policy auto medium | 2048 | 15920 | 128638 | 40238 |
| index seek policy linear large | 2048 | 20507 | 99866 | 42020 |
| index seek policy binary large | 2048 | 20109 | 101842 | 42020 |
| index seek policy auto large | 2048 | 20048 | 102154 | 42020 |
| iterator advance_to near targets | 2048 | 50 | 40892119 | 2098176 |
| iterator advance_to far targets | 2048 | 6 | 341333333 | 16041075 |
| iterator advance_to random targets | 2048 | 3 | 646873025 | 16740817 |
| codec none Trine data blocks | 2048 | 250 | 8192000 | 16777216 |
| codec fast block compression Trine data blocks | 2048 | 4490 | 456044 | 8464384 |
| codec none Trine index blocks | 2048 | 194 | 10556701 | 8388608 |
| codec fast block compression Trine index blocks | 2048 | 2317 | 883758 | 4255744 |
| codec none Trine range tombstone blocks | 2048 | 180 | 11377777 | 8388608 |
| codec fast block compression Trine range tombstone blocks | 2048 | 896 | 2284546 | 4265984 |
