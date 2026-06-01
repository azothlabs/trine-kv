# Trine KV V1 Delta Read Cost

Date: 2026-06-01

Command:

```text
cargo bench --bench v1_bench
```

Harness inputs:

- rows: 1024
- ops: 2048
- build profile: Cargo bench release profile
- storage: in-memory and temporary persistent directories under the OS temp dir

This run was recorded after in-memory writes stopped replaying into the active
memtable. The active-memtable rows use an unflushed persistent database as a
read-path comparison point. The merged-delta rows use an in-memory database
with a one-byte write buffer so every shard quickly merges its open epoch.

| name | iterations | elapsed_us | units_per_sec | checksum |
| --- | ---: | ---: | ---: | ---: |
| random get | 2048 | 1560 | 1312330 | 40230 |
| missing get | 2048 | 1221 | 1676341 | 0 |
| active memtable random get | 2048 | 768 | 2666232 | 40221 |
| merged delta random get | 2048 | 697 | 2934622 | 40221 |
| merged delta missing get | 2048 | 484 | 4223403 | 0 |
| bounded range scan | 128 | 6057 | 21132 | 77253 |
| active memtable range scan | 128 | 2040 | 62745 | 77253 |
| merged delta range scan | 128 | 3019 | 42393 | 77253 |

Interpretation:

- The merged-delta point-read row is in the same local range as the active
  memtable row.
- The merged-delta range row is about 1.5x the active-memtable range row,
  mostly because range scans still enumerate delta shard sources.
- The default in-memory rows remain materially slower than the merged-delta
  rows because the default open-epoch chain can leave multiple deltas per shard.

Recommended next action:

- Before WAL shard front doors, reduce the default delta read-chain cost while
  preserving the write-path safety already landed. The first narrow candidate
  is a delta epoch read-amplification budget or merge trigger; larger shard
  layout changes should wait for a separate phase.
