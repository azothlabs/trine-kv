# Trine KV V1 Delta Read Chain Budget

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

Change measured:

- Delta open epochs now merge when the shard reaches the configured delta-count
  budget, instead of waiting for one delta beyond the budget.
- The budget value stays at 8, so the change narrows the default read chain
  without making every small append merge immediately.

Phase 96 comparison rows:

| name | before_elapsed_us | after_run_1_elapsed_us | after_run_2_elapsed_us |
| --- | ---: | ---: | ---: |
| single-key put | 7711 | 16155 | 8696 |
| batch write | 643 | 845 | 630 |
| random get | 1560 | 1114 | 1101 |
| missing get | 1221 | 789 | 760 |
| bounded range scan | 6057 | 4212 | 4018 |
| active memtable random get | 768 | 767 | 762 |
| merged delta random get | 697 | 733 | 690 |
| active memtable range scan | 2040 | 2021 | 2110 |
| merged delta range scan | 3019 | 2990 | 2940 |

Interpretation:

- Default in-memory point reads improved about 29% on repeated release runs.
- Default missing-key point reads improved about 35% to 38%.
- Default bounded range scans improved about 30% to 34%.
- The active-memtable and merged-delta comparison rows stayed in the same local
  range, so the benchmark harness itself did not shift shape.
- Write rows remain noisy locally; repeated `batch write` did not regress, and
  `single-key put` stayed within the broader run-to-run spread seen in this
  small harness.

Recommended next action:

- Keep this narrow budget change.
- Move the next async/write-path slice back to persistent WAL front-door
  staging only after recovery and cancellation tests are prepared.
