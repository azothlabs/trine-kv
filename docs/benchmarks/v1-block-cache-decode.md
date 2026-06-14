# V1 Block Cache And Decode Path

This note records Phase 180. The phase measured hot block-cache reads and
cache-disabled block decode reads, then kept a small block-cache maintenance
optimization.

## What Changed

The benchmark harness now reports:

- `block cache random block read`: a small-block persistent table workload that
  warms cached data blocks, then performs random hot reads.
- `block cache warm hit diagnostic`: cache-hit, cache-miss, data-block read,
  metadata-probe, and storage-read counters for repeated hot reads of one key.
- `block cache random hit diagnostic`: the same counters for random hot reads
  across many warmed blocks.
- `block decode forced diagnostic`: the same counters with block cache disabled.
- Codec decode-only rows for `none` and `fast-lz4-block`, separate from the
  existing encode-plus-decode codec rows.

The block cache now stores shard entries in a `HashMap` because eviction order
is tracked separately, and the cache recency queue skips work when the hit key
is already the newest entry.

## Evidence

Both benchmark runs used:

```text
TRINE_BENCH_RUNS=5 cargo bench --bench v1_bench
```

Before the cache change, with the new diagnostics present:

| Row | Median elapsed us |
| --- | ---: |
| block cache random block read | 1532 |
| block cache warm read | 1361 |
| block cache random hit diagnostic wall micros | 1681 |
| block cache warm hit diagnostic wall micros | 1421 |
| native runtime block decode read | 7511 |
| inline runtime block decode read | 7983 |

After the cache change:

| Row | Median elapsed us |
| --- | ---: |
| block cache random block read | 1488 |
| block cache warm read | 1285 |
| block cache random hit diagnostic wall micros | 1657 |
| block cache warm hit diagnostic wall micros | 1316 |
| native runtime block decode read | 6555 |
| inline runtime block decode read | 6843 |

The warm and random cache-hit diagnostics both reported 2048 cache hits, zero
cache misses, and zero storage read-owned requests before and after the cache
change. The forced decode diagnostic reported zero cache hits, 2048 cache
misses, and 2049 storage read-owned requests, which confirms it still measures
cache-disabled table block reads.

## Interpretation

The retained block-cache change reduces hot cache maintenance cost without
changing cache keys, admission, priority eviction, table format, compression
format, or read correctness. The decode-only codec rows show that LZ4 block
decode still has a much higher allocation/decode cost than `none`, but changing
the table block ownership model or compression contract is a larger phase and
was not part of this change.
