# Performance Research Design

## Purpose

This note captures the next performance research direction for Trine KV after
the `0.1` release baseline. It maps concrete performance techniques and
LSM-tree research to Trine's measured benchmark rows, while keeping Trine's
own API, storage contract, recovery rules, and benchmark evidence as the
authority.

External systems and papers are references only. They must not become public
names, architectural boundaries, or implementation dependencies.

## Current Evidence

The `0.1` benchmark baseline shows that ordinary point reads are already in a
reasonable local range:

- `random get`: 2048 operations in 1376 us.
- `missing get`: 2048 operations in 909 us.
- `block cache warm read`: 2048 operations in 1299 us.

The larger remaining costs are concentrated in specific workload shapes:

- `cold table read`: 32 operations in 183876 us.
- `prefix scan`: 128 operations in 9571 us.
- `snapshot read under concurrent writes`: 2048 operations in 18631 us.
- `WAL replay`: 1024 rows in 33136 us.
- `flush throughput`: 1024 rows in 72563 us.
- `compaction throughput`: 1024 rows in 197844 us.
- `separated blob values`: 256 operations in 132150 us.
- `blob GC rewrite`: 128 operations in 180677 us.
- `blob level merge`: 128 operations in 191034 us.
- `long shared-prefix get`: 2048 operations in 2646 us.

Prior tuning already removed the largest blob point-read mistake: blob point
reads stopped decoding whole blob files and now seek to the indexed record. The
next work should avoid re-tuning that already-fixed path unless fresh evidence
shows a new source cost.

## Reference Ideas

### Batched Execution

Batched execution processes a fixed-size group of keys, records, or block
decisions in one operation to reduce per-item CPU overhead. Trine should apply
this as batched storage-engine work.

Candidate Trine uses:

- batch filter probes for multiple keys;
- batch table/index metadata decisions;
- batch cold block reads;
- internal scan buffers that advance several records per source step;
- optional public `multi_get` only after the internal batch path proves useful.

Best target rows:

- `cold table read`;
- `prefix scan`;
- `long shared-prefix get`;
- future application benchmarks that currently call point reads in a loop.

### Small Work Units

Small work units split long-running engine work into bounded jobs with explicit
accounting. Trine should apply this to maintenance work where a large rewrite
or merge can be broken into independently measurable chunks.

Candidate Trine uses:

- split blob GC rewrite into independent candidate/file work units;
- split compaction reads and output writing into bounded jobs;
- expose progress through existing maintenance stats before changing public
  controls;
- avoid blocking foreground writes longer than the existing publish and
  maintenance barriers allow.

Best target rows:

- `blob GC rewrite`;
- `blob level merge`;
- `compaction throughput`;
- `flush throughput` only if foreground delay is measured.

### Memory-Optimized Prefix Indexing

Adaptive prefix indexing can reduce comparisons and source scans for hot
in-memory structures and shared-prefix-heavy workloads. Trine should test this
as a local prefix/radix summary, not as a replacement for the LSM tree.

Candidate Trine uses:

- benchmark a compact radix summary for in-memory delta heads;
- benchmark prefix summaries for hot table metadata;
- keep sorted SSTable records unchanged unless a separate storage-format phase
  is approved;
- use the `long shared-prefix get` row and prefix-scan rows as gates.

Best target rows:

- `long shared-prefix get`;
- `prefix scan`;
- `active memtable range scan`;
- `merged delta range scan`.

### Filter Memory Allocation

Monkey shows that LSM lookup cost depends on the sum of false-positive rates
across levels and that equal filter budgets across levels are not optimal.
RocksDB's partitioned and prefix filter work points to a similar practical
direction: reduce unnecessary table and block work before reading data blocks.

Candidate Trine uses:

- compute per-level filter target rates from a fixed memory budget;
- bias more filter memory toward levels that create the most read cost;
- add benchmark rows that report table probes, filter checks, filter useful
  skips, data-block reads, and cache misses for point and prefix workloads;
- evaluate partitioned table filters only behind a storage-format gate.

Best target rows:

- `missing get`;
- `cold table read`;
- `prefix scan`;
- `prefix scan table partitions matching`;
- `prefix scan table partitions nonmatching`.

### Key-Value Separation Maintenance

WiscKey validates the large-value direction Trine already uses: keep large
values out of the main table path to reduce compaction write cost. The
remaining Trine question is not whether to separate values; it is how to keep
blob maintenance cheap and predictable.

Candidate Trine uses:

- retain indexed blob reads for point and lazy scan values;
- benchmark GC candidate batching by live bytes, stale bytes, and file count;
- avoid full blob-file validation in foreground read paths;
- keep crash/recovery validation strong when changing GC or merge behavior.

Best target rows:

- `separated blob values`;
- `blob GC rewrite`;
- `blob level merge`;
- `blob range scan`;
- `blob range lazy keys`.

### Compaction Tradeoffs

Dostoevsky and related LSM work show that merge policy changes can trade write
cost, read cost, and space use. This is a powerful but high-risk direction for
Trine because it touches compaction policy and possibly recovery assumptions.

Candidate Trine uses:

- treat compaction-policy work as a later phase after read-pruning benchmarks;
- require before/after rows for point read, range scan, flush, compaction, and
  blob maintenance;
- require space-use and stale-record evidence;
- update the v1 protocol before changing stable compaction behavior.

Best target rows:

- `compaction throughput`;
- `flush throughput`;
- `cold table read`;
- `blob level merge`.

## Phase Order

### Phase A: Read Pruning Measurement

Goal: determine whether filter allocation, prefix filtering, or table metadata
layout is the next best read-path target.

Scope:

- add or reuse benchmark counters for table probes, filter checks, useful
  skips, data-block reads, and cache misses;
- run point, missing, prefix, and cold table read rows;
- do not change table format or public API.

Acceptance gate:

- benchmark evidence identifies one read-pruning source cost;
- the selected next phase names exactly one implementation target.

### Phase B: Batched Point Read Prototype

Goal: reduce repeated point-read overhead for callers that already have many
keys to fetch.

Scope:

- internal batch lookup path first;
- public `multi_get` only if the internal path produces a measurable win and
  the API shape is clear;
- no table format change.

Acceptance gate:

- benchmark rows compare looped point reads against batched reads;
- stats prove the benchmark uses real table/filter/block work.

### Phase C: Blob Maintenance Work Units

Goal: reduce blob GC and level-merge cost or foreground disruption without
weakening recovery.

Scope:

- GC candidate batching and bounded rewrite jobs;
- maintenance progress stats;
- crash/reopen tests around interrupted maintenance.

Acceptance gate:

- `blob GC rewrite` or `blob level merge` improves or has lower foreground
  disruption with no recovery regression.

### Phase D: Prefix/Shared-Key In-Memory Index Experiment

Goal: decide whether an adaptive prefix structure is worth adding for delta
heads or hot metadata.

Scope:

- benchmark-only prototype or internal experiment first;
- no SSTable record-format change;
- no public API change.

Acceptance gate:

- `long shared-prefix get` or prefix-scan rows show a clear improvement that is
  large enough to justify memory and implementation complexity.

### Phase E: Compaction Policy Research

Goal: evaluate whether compaction policy can reduce write and maintenance cost
without unacceptable read or space cost.

Scope:

- separate phase only;
- protocol update required before changing stable behavior;
- broad benchmark gate required.

Acceptance gate:

- before/after evidence covers read, write, flush, compaction, blob
  maintenance, and space-use rows.

## Rejected For Now

- Replacing sorted SSTable records with an in-memory index structure.
- Changing the table or blob format before benchmark evidence proves a specific
  source cost.
- Importing an external storage engine or making a paper/system name part of
  Trine's API.
- Optimizing the already-fixed whole-blob point-read path without new evidence.

## Source Notes

- Fixed-size batch execution and bounded work-unit scheduling references.
- Adaptive radix and prefix-indexing references.
- Monkey: level-aware Bloom filter allocation for LSM lookup cost.
- RocksDB MultiGet and Bloom filter notes: batch point reads, partitioned
  filters, prefix filters.
- WiscKey: key-value separation for SSD-conscious LSM storage.
- Dostoevsky: LSM merge policy tradeoffs.
