# Lock-Free Foreground Write Path Protocol

Date: 2026-05-27

## 1. Purpose

This protocol defines the next production-grade write-path target for Trine KV:

```text
foreground point reads and blind writes do not take a global database write lock
```

The goal is not a lock-free database slogan. The goal is a concrete engine
shape:

- foreground writers build immutable commit data in private memory;
- commit sequence assignment is atomic;
- WAL front doors are sharded;
- user-visible data is published with atomic pointer swaps;
- snapshot readers read immutable structures without waiting for writers;
- background and I/O state is owned by explicit workers;
- real transaction conflicts remain visible and testable.

This protocol extends the v1 LSM MVCC design. It does not replace the SSTable,
manifest, blob, compaction, recovery, or public API contracts unless a later ADR
or protocol explicitly says so.

## 2. Non-Goals

This protocol does not promise:

- a fully lock-free database;
- wait-free operations;
- transaction commits that never fail;
- elimination of semantic ordering for writes to the same key;
- no waiting for WAL durability, fsync, memory budget, or background pressure;
- a single actor that owns all database reads and writes;
- changing the current public API solely for concurrency internals.

WAL file writes, manifest publish, segment rollover, compaction scheduling, and
file cleanup are allowed to be single-owner worker responsibilities. The
foreground path must not send every point read or blind write through one global
database actor.

## 3. Vocabulary

- **Blind write**: a write batch that does not depend on a transaction read set.
- **Semantic conflict**: a conflict required by the chosen isolation level, such
  as a transaction read being invalidated by a later write.
- **Implementation conflict**: avoidable contention caused by one global writer
  lock, one WAL cursor, or one shared memtable insertion point.
- **Commit sequence**: globally unique sequence assigned to a commit attempt.
- **Visible sequence**: highest sequence boundary that snapshot creation may use
  after all earlier sequence slots are terminal.
- **Sequence slot**: state for one assigned commit sequence.
- **PreparedCommit**: immutable commit record built by a foreground writer
  before publication.
- **PreparedShardDelta**: immutable subset of a commit for one bucket/key shard.
- **DeltaShard**: key-sharded in-memory head that stores recently published
  immutable deltas.
- **Delta epoch**: bounded group of deltas that can be sealed and handed to
  background merge.
- **WAL shard**: independent WAL append lane with its own worker, segment state,
  and backlog.
- **Terminal slot**: sequence slot that is visible or skipped. Skipped slots are
  aborted or lost and never publish records.

## 4. Design Principles

### 4.1 No Global Foreground Writer Lock

The foreground write path must not require a mutex that serializes all writers.
Allowed shared steps are:

- atomic sequence allocation;
- bounded queue enqueue or atomic slot reservation for one WAL shard;
- compare-and-swap publication to affected delta shards;
- atomic completion-slot update;
- optional visible-sequence advancement.

### 4.2 WAL Is The Recovery Truth

Published in-memory deltas are not recovery truth. Recovery rebuilds memory
state from manifest-published tables plus WAL records that survive validation.

### 4.3 Commit Sequence Is Not Visibility

Assigning a commit sequence does not make data visible. A commit becomes visible
only after:

1. the WAL accept rule for its write options has passed;
2. every affected shard delta has been published;
3. its sequence slot is marked visible;
4. the visible sequence has advanced across that terminal slot.

### 4.4 Gaps Must Be Explicit

The visible sequence can advance across a gap only when the missing slot is
terminal. A slow writer keeps its slot open. A failed writer marks the slot
skipped. A skipped slot never publishes user records.

### 4.5 Reads Must Be Bounded

Writer-local data must not stay permanently writer-local on the read path.
Writers may construct commits locally, but publication routes records into
bucket/key shards. Point reads must inspect only the target shard plus stable
tables, not every writer.

### 4.6 Immutable Publish Outputs

Foreground writers publish immutable deltas. Readers never observe in-place
mutation of a published delta.

### 4.7 Actors Are Ownership Boundaries

Actors or worker loops are appropriate for:

- WAL shard file ownership;
- manifest publish ownership;
- delta merge;
- compaction scheduling;
- retired file cleanup.

They are not the foreground read/write abstraction.

## 5. Runtime Components

### 5.1 Commit Sequencer

The commit sequencer owns an atomic `next_sequence`.

Rules:

- every commit attempt receives one sequence;
- duplicate keys inside the batch are ordered by batch index;
- skipped sequence numbers are allowed only through terminal skipped slots;
- sequence allocation must not wait for WAL or delta publication.

### 5.2 Commit Tracker

The commit tracker stores sequence slot states:

```text
Open -> Visible
Open -> Skipped
```

State meaning:

- `Open`: sequence was assigned; commit may still publish or fail.
- `Visible`: WAL accept rule passed and all shard deltas were published.
- `Skipped`: commit failed before visibility, was aborted, or was lost during
  recovery.

The tracker advances `visible_sequence` only over terminal slots.

### 5.3 PreparedCommit

`PreparedCommit` is built in writer-private memory.

It contains:

- commit sequence;
- write options and durability mode;
- ordered operations grouped by bucket;
- per-operation batch index;
- key bounds;
- affected bucket/key shards;
- checksum-covered WAL payload bytes or encodable equivalent;
- transaction validation metadata when the batch comes from a transaction.

`PreparedCommit` is immutable after sequence assignment.

### 5.4 PreparedShardDelta

Each prepared commit is split into one or more `PreparedShardDelta` values.

Rules:

- each delta belongs to one bucket and one key shard;
- point records inside a delta are sorted by internal key;
- range tombstones inside a delta carry explicit bounds;
- delta key bounds and approximate byte size are recorded;
- deltas are immutable and reference-counted after publication.

### 5.5 DeltaShard

Each bucket owns a fixed or configurable number of key shards. A shard owns an
atomic head pointer to the current delta epoch.

Rules:

- publishing uses compare-and-swap on the target shard head;
- CAS retry rebuilds only the small head link, not the whole commit payload;
- point reads load the target shard head with acquire ordering;
- range/prefix scans enumerate only shards whose key span can overlap the
  selector;
- shard chain length and bytes are budgeted.

### 5.6 Delta Epoch

An epoch groups deltas so background merge can bound read amplification.

Epoch states:

```text
Open -> Sealed -> Merging -> Merged -> Retired
```

Rules:

- foreground writers publish only into open epochs;
- sealing installs a new open epoch before background merge begins;
- readers holding snapshots may keep sealed or merged epochs alive;
- retired epochs are reclaimed only after no snapshot can read them.

### 5.7 WAL Shard

A WAL shard is an append lane with one owner for file cursor, segment rollover,
and group sync.

Rules:

- a single commit record is assigned to one WAL shard;
- one commit record must not be split across multiple WAL shards in this
  protocol version;
- the shard accepts commit records through a bounded queue or atomic reservation
  ring;
- the WAL shard worker writes records in its lane order;
- recovery merges records from all WAL shards by commit sequence.

The shard choice may use sequence, thread-local writer id, or bucket id. The
choice must not break cross-bucket atomicity because the entire commit record is
kept together.

### 5.8 Background Workers

Background workers may use normal locks internally if they do not put a global
lock on the foreground hot path.

Workers:

- WAL shard worker;
- delta merge worker;
- compaction scheduler;
- manifest publisher;
- GC worker.

## 6. Foreground Write Protocol

### Step 1: Validate And Prepare

The writer validates public API inputs and builds a `PreparedCommit` in private
memory.

Rules:

- no shared memtable mutation occurs in this step;
- bucket handles and options are validated before sequence assignment when
  possible;
- transaction validation may read current conflict metadata but does not publish
  user records.

### Step 2: Assign Commit Sequence

The writer reserves a sequence slot:

```text
sequence = next_sequence.fetch_add(1)
slot[sequence] = Open
```

The sequence is used for every record in the commit.

### Step 3: Route To WAL Shard

The writer sends or reserves the whole commit record in one WAL shard.

The WAL accept rule depends on write options:

- relaxed mode: record copied into the WAL shard's accepted memory boundary;
- write-through mode: record written to the operating-system file boundary;
- sync mode: record and required directory or segment metadata synced according
  to the durability contract.

No delta may become visible before the WAL accept rule for the commit succeeds.

### Step 4: Publish Shard Deltas

After WAL accept, the writer publishes every `PreparedShardDelta`:

```text
loop:
  old_head = shard.head.load(Acquire)
  new_head = link(delta, old_head)
  if shard.head.compare_exchange(old_head, new_head, Release, Acquire) succeeds:
    break
```

Rules:

- a commit that affects multiple shards publishes all of them before marking its
  sequence slot visible;
- partially published deltas are harmless before visibility because readers
  ignore records with sequence above their snapshot sequence;
- after any shard delta has been published, the commit must finish publishing
  all remaining shard deltas and then mark the slot visible. It must not mark
  the slot skipped, because skipped slots can be crossed by future visible
  snapshots;
- a slot may be marked skipped only before any user delta for that sequence is
  published.

### Step 5: Mark Slot Visible

When all affected deltas are published:

```text
slot[sequence].store(Visible, Release)
```

If the commit fails before WAL accept or before any user delta is published:

```text
slot[sequence].store(Skipped, Release)
```

Skipped commits must not leave published user records. Once publication starts,
the runtime must either complete publication or let crash recovery rebuild the
commit from WAL.

### Step 6: Advance Visible Sequence

Any writer or maintenance worker may try to advance `visible_sequence`:

```text
current = visible_sequence.load(Acquire)
while slot[current + 1] is terminal:
  current += 1
visible_sequence.compare_exchange(old, current, Release, Acquire)
```

Readers use `visible_sequence` as the default snapshot boundary.

### Step 7: Return To Caller

The commit returns after its own visibility and requested durability rule have
been satisfied. If backpressure or durability fails, the error must be explicit.

## 7. Read Protocol

### 7.1 Snapshot Capture

A read snapshot captures:

- `visible_sequence`;
- current LSM table version handles;
- delta shard epoch heads needed by the read;
- active snapshot pin for retention.

The snapshot boundary is not `next_sequence`. It is `visible_sequence`.

### 7.2 Point Read

Point read order:

1. load the bucket/key shard head;
2. search visible deltas newest to oldest within the shard;
3. search immutable memtables or merged delta outputs;
4. search L0 overlaps and at most one table per non-overlapping level;
5. apply range tombstone coverage;
6. return the newest visible record.

Point reads must not:

- scan all writer-local structures;
- send a request to a database actor;
- wait for a foreground writer that has not yet become visible.

### 7.3 Range And Prefix Scan

Range and prefix scans may touch multiple shards. They must:

- choose candidate shards by key span or prefix extractor;
- lazily merge shard deltas, memtables, and SSTables;
- apply MVCC and range tombstone rules per returned key;
- avoid eager construction of the full result set.

## 8. Transaction Conflict Rules

This protocol removes avoidable implementation conflicts. It does not remove
semantic conflicts.

Rules:

- blind writes never abort because another blind writer wrote concurrently;
- same-key blind writes are ordered by commit sequence;
- read-only snapshots never abort;
- optimistic transactions validate their read keys and ranges at commit;
- a transaction fails only when accepting it would violate its isolation rules;
- transaction validation must run before slot visibility.

Recommended target statement:

```text
Blind writes never conflict. Reads never block writes. Writes never block
snapshot reads. Transactions only fail when accepting them would violate the
chosen isolation level.
```

## 9. WAL Recovery Protocol

Recovery input:

- manifest-published tables and blob metadata;
- all valid WAL shard records after the manifest replay floor;
- repair policy for temporary or malformed files.

Recovery steps:

1. read every WAL shard independently;
2. validate record header, length, checksum, and format version;
3. discard torn final records according to existing WAL rules;
4. collect valid commit records above the manifest replay floor;
5. sort by commit sequence;
6. replay records into key-sharded deltas or memtables;
7. treat missing sequence numbers as skipped slots;
8. rebuild `visible_sequence` from replayed and skipped terminal slots;
9. fail closed if a sync-required record is known to be corrupt rather than
   merely absent due to relaxed durability.

Recovery must not depend on in-memory publish state that existed before the
crash.

## 10. Backpressure

Foreground writes are allowed to wait for resource budgets. That is not a
global write lock.

Required budgets:

- WAL shard queue bytes and record count;
- open sequence slot window;
- per-delta-shard chain length;
- open and sealed delta bytes;
- immutable memtable bytes;
- L0 table count or bytes;
- pending manifest publish count;
- pending GC bytes.

Backpressure rules:

- if WAL backlog exceeds budget, writers wait or return an explicit pressure
  error according to options;
- if delta chain length exceeds budget, seal an epoch and request merge;
- if sealed epochs exceed budget, writes wait for merge progress;
- if open sequence slots exceed budget, writes wait for terminal slot progress;
- background errors surface through later writes and maintenance calls.

## 11. Memory Ordering Requirements

The implementation may choose exact Rust atomic orderings, but it must preserve
these happens-before relationships:

- all delta contents are initialized before publishing the shard head;
- publishing shard heads happens before marking the slot visible;
- marking the slot visible happens before advancing `visible_sequence`;
- snapshot creation that observes a visible sequence must be able to observe the
  deltas that made those sequences visible;
- epoch retirement happens only after no reader can hold the epoch.

The default implementation should prefer the weakest ordering that preserves
these rules, but correctness comes before micro-optimization.

## 12. Observability

The engine must expose enough stats to diagnose the new path:

- commit sequences allocated;
- visible sequence;
- open sequence slots;
- skipped slots;
- WAL shard queue depth and bytes;
- WAL shard write/sync latency;
- CAS publish retries per delta shard;
- delta chain length and bytes per bucket;
- sealed delta epochs;
- merge requests and merge latency;
- writer backpressure counts;
- transaction validation failures by reason.

## 13. Required Tests

Correctness tests:

- concurrent blind writes to disjoint keys all become visible;
- concurrent blind writes to the same key are ordered by sequence;
- reads never observe a sequence above captured `visible_sequence`;
- sequence gap stays invisible while a lower slot is open;
- skipped lower slot with no published user delta lets later visible records
  become readable;
- partially published multi-shard commit cannot be marked skipped;
- multi-shard commit is not visible until all affected shard deltas publish;
- transaction read conflict fails when required by isolation;
- read-only snapshot keeps old values after concurrent writes;
- range and prefix scans merge shard deltas with SSTables correctly;
- delta epoch retirement waits for snapshot release.

Recovery tests:

- replay multiple WAL shards in sequence order;
- recover when one relaxed-durability sequence is absent and later records
  exist;
- fail closed on corrupt non-torn WAL records;
- preserve cross-bucket atomic batch replay;
- recover after crash between WAL accept and delta publish;
- recover after crash during manifest publish with WAL shards present.

Shape tests or benchmark notes:

- blind write hot path does not take a global database writer mutex;
- point read hot path does not send to an actor;
- point read only inspects the relevant key shard delta heads;
- WAL shard workers own file cursors;
- delta chain budgets trigger seal and merge;
- background merge reduces read amplification.

## 14. Relationship To Async Storage

This protocol and the async-first portable storage protocol describe one final
engine shape, but their implementation slices must stay separable.

Rules:

- do not combine the async public API migration, backend contract migration,
  WAL sharding, and delta publication in one code change;
- async storage owns API waiting points, backend capabilities, durability
  mapping, runtime boundaries, and cancellation rules;
- this protocol owns commit sequence assignment, slot terminal states,
  visible-sequence advancement, WAL shard ordering, shard-delta publication,
  and recovery merge order;
- the first write-path implementation slice should place commit tracker and
  visible sequence behind the current writer coordinator;
- WAL shard front doors should wait until the async storage boundary has typed
  durability and cancellation tests;
- key-sharded delta publication should wait until visible-sequence behavior is
  tested independently.

If these ownership boundaries conflict during implementation, update both
protocols before changing Rust code.

## 15. Implementation Staging

The implementation should land in protocol-preserving slices:

1. introduce commit tracker and visible-sequence slot states behind the current
   write coordinator;
2. introduce immutable prepared commits and shard delta types without changing
   public API;
3. publish deltas through key-sharded heads for in-memory mode;
4. route persistent writes through WAL shard front doors while keeping one
   commit record per batch;
5. move foreground visibility to slot completion plus visible sequence;
6. add delta epoch sealing and background merge;
7. update recovery to merge WAL shard records by sequence;
8. add full observability and benchmark gates.

Each slice must keep the existing MVCC, range-delete, transaction, recovery, and
compaction tests passing or update the protocol first when behavior changes.

## 16. Acceptance Gate

This protocol is accepted for implementation when:

- the roadmap and current phase identify it as the active source of truth;
- tests listed above are either implemented or staged with explicit blockers;
- benchmark harness can measure concurrent blind writes and mixed read/write;
- recovery rules for multi-WAL-shard replay are tested;
- no public API behavior changes silently;
- evidence records before/after write-path contention and read amplification.
