# Current Phase

## Status

Object-store group commit scheduling and the explicit split WAL tier API are
completed for the current phase. Trine can now use one object client for bulk
storage/manifest and a separate object client for writer lease plus remote WAL.
Billing-aware R2 measurement now has per-scenario request-class output and
regression budgets for the expensive WAL publish path.

## Goal

Reduce confirmed object-store write latency and allow deployments to place the
confirmed write log on a lower-latency durable tier, without weakening the
durability contract: an acknowledged durable object-store write must remain
recoverable after process loss and writer takeover.

## Backend Boundary Receipt

- Trine operation names: open object-store database, accept write commit,
  publish remote WAL head, recover from manifest plus remote commit log, advance
  replay floor, fence stale writer, clean orphan objects.
- Owned interface: Trine's `ObjectClient`, storage substrate, manifest store,
  write pipeline, recovery path, internal WAL lane scheduling, and explicit
  split-tier object-store open APIs.
- Chosen backend: S3-compatible object storage through the existing
  object-store client contract, with the in-memory object client as the
  deterministic backend.
- Low-latency boundary: the object-store substrate owns an independent WAL
  lane/worker, and `Db::open_object_store_with_wal(_at)` lets callers supply a
  separate WAL client for writer lease, remote WAL head, and WAL segment bytes.
  That client is the confirmed-write durability sink.
- Known backend limits: no append primitive, higher latency than local files,
  conditional writes required for lease/head ownership, listing may be weaker
  than point reads on some providers, deletes are cleanup only.

## Completed Task Slice

- Kept the stable per-writer-epoch remote WAL segment from the prior phase.
- Added an object WAL lane with bounded queue, worker-owned writer lease, and a
  short group-commit delay.
- Batched queued commit accepts into one segment overwrite and one remote WAL
  head publish.
- Preserved stale-writer `Fenced` errors for single-commit failures.
- Added a contiguous-sequence guard before publishing a grouped WAL head.
- Added a Tokio-capable future driver for the WAL worker under the `s3` feature,
  so real R2 object-store I/O runs with a reactor instead of panicking in the
  worker thread.
- Extended the R2 live suite with a concurrent group-commit measurement.
- Added explicit split WAL tier open APIs:
  `Db::open_object_store_with_wal` and `Db::open_object_store_with_wal_at`.
- Recovery and read-only refresh now read the lease/head and WAL segment from
  the WAL tier while manifest/tables remain on the storage tier.
- Added deterministic split-tier regressions for unflushed confirmed-write
  recovery and read-only refresh.
- Extended the R2 live suite with a split-tier reopen smoke. In that run both
  clients point at R2, so it proves API/recovery semantics, not an external
  low-latency service's latency.
- Added billing-aware R2 live output per scenario, including Class A/Class B/free
  request counts and Standard-storage request-cost estimates.
- Added live budget guards:
  - sequential durable writes must stay at or below one WAL PUT plus one WAL head
    CAS per write;
  - concurrent group commit writes must use exactly one WAL PUT and one WAL head
    CAS for the measured batch.

## Out Of Scope

- Weakening confirmed durability into buffered writes.
- Implementing a new external WAL service/provider adapter in this phase.
- Provider-specific lifecycle/billing automation.
- Changing read-only refresh cadence or flush policy beyond measuring that they
  still behave after the scheduling change.

## Acceptance Gate

- A confirmed durable object-store write survives closing without flush and
  reopening from the shared backend. Met.
- A stale writer cannot acknowledge a durable write after another writer takes
  ownership. Met.
- Grouped commits publish a contiguous remote WAL head only after their frames
  are present in the segment. Met.
- Queued object-store commit accepts can share one segment PUT and one head CAS.
  Met.
- Real R2 live run shows concurrent confirmed writes use fewer remote publishes
  than one publish per write. Met: 12 concurrent writes used 1 WAL PUT and 1
  head `put_if`.
- Split-tier open can recover an unflushed confirmed write when storage and WAL
  clients are different. Met.
- Read-only refresh can replay WAL from the WAL tier while reading manifest and
  tables from the storage tier. Met.
- Real R2 live run exercises the split-tier API path. Met.
- Real R2 live run reports request classes per scenario and enforces the group
  commit Class A budget. Met.
- Existing native persistent and in-memory behavior stays compatible. Met.

## Verification

- `cargo fmt --check`
- `git diff --check`
- `cargo check -q --features s3`
- `cargo test -q object_wal_lane_group_commits_queued_accepts`
- `cargo test -q object_store`
- `cargo test -q --lib`
- `cargo clippy -q --lib`
- `cargo clippy -q --features s3 --lib`
- `cargo test -q object_store_split_wal_tier`
- `cargo test -q --features s3 s3_live_measurement_and_fault_suite`
- `cargo test -q --doc --all-features`
- `cargo rustdoc --all-features -- -D warnings`
- `cargo test -q --all-features`
- `infisical run --silent --env=dev --path=/ --recursive -- cargo test -q --features s3 s3_live_measurement_and_fault_suite -- --ignored --nocapture`

## Next Recommendation

- Stop this phase here. Trine now has stable WAL objects, measured group commit
  scheduling, an explicit split WAL tier API, and billing-aware live guards.
- Only start another phase if we need a concrete external WAL service/provider
  adapter. That phase should implement the adapter behind `ObjectClient`, then
  measure single-commit latency against R2 storage plus that WAL tier.
