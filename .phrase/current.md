# Current Phase

## Status

Object-store compute/storage split hardening completed for the current
correctness phase. Previous durability tiering is complete.

## Goal

Make object-store persistence a real shared storage backend for separated
compute and storage: confirmed durable writes must be recoverable after process
loss and writer takeover, while explicitly buffered writes remain opt-in.

## Backend Boundary Receipt

- Trine operation names: open object-store database, accept write commit, publish
  table flush, publish manifest, recover from manifest plus remote commit log,
  advance replay floor, fence stale writer, clean orphan objects.
- Owned interface: Trine's `ObjectClient`, storage substrate, manifest store,
  write pipeline, and recovery path. External object-storage crates are adapters
  behind that boundary.
- Chosen backend: S3-compatible object storage through the existing object-store
  client contract, with the in-memory object client as deterministic test
  backend.
- Known backend limits: no append primitive, higher latency than local files,
  conditional writes required for manifest/lease/head ownership, listing may be
  weaker than point reads on some providers, deletes are cleanup only.
- Leak-check scope: public API names, error variants, stats, and docs must speak
  in Trine database operations rather than naming a provider as the architecture.
- Verification gate: crash/reopen recovery for confirmed object-store writes,
  stale-writer fencing, manifest CAS conflict behavior, WAL replay floor
  advancement, orphan cleanup safety, `cargo fmt --check`, targeted tests, and
  rustdoc/doc tests if public API docs change.

## Completed Task Slice

- Define explicit object-store commit modes so the current buffered behavior
  cannot masquerade as confirmed remote durability.
- Add a remote commit log path for durable object-store writes.
- Replay that log on open before serving reads.
- Advance and clean the log after table flush publishes a manifest that covers
  the replay floor.
- Fence stale writers before they can acknowledge new writes.
- Keep remote WAL metadata bounded by segmenting commit records instead of
  growing the lease/head object per commit.
- Define read-only compute refresh semantics for observing newer shared state.
- Record provider assumptions that correctness depends on: conditional writes,
  point-read visibility, listing use, idempotent delete, and retry boundaries.

## Out Of Scope

- Provider-specific S3 tuning beyond the current adapter contract.
- Performance tuning beyond the first grouped/segmented WAL correctness model.

## Acceptance Gate

- A confirmed durable object-store write survives closing without flush and
  reopening from the shared backend. Met.
- A stale writer cannot acknowledge a durable write after another writer takes
  ownership. Met.
- Flush publication advances the remote replay floor and does not lose writes
  across reopen. Met.
- Existing native persistent and in-memory behavior stays compatible. Met.
- Remote WAL metadata does not grow one lease/head entry per durable commit.
  Met.
- Durable write confirmation can batch more than one commit into one remote WAL
  segment. Met.
- Read-only compute handles can refresh to a newer manifest/replay boundary
  without reopening. Met.
- Provider correctness assumptions are captured in code-facing docs and covered
  by deterministic object-client tests where possible. Met.

## Verification

- `cargo check -q`
- `cargo test -q object_store`
- `cargo test -q --lib`
- `cargo fmt --check`
- `git diff --check`
- `cargo clippy -q --lib`
- `cargo test -q --doc --all-features`
- `cargo rustdoc --all-features -- -D warnings`
- `cargo test -q --all-features`

## Next Recommendation

- Next phase should measure remote-provider workloads and tune batching/cost
  behavior if measurements show the correctness-first segment model is too
  expensive. No correctness blocker remains in the deterministic object-client
  gate.
