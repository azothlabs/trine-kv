# Current Phase

## Status

Object-store group commit scheduling and the explicit split WAL tier API are
completed for the current phase. Trine can now use one object client for bulk
storage/manifest and a separate object client for writer lease plus remote WAL.
Billing-aware R2 measurement now has per-scenario request-class output and
regression budgets for the expensive WAL publish path.

Post-phase native async close/lease correctness audit found and fixed a race:
close now rejects new publish activity, waits for already admitted commit,
flush, or compaction publish activity to finish, and only then releases the
writer lease.

Post-phase native writer lease availability audit found and fixed the stale
`LOCK` marker trap: native-file writer leases are now OS file locks held on the
open `LOCK` handle, while the file contents remain owner diagnostics and
release-time cleanup guards.

Post-phase table block decode hardening found and fixed a memory DoS risk:
decoded block lengths from SSTable block headers are now bounded before LZ4
decode can allocate output bytes. Table writes also cap the effective inline
value threshold so oversized values are sent to blob storage instead of creating
blocks the decoder would later reject. The same resource-bound pass now covers
whole-object reads, manifest/WAL payload lengths, blob file/property/record
lengths, direct blob references, cursor byte-field overflow checks, and
object-store `head` preflight before whole-object `get`.

Post-phase browser WASM hardening found and fixed four support gaps:
`platform-io` and `platform-io-native` now compile on
`wasm32-unknown-unknown` without routing to native-thread writer-lease paths or
advertising platform async I/O; browser persistence has explicit namespace
options; browser WAL/OPFS/Web Locks behavior has a dedicated browser integration
test; and CI/publish workflows now install Chrome, ChromeDriver, and
`wasm-bindgen-test-runner` before running that browser test.

Follow-up browser WASM review found and fixed three gaps: browser namespace
paths are now normalized before OPFS access and Web Locks naming, so path aliases
share one writer lease; browser whole-object reads check object-kind byte limits
before reading bytes from OPFS; and read-only browser open now fails with
not-found when the namespace lacks a Trine manifest. The browser integration
test now covers unflushed WAL reopen, flush reopen, manual compaction reopen,
blob-backed reads, bucket create/drop, namespace isolation, writer-lease
rejection, path-alias writer locking, missing read-only namespace failure, and
oversized manifest rejection.

Second follow-up browser WASM production hardening fixed the remaining browser
KV gaps: WAL append now writes only new bytes through OPFS instead of rereading
and rewriting the whole WAL; browser callers have public storage-manager helpers
for quota estimates, persisted status, and persistent-storage requests; browser
async write/maintenance lifecycle is documented as non-cancelable after first
poll; safe-temp recovery writes browser repair reports with `Flush`; and browser
bucket drop no longer self-deadlocks or leaves a normally dropped bucket's table
file behind. The browser integration test now also covers many unflushed WAL
appends, browser storage-manager status, safe WAL temp fail-closed behavior, and
explicit safe-temp repair.

SharedWorker review found a browser host-boundary leak: Trine's OPFS backend
used the `opfs` crate's `app_specific_dir()` helper, which calls
`web_sys::window()` and therefore cannot open OPFS from worker globals. Trine now
opens the OPFS root through `globalThis.navigator.storage.getDirectory()` and
keeps both the existing `opfs` wrapper root and the native `web_sys` root.
Worker-context file bytes now use OPFS synchronous access handles for
whole-object reads, random reads, object writes, WAL append, manifest publish,
and WAL rewrite; each access handle is scoped to one storage operation and
closed before returning. Window-context browser storage keeps the async OPFS
path. Worker globals that expose OPFS without `FileSystemSyncAccessHandle` now
fail with `UnsupportedBackend` instead of silently using the Window path. The
browser wasm test now includes real DedicatedWorker and SharedWorker Trine DB
round trips, a SharedWorker sync-access capability probe, a sync-handle
exclusivity probe, and a lightweight sync-handle timing probe; CI and publish
workflows already run that test under Chrome/ChromeDriver.

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
- Native async close waits for admitted publish activity before releasing the
  writer lease. Met.
- Native writable open recovers from a crash-left `LOCK` marker when no process
  still holds the OS file lock, while live second writers still fail. Met.
- Malicious or corrupt SSTable block headers cannot force oversized LZ4 output
  allocation before decode validation. Met.
- Malicious or corrupt manifest, WAL, table, or blob length fields cannot force
  unbounded buffer allocation before validation. Met.
- Browser WASM with `platform-io` and `platform-io-native` compiles without
  native-thread capability leakage. Met.
- Browser persistent callers can select explicit origin-private namespaces.
  Met.
- Browser OPFS/Web Locks behavior is covered by a real browser test gate for
  WAL reopen, namespace isolation, and second-writer rejection. Met in local
  Chromium/Playwright execution and CI workflow configuration.
- Browser OPFS/Web Locks behavior is covered beyond smoke paths: flush,
  compaction, blob reads, bucket drop, namespace aliases, missing read-only
  manifest, oversized manifest preflight, many WAL appends, storage-manager
  status, safe-temp fail-closed behavior, and safe-temp repair all have
  browser-target integration coverage. Met in local Chromium/Playwright
  execution; Safari WebDriver remains host-killed, so CI should continue using
  ChromeDriver.
- Browser OPFS root open is no longer tied to `window`; it uses the current
  browser global's `navigator.storage.getDirectory()`. Met by wasm target build
  and browser test no-run.
- Worker-context browser storage uses OPFS synchronous access handles for file
  byte operations. Met by wasm target build, wasm clippy, and browser-target
  integration tests that run Trine DB round trips from DedicatedWorker and
  SharedWorker, plus SharedWorker capability, exclusivity, and timing probes.
  Local runtime execution still needs ChromeDriver because Safari WebDriver is
  host-killed; CI provisions ChromeDriver.

## Verification

- `cargo test -q oversized_uncompressed_len`
- `cargo test -q lz4_decode_rejects_oversized_output_before_allocation`
- `cargo test -q blob_threshold_is_capped_to_keep_inline_values_decodable`
- `cargo test -q large_allocation`
- `cargo test -q oversized`
- `cargo test -q native_async_close_waits_for_active_publish_before_releasing_lease`
- `cargo test -q writer_lease`
- `cargo test -q writer_lease --features platform-io`
- `cargo fmt --check`
- `git diff --check`
- `cargo check -q`
- `cargo check -q --features platform-io`
- `cargo check -q --all-features`
- `cargo check -q --features s3`
- `cargo test -q object_wal_lane_group_commits_queued_accepts`
- `cargo test -q object_store`
- `cargo test -q --lib`
- `cargo clippy -q --lib`
- `cargo clippy -q --all-features --lib`
- `cargo clippy -q --features s3 --lib`
- `cargo test -q object_store_split_wal_tier`
- `cargo test -q --features s3 s3_live_measurement_and_fault_suite`
- `cargo test -q --doc --all-features`
- `cargo rustdoc --all-features -- -D warnings`
- `cargo test -q --all-features`
- `infisical run --silent --env=dev --path=/ --recursive -- cargo test -q --features s3 s3_live_measurement_and_fault_suite -- --ignored --nocapture`
- `cargo check -q --target wasm32-unknown-unknown --no-default-features --features platform-io --lib`
- `cargo check -q --target wasm32-unknown-unknown --no-default-features --features platform-io-native --lib`
- `cargo clippy -q --target wasm32-unknown-unknown --no-default-features --features platform-io --lib -- -D warnings`
- `cargo clippy -q --target wasm32-unknown-unknown --no-default-features --features platform-io-native --lib -- -D warnings`
- `cargo clippy -q --target wasm32-unknown-unknown --test browser_persistent_wasm -- -D warnings`
- `cargo test -q --target wasm32-unknown-unknown --test browser_persistent_wasm --no-run`
- `cargo test -q`
- `cargo clippy -q --all-features --all-targets -- -D warnings`
- `cargo rustdoc --all-features -- -D warnings`
- `cargo test --doc --all-features -q`
- `cargo test --target wasm32-unknown-unknown --test browser_persistent_wasm --no-run`
- `cargo check -q`
- `cargo check -q --all-features`
- `cargo check -q --target wasm32-unknown-unknown --no-default-features --lib`
- `cargo check -q --target wasm32-unknown-unknown --no-default-features --features platform-io --lib`
- `cargo check -q --target wasm32-unknown-unknown --no-default-features --features platform-io-native --lib`
- `cargo clippy -q --target wasm32-unknown-unknown --test browser_persistent_wasm -- -D warnings`
- `cargo test -q --target wasm32-unknown-unknown --test browser_persistent_wasm --no-run`
- `cargo test -q`
- `cargo clippy -q --all-features --all-targets -- -D warnings`
- Attempted real browser run with
  `CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner
  cargo test --target wasm32-unknown-unknown --test browser_persistent_wasm`;
  sandboxed execution cannot spawn the local test server; escalated execution
  starts the runner but Safari WebDriver is host-killed before tests enter the
  page. CI provisions ChromeDriver for this gate.
- `git diff --check`
- real Chromium OPFS browser integration run via temporary wasm-bindgen +
  Playwright harness: 14 browser tests passed

## Next Recommendation

- Stop this phase here. Trine now has stable WAL objects, measured group commit
  scheduling, an explicit split WAL tier API, and billing-aware live guards.
- Browser persistent KV is now production-grade for the documented browser
  boundary: callers can check quota/persistence, WAL append is efficient, safe
  temp repair and bucket drop are covered, and the real Chromium OPFS gate
  passed locally in the prior harness. The OPFS entry no longer depends on
  `window`, and Worker contexts now use synchronous access handles for file byte
  operations. DedicatedWorker and SharedWorker now both run a real Trine DB
  open/write/delete/flush/compact/reopen cycle in the browser test harness; the
  remaining runtime gap is only this host's local WebDriver setup.
- Only start another phase if we need a concrete external WAL service/provider
  adapter. That phase should implement the adapter behind `ObjectClient`, then
  measure single-commit latency against R2 storage plus that WAL tier.
