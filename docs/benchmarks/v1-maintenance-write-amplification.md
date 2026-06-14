# V1 Maintenance Write-Amplification

Date: 2026-06-14

## Scope

This note records Phase 179. The phase measured flush, compaction, blob-GC
rewrite, and blob-level merge write amplification.

The retained change does not alter storage formats, manifest cutover semantics,
blob reachability, durability, or snapshot-delayed cleanup. It only moves one
cleanup-only manifest publish out of foreground blob maintenance.

## Measurement

The added diagnostics report storage writes, manifest publishes, directory
syncs, object deletes, compaction bytes, blob-GC bytes, and wall time.

Before the change:

- Flush diagnostic: one object write, one manifest publish, one directory sync.
- Compaction diagnostic: one object write, one manifest publish, one directory
  sync, four object deletes.
- Blob-GC diagnostic: three object writes, three manifest publishes, two
  directory syncs, four object deletes.
- Blob-level merge diagnostic: two object writes, two manifest publishes, one
  directory sync, four object deletes.

After the change:

- Blob-GC diagnostic: three object writes, two manifest publishes, two
  directory syncs, four object deletes.
- Blob-level merge diagnostic: two object writes, one manifest publish, one
  directory sync, four object deletes.
- Flush and ordinary compaction request counts are unchanged.

The filtered 3-run after-change medians were:

- `write amp blob GC diagnostic wall micros`: 40602.
- `write amp blob level merge diagnostic wall micros`: 35974.
- `blob GC rewrite`: 176798 us.
- `blob level merge`: 169080 us.
- `compaction throughput`: 195383 us.
- `flush throughput`: 69857 us.

## Change

Foreground compaction and blob maintenance still delete obsolete blob files
before returning when no read pin can reach them. They no longer publish a
second manifest only to clear the pending-deletion metadata for files that have
already been deleted.

The existing cleanup boundaries still clear that metadata:

- writable open cleanup,
- later flush cleanup,
- close/drop cleanup.

Native file deletion is idempotent for missing files, so a later cleanup can
retry the pending deletion safely and then clear the manifest metadata.

## Interpretation

The measured table/blob output counts are real work for the selected policies.
The safe reduction was the cleanup-only manifest publish. This lowers
foreground blob maintenance write amplification without weakening recovery:
pending blob ids remain recorded until a cleanup boundary clears them.

## Verification

- `cargo fmt --check`
- `cargo test -q persistent_blob_level_merge_defers_pending_blob_clear_publish --lib`
- `cargo test -q blob_gc --lib`
- `cargo test -q blob_level_merge --lib`
- `cargo test -q compaction --lib`
- `cargo clippy --bench v1_bench -- -D warnings`
- `cargo test -q --lib`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `TRINE_BENCH_RUNS=1 cargo bench --bench v1_bench`
- `TRINE_BENCH_RUNS=3 cargo bench --bench v1_bench`
- `git diff --check`
