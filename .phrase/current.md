# Current Phase

## Status

Durability tiering done. Also this session: snapshot-version-pin Phase 1 (merged;
2-3 deferred), delete-gc 4b (source-level GC), layered-filter Phase 5 D1 (opt-in
curve), manifest clean break (v11, no back-compat).

## Goal

Stop paying macOS `F_FULLFSYNC` on every durable commit. Expose a strict vs
non-strict durability tier (default non-strict for throughput; strict opt-in for
power-loss durability), routed through a single fsync abstraction.

## Design Assessment

macOS std `sync_all`/`sync_data` always issue `F_FULLFSYNC` — the only call that
flushes the drive's volatile cache (plain `fsync` does not; Apple `fsync(2)`,
per https://bonsaidb.io/blog/acid-on-apple/). That was the ~500 ops/s floor. Make
the strength a tier and centralize the platform decision in one place rather than
editing every sync site (per the user's direction).

## Scope (done)

- `src/durability.rs`: single abstraction `sync_file_for_durability(&File, mode)`
  (+ `sync_fd_for_durability` for the macOS DispatchIO backend,
  `requires_file_sync`, `durability_is_strict`). macOS: strict → `F_FULLFSYNC`
  (fallback `fsync` on ENOTSUP), non-strict → `fsync`. Non-macOS → std sync
  (already durable; strict == non-strict).
- `DurabilityMode::SyncAllStrict` + `WriteOptions::sync_all_strict()`; docs on the
  enum spell out the power-loss caveat.
- All sync sites (storage.rs, io/platform_threadpool.rs, io/platform_backend.rs,
  apple_dispatch.rs) route through the abstraction. `libc` is a non-optional macOS
  dependency.
- Default persistent durability stays `SyncAll`, now non-strict (faster).

## Acceptance Gate

- Measured (macOS, 200 single-key sync writes): non-strict ~8,234 ops/s vs strict
  ~386 ops/s (~21x). Met.
- Strict write persists across reopen (`strict_durability_write_persists_and_reopens`);
  durability unit tests classify modes and sync every mode. Met.
- Full gate: fmt + clippy (default and `--features platform-io-native`) clean;
  `--lib` 373, `--all-features` 377 / 1 ignored, native `--lib` 374; benches ok.

## Next Recommendation

- Deferred/measure-gated as before: snapshot-version-pin Phases 2-3; layered-filter
  Phase 5 D1 default-flip + D2 (need s3 benchmark / static-headroom evidence). A
  future polish: surface a database-default `SyncAllStrict` option for users who
  want power-loss durability by default.
