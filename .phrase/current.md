# Current Phase

## Native content reclamation support for TrineDB Phase 19

**Status:** Complete — 2026-07-22.

### Implemented boundary

- Physical deletion is default-off; only
  `ContentReclamationMode::QualifiedNativeFilesystem` is enabled.
- Final staging requires a fresh post-grace logical authorization, the exact
  barrier/drain/quarantine/grace chain, trusted clock/restart evidence, a valid
  descriptor, and no newer token, lease, hold, or physical activity.
- Prepared durably stores the exact upload/chunk manifest, blocks revival, and
  resumes chunk-before-descriptor deletion after failure or reopen.
- Reclaimed is recorded only after native deletion and directory durability
  complete. A later identical-content upload may clear the tombstone only after
  new descriptor bytes exist.
- Grace recovery accepts a fresh proof over a continuously durable quarantine
  when the original short-lived proof is no longer usable. The original intent
  and quarantine are validated and preserved, not rewritten.

### Verification

- All-feature regression: 513 passed, two ignored, all integration targets and
  30 doctests passed.
- Strict Rustdoc, all-target/all-feature Clippy with wildcard imports denied,
  formatting, diff checks, and `wasm32-wasip1` compilation passed.
- Native partial deletion, descriptor fault, close/reopen resume, actual byte
  absence, and identical-content re-upload are green.
- Fresh-proof grace recovery retains the original quarantine identity and
  commit coordinate.
- Object storage performs zero deletes and returns `UnsupportedBackend`; WASI,
  browser, and memory remain disabled.

### Remaining boundary

No Trine KV task is active. WASI, browser, and provider-backed deletion require
new consumer evidence and independent capability protocols before enablement.
