# Evidence

Record only evidence that can change planning or durable decisions.

## Template

### YYYY-MM-DD: <topic>

**Observation**:

- What was directly observed.

**Interpretation**:

- What the observation likely means.

**Verification**:

- Test, trace, benchmark, audit, manual check, or other proof.

**Remaining Blockers**:

- What still prevents completion.

**Recommended Next Action**:

- What the next phase or task should do.

## 2026-05-25: V1 Spec Baseline

### Observation

- Repository is a clean project skeleton with phrase workflow files and local
  Rust skills.
- User wants a new independent embedded KV, not a comparison project and not a
  previous-engine continuation.
- User requires LSM-tree based storage, MVCC, persistence, in-memory mode, and
  first-version completeness.

### Interpretation

- The first useful deliverable is a durable spec, not Rust code.
- Trine should be specified and implemented from its own docs and tests.

### Recommended Next Action

- Review `.phrase/protocol/trine-kv-v1-spec.md`.
- If accepted, start Phase 2 by scaffolding the Rust crate and module layout.

## 2026-05-25: Search Policy Added To Spec

### Observation

- Binary search can be a measurable CPU cost in immutable table indexes and
  block restart indexes.
- The useful alternatives are not universal replacements: Eytzinger layout fits
  immutable search arrays, while galloping search fits cursor movement with a
  position hint.

### Interpretation

- Trine should expose stable `seek` and `advance_to` index APIs while keeping
  the algorithm behind an internal search policy.
- Primary SSTable record order should remain sorted for range scans,
  validation, and simple recovery.

### Recommended Next Action

- When implementation reaches SSTable indexes, add canonical sorted-search
  tests first, then add optimized search layouts behind benchmarked thresholds.

## 2026-05-25: Prefix Filters And Compression Policy Added

### Observation

- Prefix scan is a common KV operation and should not depend only on caller-side
  range construction.
- SSTable block decompression sits on the read path. Fast decompression is more
  important for hot blocks than maximum compression ratio.
- A compact zlib-style codec is still useful for workloads that value space over
  CPU.

### Interpretation

- Prefix extractor and prefix filter support must be part of v1 table format,
  keyspace options, tests, and metrics.
- Trine should default to a fast block codec implemented with `lz4_flex`, while
  also supporting a compact zlib/DEFLATE codec implemented with `flate2`.
- On-disk codec ids should be Trine names, not Rust crate names.

### Recommended Next Action

- During crate scaffolding, add codec and prefix-filter modules as first-class
  boundaries instead of burying them inside SSTable reader code.
