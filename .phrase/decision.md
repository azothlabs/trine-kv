# Decision Framework

## Principles

- Use minimal default context.
- Let evidence choose the next phase.
- Keep roadmaps at phase granularity.
- Keep tasks local to the current phase.
- Record only decision-relevant memory.
- Verify before claiming completion.

## Evidence Rules

Accepted evidence may include tests, traces, benchmarks, audits, user
observations, incident facts, data checks, manual verification, and prototype
results.

Evidence notes should separate:

- observation
- interpretation
- recommended next action

## Durable Boundaries

- Do not silently change stable contracts; update ADR or protocol docs.
- Do not pre-split future roadmap phases into tasks.
- Do not read archive files by default.
- Do not maintain mechanical per-file changelogs when the git diff is enough.
- Write comments for non-obvious Rust engine invariants, especially lock order,
  MVCC visibility, batch atomicity, and storage-format assumptions.
- Prefer concrete storage-engine language in explanations and comments.

## Trine KV V1 Source Of Truth

- V1 engine decision: `.phrase/adr/0001-v1-lsm-mvcc-engine.md`.
- V1 protocol and storage contract: `.phrase/protocol/trine-kv-v1-spec.md`.
- Trine specs, ADRs, tests, and local design notes are the source of truth.
- Do not implement Trine by depending on another storage engine.
- V1 compression uses only `none` and `lz4_flex`-backed fast block compression;
  do not add zlib/DEFLATE or `flate2` for v1.
- Public crate versions use Semantic Versioning. Before `1.0.0`, breaking
  public API or storage-contract changes should increment the minor version;
  compatible fixes should increment the patch version.
- Do not change MVCC, WAL, SSTable, manifest, compaction, transaction,
  prefix-filter, compression, or search-policy behavior without updating the
  protocol spec or adding a follow-up ADR.
- Trine's primary database API and storage boundary are async-first. Blocking
  native APIs are adapters over the primary async engine.
- Persistent storage behavior is governed by backend capabilities, including
  writer lease, manifest publish, durability strength, and background-work
  support.
- WASM readiness is a design constraint for public API, runtime boundary, and
  storage backend boundaries.
- Titan and other external storage engines may be used as design references,
  but Trine must keep its own code, file formats, tests, and recovery contract.

## Phase Gate Rules

A phase can close only when:

- acceptance gate is checked
- verification evidence exists
- remaining blockers are recorded
- next phase recommendation is written
- durable decisions are updated if needed

## Rejected Paths

- Full-history loading as the default agent behavior.
- Static spec/plan/task/change bookkeeping for every session.
- Treating stale plans as current truth after fresh evidence contradicts them.
- Abstract jargon when plain engine terminology is clearer.
