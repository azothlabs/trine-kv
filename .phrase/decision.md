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
- Do not read archive material by default.
- Do not maintain mechanical per-file changelogs when the git diff is enough.

## Trine KV V1 Source Of Truth

- V1 engine decision: `.phrase/adr/0001-v1-lsm-mvcc-engine.md`.
- V1 protocol and storage contract: `.phrase/protocol/trine-kv-v1-spec.md`.
- Trine specs, ADRs, tests, and local design notes are the source of truth.
- Do not implement Trine by depending on another storage engine.
- Do not change MVCC, WAL, SSTable, manifest, compaction, transaction,
  prefix-filter, compression, or search-policy behavior without updating the
  protocol spec or adding a follow-up ADR.

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
