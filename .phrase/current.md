# Current Phase

## Status

In progress

## Goal

Freeze the Trine KV v1 database specification before implementation.

## Entry Condition

- The project is a clean repository skeleton.
- The user wants a serious embedded LSM KV, independent from previous projects.
- Trine specs and tests are the source of truth.

## Scope

- Define v1 database capabilities.
- Define MVCC, transactions, LSM layout, WAL, SSTable, manifest, compaction,
  recovery, in-memory mode, prefix filters, compression, search policy, and
  public API shape.
- Record the project independence boundary as a durable decision.

## Out Of Scope

- Writing Rust implementation code.
- Benchmarking before the engine exists.
- Depending on another storage engine.
- Choosing a specific compression codec name as part of the core contract.

## Acceptance Gate

- `.phrase/adr/0001-v1-lsm-mvcc-engine.md` records the durable design decision.
- `.phrase/protocol/trine-kv-v1-spec.md` defines the v1 protocol and storage
  contract.
- The spec includes in-memory mode and persistent mode.
- The spec includes prefix extractor and prefix filter rules.
- The spec chooses a fast default compression codec and compact zlib option.
- The spec includes search-policy rules for immutable indexes and iterators.
- The spec includes required tests and benchmarks.

## Active Task Slice

```text
task001 [x] goal:v1 spec exists | scope:.phrase/adr,.phrase/protocol | verify:manual review + diff check
```

## Known Blockers

- No Cargo project exists yet; implementation starts after spec review.

## Evidence To Record

- Spec files created.
- User review outcome.
- Any capability that should be cut or strengthened before coding.
