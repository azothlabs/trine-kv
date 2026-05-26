# Current Phase

## Status

Complete

## Goal

Polish the public API around buckets and make the default bucket the direct
`Db` read/write target.

## Entry Condition

- Phase 30 completed local P0-P10 LSM hardening verification.
- User requested renaming the public key namespace concept to bucket and
  making common operations work without explicitly opening a bucket.

## Scope

- Public API rename from the prior namespace naming to bucket naming.
- Add built-in default bucket direct helpers on `Db`.
- Keep named buckets available for logical isolation and custom options.
- Update examples, usage docs, protocol, tests, benches, and stats naming.

## Out Of Scope

- LSM core behavior changes beyond namespace naming and default-bucket routing.
- Large benchmark policy decisions.
- External test services.

## Acceptance Gate

- `Db::put/get/range/prefix` operate on the default bucket.
- `Db::open_bucket` and `Db::open_bucket_with_options` open named buckets.
- `BucketOptions` replaces the public options type for named buckets.
- Default bucket exists after open in memory and persistent modes.
- Usage docs and examples show the default path first.
- Full local Rust verification passes.

## Active Task Slice

```text
task103 [x] goal:rename public namespace API to bucket | scope:src,tests,examples,docs | verify:cargo check
task104 [x] goal:add default bucket direct Db helpers | scope:src/db.rs,tests | verify:focused default bucket tests
task105 [x] goal:update evidence and close phase | scope:.phrase | verify:full Rust gate
```

## Known Blockers

- Remote CI cannot be executed locally; it must run after push.

## Evidence

- Phase 30 full local verification passed.
- The prior public API required users to open a named handle before basic
  reads/writes, which made the simple path heavier than needed.
- The new API remains pre-1.0 and can make a breaking public rename under the
  existing Semantic Versioning rule.
- `Db::put/get/range/prefix` now route to the built-in default bucket, and
  named buckets are opened through `Db::open_bucket`.
- Persistent and in-memory opens create the default bucket before user data is
  read or written.
- Full local Rust verification passed for this phase.

## Next Recommendation

- Commit the Phase 31 API polish when ready, then push to CI for remote
  verification.
