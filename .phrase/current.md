# Current Phase

## Status

Complete

## Goal

Document the `0.3.0` read-version and checkpoint feature set before release.

## Scope

- Add a runnable example for `ReadVersion`, `snapshot_at`, named checkpoints,
  checkpoint lookup after reopen, and expiration after deletion.
- Update README feature summaries, command lists, and example index.
- Update the usage guide dependency version, historical-read section, and
  verification path.
- Update release checklist and changelog so the new example is part of the
  release story.

## Out Of Scope

- Changing storage behavior, retention semantics, manifest format, or public
  API names.
- Publishing, tagging, pushing, or creating a GitHub release.
- Adding more checkpoint lifecycle features beyond the existing public API.

## Acceptance Gate

- `cargo run --example read_versions` passes.
- All README/usage/release command lists mention the new example.
- Usage docs explain `ReadVersion`, `snapshot_at`,
  `DbOptions::with_keep_last_read_versions`, and checkpoint deletion.
- Focused clippy, doctests, full tests, all examples, diff checks, and scans
  pass.

## Active Task Slice

```text
task641 [x] goal:add checked read-version example | scope:examples/read_versions.rs | verify:cargo run --example read_versions
task642 [x] goal:update user docs | scope:README docs/usage docs/release CHANGELOG | verify:rg/read_versions + docs scan
task643 [x] goal:verify and commit docs slice | scope:examples docs phrase | verify:clippy/doctest/full tests/examples/scans
```

## Evidence

- Existing docs mentioned `ReadVersion` only lightly and `docs/usage.md` still
  showed `0.1` dependency examples.
- No runnable example covered checkpoint reopen and expiration behavior.
- The new `read_versions` example passes and demonstrates durable checkpoint
  lookup after reopen plus expiration after checkpoint deletion.

## Known Residuals

- None for this documentation slice.

## Next Recommendation

- Continue release operations only after an explicit tag/publish request.
