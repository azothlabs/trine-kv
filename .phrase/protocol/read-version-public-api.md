# Read Version Public API

Date: 2026-06-12

## Status

Accepted and implemented for the `ReadVersion`, checkpoint, and configurable
recent-retention slices.

## Purpose

Trine should let callers read a consistent database state from an earlier
committed point without requiring them to understand how the engine assigns or
publishes internal commit numbers.

The public concept is `ReadVersion`: a stable numeric cursor for a database
state that can be used as a read boundary. The number is public so applications
can persist and exchange cursors, but the API must not require callers to
understand the engine's internal allocation or publication mechanics.

## Design Rules

- Public APIs should use `ReadVersion`, not internal sequence terminology.
- A `ReadVersion` is database-scoped, not bucket-scoped.
- A `ReadVersion` identifies a committed state, never an in-progress write.
- A successful atomic write that changes visible state returns the read version
  at which the whole write became visible.
- An accepted empty write batch does not create a new database state; it returns
  the latest read version that was already visible.
- The main read boundary is `Snapshot`; version-specific point and range reads
  should be expressed by creating a snapshot at a read version.
- Reads must never silently fall back to the latest state when a requested read
  version is unavailable.
- Retention is part of the public contract. Historical reads are guaranteed
  only for retained read versions.

## Public Vocabulary

```rust
pub struct ReadVersion(u64);
```

`ReadVersion` should be a newtype rather than a type alias. The API exposes
stable conversion helpers so applications can persist cursors while keeping
engine-internal sequence types out of the user model.

Recommended surface:

```rust
impl ReadVersion {
    pub const ZERO: Self;
    pub const fn from_u64(value: u64) -> Self;
    pub const fn as_u64(self) -> u64;
}
```

`ReadVersion::ZERO` is the empty database state before the first successful
write. It is valid when retained.

## Scope And Portability

A read version is meaningful only for the database lineage that produced it.
Using a read version from one database with another database has no semantic
guarantee, even if the numeric value happens to fall inside the other
database's retained range.

Export, import, restore, or replication features must define their own
lineage-mapping rules before promising that read versions survive those
operations unchanged.

## Primary API

```rust
impl Db {
    pub fn latest_read_version(&self) -> ReadVersion;
    pub fn oldest_retained_read_version(&self) -> ReadVersion;
    pub fn snapshot_at(&self, version: ReadVersion) -> Result<Snapshot>;

    pub async fn create_checkpoint(&self, name: &str) -> Result<ReadVersion>;
    pub async fn delete_checkpoint(&self, name: &str) -> Result<()>;
    pub async fn checkpoint_read_version(&self, name: &str) -> Result<ReadVersion>;

    pub fn create_checkpoint_sync(&self, name: &str) -> Result<ReadVersion>;
    pub fn delete_checkpoint_sync(&self, name: &str) -> Result<()>;
    pub fn checkpoint_read_version_sync(&self, name: &str) -> Result<ReadVersion>;
}

impl Snapshot {
    pub fn read_version(&self) -> ReadVersion;
}

impl CommitInfo {
    pub fn read_version(self) -> ReadVersion;
}
```

`Db::snapshot()` remains the convenient way to create a snapshot at the latest
read version.

Existing snapshot reads stay the preferred shape:

```rust
let snapshot = db.snapshot_at(version)?;
let value = bucket.get_at(&snapshot, key).await?;
let rows = bucket.range_at(&snapshot, range).await?;
```

Version-specific convenience methods such as `get_at_version` or
`range_at_version` should be added only if user evidence shows they reduce real
friction. They duplicate snapshot validation and pinning semantics, so they are
not the core design.

## Error Semantics

```rust
Error::ReadVersionTooNew {
    requested: ReadVersion,
    latest: ReadVersion,
}

Error::ReadVersionExpired {
    requested: ReadVersion,
    oldest_retained: ReadVersion,
}
```

Rules:

- `requested > latest_read_version()` returns `ReadVersionTooNew`.
- `requested < oldest_retained_read_version()` returns `ReadVersionExpired`.
- A missing key at a valid read version returns `Ok(None)`.
- A key hidden by a delete at a valid read version returns `Ok(None)`.
- The database must not read the latest state as a fallback for either error.

`InvalidReadVersion` should not be added unless a future format reserves values
that cannot represent a database state. Zero is not invalid by itself.

## Retention Contract

Historical reads are safe only when the requested read version is retained.
Trine should expose this directly through `oldest_retained_read_version`.

The retention floor is the oldest read version that Trine promises to answer.
Cleanup work may keep older bytes temporarily, but callers must treat read
versions below the floor as expired.

The long-term retention sources are:

- configured recent-history retention;
- active snapshots;
- named checkpoints.

The effective retained floor is the oldest read version needed by any retention
source. Cleanup must not remove data needed by that floor.

## Checkpoints

Checkpoints are persistent named pins for read versions. They are a follow-up
surface from the first `ReadVersion` slice and part of the implemented
retention contract.

Implemented API:

```rust
impl Db {
    pub async fn create_checkpoint(&self, name: &str) -> Result<ReadVersion>;
    pub async fn delete_checkpoint(&self, name: &str) -> Result<()>;
    pub async fn checkpoint_read_version(&self, name: &str) -> Result<ReadVersion>;

    pub fn create_checkpoint_sync(&self, name: &str) -> Result<ReadVersion>;
    pub fn delete_checkpoint_sync(&self, name: &str) -> Result<()>;
    pub fn checkpoint_read_version_sync(&self, name: &str) -> Result<ReadVersion>;
}
```

Recommended errors:

```rust
Error::CheckpointAlreadyExists { name: String }
Error::CheckpointNotFound { name: String }
```

`create_checkpoint(name)` pins the latest read version by default. Replacement
semantics should not be implicit; if replacement is needed later, add an
explicit API.

Checkpoint creation and deletion update persistent metadata in durable storage
modes. The async methods are therefore the primary API, and the `*_sync`
methods are native blocking adapters over the same engine behavior.

Persistent and object-store databases store checkpoint pins in the manifest.
In-memory databases use process-local checkpoint metadata with the same public
semantics.

## Configurable Recent Retention

`DbOptions::with_keep_last_read_versions(count)` configures how many recent
read versions are retained without an active snapshot or checkpoint. `count`
is measured in read versions, not bytes or time. `0` is invalid.

The default is `1`, meaning the latest read version is retained by
configuration. Active snapshots and checkpoints can still retain older versions
outside the configured recent window.

## Non-Goals

- Writable branches.
- Merge or rebase APIs.
- Exposing internal commit allocation or WAL ordering.
- Opportunistic reads below the retained floor.
- Changing normal latest reads.

## Acceptance Gates

The first implementation phase should prove:

- latest reads keep their current behavior;
- each successful state-changing atomic write returns one read version for the
  whole write;
- empty write batches return the current latest read version without creating a
  new state;
- `snapshot_at(version)` reads a consistent view across buckets;
- range reads return each user key at most once and in user-key order;
- deleted keys are hidden at valid read versions;
- too-new and expired versions return typed errors;
- cleanup preserves every retained read version and rejects older versions
  consistently.
