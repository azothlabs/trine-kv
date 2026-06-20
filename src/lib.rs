//! Trine KV is an embedded LSM MVCC key-value database.
//!
//! Use Trine KV when an application needs a local key/value store with
//! persistent storage, atomic batches, snapshots, range scans, prefix scans,
//! and optimistic transactions. The primary API is async-first; synchronous
//! callers use the explicit `*_sync` adapters.
//!
//! # Quick start
//!
//! `Db::open` and `Db::open_sync` are path-first. Passing a path opens a
//! persistent database. Use `DbOptions::memory()` when the database should live
//! only in memory.
//!
//! ```rust
//! use trine_kv::Db;
//!
//! # fn main() -> trine_kv::Result<()> {
//! let db = Db::open_sync("target/doc-example-basic")?;
//! db.put_sync(b"user:1", b"Ada")?;
//!
//! let value = db.get_sync(b"user:1")?;
//! assert_eq!(value, Some(b"Ada".to_vec()));
//! # Ok(())
//! # }
//! ```
//!
//! # Core concepts
//!
//! - [`Db`] is the database handle. Direct `Db` read and write methods operate
//!   on the built-in default bucket.
//! - [`Bucket`] is a handle for an optional named bucket with its own options,
//!   memtables, tables, filters, and compaction state.
//! - [`WriteBatch`] groups puts, point deletes, and range deletes into one
//!   atomic commit.
//! - [`ReadVersion`] is the application-facing cursor for a committed database
//!   state. [`Snapshot`] pins one so repeated reads see a stable view while
//!   newer writes continue.
//! - [`Transaction`] records reads and stages writes, then rejects commit if a
//!   later committed write conflicts with the read set.
//!
//! # Durability
//!
//! Persistent databases default to safety-first durability for confirmed
//! writes. Lower durability modes such as [`DurabilityMode::Buffered`] are
//! available through [`WriteOptions`] for data that can tolerate losing recent
//! confirmed writes after a crash.
//!
//! # Platform I/O features
//!
//! The async API works without feature flags. Enable `platform-io` when
//! native-file storage should complete through Trine's portable platform I/O
//! boundary, backed by a bounded Trine-owned thread pool. Enable
//! `platform-io-native` when Trine should prefer audited native async backends
//! and use the same thread-pool backend for unsupported operation rows.
//!
//! After enabling either feature, select the platform I/O runtime for a
//! database:
//!
//! ```rust
//! use trine_kv::{Db, DbOptions, RuntimeOptions};
//!
//! # async fn example() -> trine_kv::Result<()> {
//! let mut options = DbOptions::new("target/doc-example-platform-io");
//! options.runtime = RuntimeOptions::platform_io();
//!
//! let db = Db::open(options).await?;
//! db.put(b"k", b"v").await?;
//! db.flush().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Use [`DbStats::storage_platform_io_operations`] to inspect whether each
//! Trine operation completed as true native async, partial native async,
//! thread-pool managed async, fallback, or unsupported work.

#![warn(missing_docs)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

mod blob;
mod block;
pub mod branch;
/// Bucket handles and bucket-bound readers.
pub mod bucket;
#[allow(dead_code)]
mod cache;
mod checksum;
mod codec;
mod compaction;
/// Database open, read, write, scan, and maintenance APIs.
pub mod db;
mod durability;
/// Error and result types returned by Trine KV.
pub mod error;
#[allow(dead_code)]
mod filter;
mod internal_key;
mod io;
/// Forward and reverse iterators over committed rows.
pub mod iterator;
mod limits;
mod lsm;
mod manifest;
#[allow(dead_code)]
mod memtable;
#[allow(dead_code)]
mod mvcc;
/// Provider-agnostic object-store client ("bring your own object store"): the
/// `ObjectClient` trait, its `ETag`/conditional-write types, and an in-memory
/// fake. Implement it for S3 (or use the `s3` feature) and open with
/// [`Db::open_object_store`].
pub mod object_store;
/// Database, bucket, write, storage, runtime, and durability options.
pub mod options;
mod point_value;
/// Prefix extraction policies used by prefix filters.
pub mod prefix;
mod range_tombstone;
/// Startup recovery helpers and recovery reports.
pub mod recovery;
/// Runtime selection, capabilities, and cancellation support.
pub mod runtime;
/// Real object-storage `ObjectClient` (S3 and compatible) via the `object_store`
/// crate. Enabled by the `s3` feature.
#[cfg(feature = "s3")]
pub mod s3;
/// Search policy helpers for table indexes.
pub mod search;
/// Snapshot handles for repeatable reads.
pub mod snapshot;
/// Live database statistics exposed to callers.
pub mod stats;
mod storage;
mod substrate;
mod table;
/// Optimistic transaction API.
pub mod transaction;
/// Core key, value, range, read-version, and commit types.
pub mod types;
mod version;
mod wal;
/// Atomic write batch types.
pub mod write_batch;

pub use branch::{Branch, BranchInfo, BranchRange};
pub use bucket::{Bucket, BucketName, BucketReader};
pub use db::{Db, IntoOpenOptions, MaintenanceBudget, MaintenanceOutcome};
pub use error::{Error, Result};
pub use iterator::{Direction, Iter, LazyIter, LazyKeyValue, LazyValue};
pub use object_store::{
    ETag, InMemoryObjectStore, ObjectClient, ObjectFuture, ObjectMeta, Precondition, PutIf,
};
pub use options::{
    BlobGcRatio, BlobLevelMergePolicy, BucketOptions, CompressionProfile, DbOptions,
    DurabilityMode, FailOnCorruptionPolicy, FilterDepthCurve, FilterPolicy, HostStorageBackend,
    IndexSearchPolicy, PrefixFilterPolicy, StorageMode, WalShardPolicy, WriteOptions,
};
pub use point_value::PointValue;
pub use prefix::PrefixExtractor;
pub use recovery::RecoveryReport;
pub use runtime::{CancellationToken, RuntimeCapabilities, RuntimeMode, RuntimeOptions};
pub use snapshot::Snapshot;
pub use stats::{
    CompactionSkip, CompactionSkipStats, CompactionTrigger, CompactionTriggerStats, DbStats,
    FilterStats, LevelFilterStats, LevelStats, PlatformIoClassCounters, PlatformIoOperationStats,
};
pub use transaction::{Transaction, TransactionOptions};
pub use types::{CommitInfo, KeyRange, KeyValue, ReadVersion, Value};
pub use wal::is_wal_object_key;
pub use write_batch::WriteBatch;

#[cfg(test)]
mod persistent_wal_tests {
    use crate as trine_kv;

    include!("../tests/internal/persistent_wal.rs");
}
