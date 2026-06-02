//! Trine KV is an embedded LSM MVCC key-value database.
//!
//! The v1 API exposes a built-in default bucket for direct `Db` reads and
//! writes, optional named buckets, atomic write batches, snapshots, optimistic
//! transactions, range/prefix iteration, WAL recovery, `SSTable`
//! flush/compaction, async-first host-storage entry points, explicit sync
//! adapters, and live stats.

#![warn(missing_docs)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

#[doc(hidden)]
pub mod blob;
mod block;
/// Bucket handles and bucket-bound readers.
pub mod bucket;
#[allow(dead_code)]
mod cache;
mod checksum;
#[doc(hidden)]
pub mod codec;
mod compaction;
/// Database open, read, write, scan, and maintenance APIs.
pub mod db;
mod durability;
/// Error and result types returned by Trine KV.
pub mod error;
#[allow(dead_code)]
mod filter;
#[doc(hidden)]
pub mod internal_key;
mod io;
/// Forward and reverse iterators over committed rows.
pub mod iterator;
mod lsm;
#[doc(hidden)]
pub mod manifest;
#[allow(dead_code)]
mod memtable;
#[allow(dead_code)]
mod mvcc;
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
/// Search policy helpers for table indexes.
pub mod search;
/// Snapshot handles for repeatable reads.
pub mod snapshot;
/// Live database statistics exposed to callers.
pub mod stats;
mod storage;
#[doc(hidden)]
pub mod table;
/// Optimistic transaction API.
pub mod transaction;
/// Core key, value, range, sequence, and commit types.
pub mod types;
mod version;
#[doc(hidden)]
pub mod wal;
/// Atomic write batch types.
pub mod write_batch;

pub use bucket::{Bucket, BucketName, BucketReader};
pub use db::{Db, IntoOpenOptions, MaintenanceBudget, MaintenanceOutcome};
pub use error::{Error, Result};
pub use iterator::{Direction, Iter, LazyIter, LazyKeyValue, LazyValue};
pub use mvcc::SnapshotSequence;
pub use options::{
    BlobGcRatio, BlobLevelMergePolicy, BucketOptions, CompressionProfile, DbOptions,
    DurabilityMode, FailOnCorruptionPolicy, FilterPolicy, HostStorageBackend, IndexSearchPolicy,
    PrefixFilterPolicy, StorageMode, WriteOptions,
};
pub use point_value::PointValue;
pub use prefix::PrefixExtractor;
pub use recovery::RecoveryReport;
pub use runtime::{CancellationToken, RuntimeCapabilities, RuntimeMode, RuntimeOptions};
pub use snapshot::Snapshot;
pub use stats::DbStats;
pub use transaction::{Transaction, TransactionOptions};
pub use types::{CommitInfo, KeyRange, KeyValue, Sequence, Value};
pub use write_batch::WriteBatch;
