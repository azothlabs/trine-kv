//! Trine KV is an embedded LSM MVCC key-value database.
//!
//! The v1 API exposes a built-in default bucket for direct `Db` reads and
//! writes, optional named buckets, atomic write batches, snapshots, optimistic
//! transactions, range/prefix iteration, WAL recovery, `SSTable`
//! flush/compaction, async-first host-storage entry points, explicit sync
//! adapters, and live stats.

#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

#[doc(hidden)]
pub mod blob;
mod block;
pub mod bucket;
#[allow(dead_code)]
mod cache;
mod checksum;
#[doc(hidden)]
pub mod codec;
mod compaction;
pub mod db;
mod durability;
pub mod error;
#[allow(dead_code)]
mod filter;
#[doc(hidden)]
pub mod internal_key;
mod io;
pub mod iterator;
mod lsm;
#[doc(hidden)]
pub mod manifest;
#[allow(dead_code)]
mod memtable;
#[allow(dead_code)]
mod mvcc;
pub mod options;
mod point_value;
pub mod prefix;
mod range_tombstone;
pub mod recovery;
pub mod runtime;
pub mod search;
pub mod snapshot;
pub mod stats;
mod storage;
#[doc(hidden)]
pub mod table;
pub mod transaction;
pub mod types;
mod version;
#[doc(hidden)]
pub mod wal;
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
