//! Trine KV is an embedded LSM MVCC key-value database.
//!
//! The v1 API exposes a built-in default bucket for direct `Db` reads and
//! writes, optional named buckets, atomic write batches, snapshots, optimistic
//! transactions, range/prefix iteration, WAL recovery, `SSTable`
//! flush/compaction, and live stats.

#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

pub mod blob;
mod block;
pub mod bucket;
pub mod cache;
pub mod codec;
pub mod compaction;
pub mod db;
mod durability;
pub mod error;
pub mod filter;
pub mod internal_key;
pub mod iterator;
mod lsm;
pub mod manifest;
pub mod memtable;
pub mod mvcc;
pub mod options;
pub mod point_value;
pub mod prefix;
mod range_tombstone;
pub mod recovery;
pub mod search;
pub mod snapshot;
pub mod stats;
mod storage;
pub mod table;
pub mod transaction;
pub mod types;
pub mod version;
pub mod wal;
pub mod write_batch;

pub use bucket::{Bucket, BucketName, BucketReader};
pub use db::Db;
pub use error::{Error, Result};
pub use iterator::{Direction, Iter, LazyIter, LazyKeyValue, LazyValue};
pub use mvcc::SnapshotSequence;
pub use options::{
    BlobGcRatio, BlobLevelMergePolicy, BucketOptions, CompressionProfile, DbOptions,
    DurabilityMode, FailOnCorruptionPolicy, FilterPolicy, IndexSearchPolicy, PrefixFilterPolicy,
    StorageMode, WriteOptions,
};
pub use point_value::PointValue;
pub use prefix::PrefixExtractor;
pub use recovery::RecoveryReport;
pub use snapshot::Snapshot;
pub use stats::DbStats;
pub use transaction::{Transaction, TransactionOptions};
pub use types::{CommitInfo, KeyRange, KeyValue, Sequence, Value};
pub use write_batch::WriteBatch;
