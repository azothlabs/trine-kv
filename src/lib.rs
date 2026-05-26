//! Trine KV is an embedded LSM MVCC key-value database.
//!
//! The v1 API exposes in-memory and persistent databases, named buckets,
//! atomic write batches, snapshots, optimistic transactions, range/prefix
//! iteration, WAL recovery, `SSTable` flush/compaction, and live stats.

#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

pub mod blob;
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
pub mod prefix;
mod range_tombstone;
pub mod recovery;
pub mod search;
pub mod snapshot;
pub mod stats;
pub mod table;
pub mod transaction;
pub mod types;
pub mod version;
pub mod wal;
pub mod write_batch;

pub use bucket::{Bucket, BucketName};
pub use db::Db;
pub use error::{Error, Result};
pub use iterator::{Direction, Iter};
pub use mvcc::SnapshotSequence;
pub use options::{
    BucketOptions, CompressionProfile, DbOptions, DurabilityMode, FailOnCorruptionPolicy,
    FilterPolicy, IndexSearchPolicy, PrefixFilterPolicy, StorageMode, WriteOptions,
};
pub use prefix::PrefixExtractor;
pub use recovery::RecoveryReport;
pub use snapshot::Snapshot;
pub use stats::DbStats;
pub use transaction::{Transaction, TransactionOptions};
pub use types::{CommitInfo, KeyRange, KeyValue, Sequence, Value};
pub use write_batch::WriteBatch;
