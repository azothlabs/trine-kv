//! Trine KV is an embedded LSM MVCC key-value database.
//!
//! This crate is currently in its scaffold phase. The public modules mirror the
//! accepted v1 protocol so implementation slices can land behind stable
//! boundaries.

#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

pub mod blob;
pub mod cache;
pub mod codec;
pub mod compaction;
pub mod db;
pub mod error;
pub mod filter;
pub mod internal_key;
pub mod iterator;
pub mod keyspace;
pub mod manifest;
pub mod memtable;
pub mod mvcc;
pub mod options;
pub mod prefix;
pub mod search;
pub mod snapshot;
pub mod stats;
pub mod table;
pub mod transaction;
pub mod types;
pub mod version;
pub mod wal;
pub mod write_batch;

pub use db::Db;
pub use error::{Error, Result};
pub use iterator::{Direction, Iter};
pub use keyspace::{Keyspace, KeyspaceName};
pub use mvcc::SnapshotSequence;
pub use options::{
    CompressionProfile, DbOptions, DurabilityMode, FailOnCorruptionPolicy, FilterPolicy,
    IndexSearchPolicy, KeyspaceOptions, PrefixFilterPolicy, StorageMode, WriteOptions,
};
pub use prefix::PrefixExtractor;
pub use snapshot::Snapshot;
pub use stats::DbStats;
pub use transaction::{Transaction, TransactionOptions};
pub use types::{CommitInfo, KeyRange, KeyValue, Sequence, Value};
pub use write_batch::WriteBatch;
