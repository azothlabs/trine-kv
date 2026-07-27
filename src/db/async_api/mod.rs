//! Primary asynchronous database operations.
//!
//! Opening/refresh, bucket/checkpoint management, default-bucket data access,
//! and maintenance each own their orchestration. Synchronous APIs remain thin
//! adapters to the same engine boundaries.

use super::open_helpers::{
    buckets_from_manifest_async, ensure_default_bucket_loaded, lock_poisoned,
    object_store_committed_wal_batches, require_internal_checkpoint_name, validate_checkpoint_name,
};
use super::{
    Arc, Bucket, BucketName, BucketOptions, CommitInfo, DEFAULT_BUCKET_NAME, DatabaseStorageRef,
    Db, Direction, DurabilityMode, Error, HostStorageBackend, IntoOpenOptions, Iter, KeyRange,
    LazyIter, MaintenanceBudget, MaintenanceOutcome, ManifestStore, ObjectLeaseState,
    ObjectWriterLease, ReadVersion, Result, Snapshot, StorageMode, Value, WriteOptions, commit,
    manifest, recovery,
};
use crate::{
    bucket::{require_internal_bucket, validate_user_named_bucket},
    object_store::canonical_object_key,
    substrate::object_store_wal_batches_after_replay_floor,
};

mod buckets;
mod data;
mod maintenance;
mod open;
