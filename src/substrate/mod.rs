//! Durability substrate — the Band 3 seam from `docs/storage-substrate-seam.md`.
//!
//! This isolates the *runtime* durability operations whose semantics genuinely
//! diverge between storage backends:
//!
//! - the **write-ahead log** lifecycle (filesystem appends to one growing file
//!   per shard; object storage appends frames into immutable WAL segments and
//!   advances a CAS-protected remote WAL head),
//!   and
//! - the **single-writer lease** (filesystem holds a `LOCK` file via a writer
//!   lease; object storage needs a lease object + TTL + fencing token).
//!
//! Everything else is already abstracted: byte-level object IO stays on the
//! fine-grained `Storage*Backend` traits (Band 2), and the manifest publish
//! point lives in [`crate::manifest::ManifestStore`] — made conflict-aware in
//! slice 2b ① ([`crate::manifest::PublishOutcome`]).
//!
//! `DbInner` holds a `DurabilitySubstrate` and the commit / flush / close paths
//! drive it. The `Filesystem` variant wraps the real `WalFrontDoor` +
//! `ProcessLock`; the `ObjectStore` variant publishes immutable remote WAL
//! segments and uses the writer lease object as the CAS-protected WAL head and
//! fencing token.

#[cfg(not(target_os = "wasi"))]
use crate::error::Error;
use crate::error::Result;
use crate::options::DurabilityMode;
use crate::types::Sequence;
use crate::wal::WalFrontDoorStats;
use crate::write_batch::BatchOperation;

#[cfg(not(target_os = "wasi"))]
pub(crate) use object_store::ObjectWalWaiter;
pub(crate) use object_store::{
    ObjectLeaseState, ObjectWriterLease, object_store_wal_batches_after_replay_floor,
    validate_object_lease_wal_key_capacity,
};

mod filesystem;
mod object_store;

pub(crate) use filesystem::FilesystemSubstrate;
pub(crate) use object_store::ObjectStoreSubstrate;

#[cfg(feature = "fuzzing")]
pub(crate) use object_store::fuzz_decode_object_control;

/// Backend-specific runtime durability operations (WAL lifecycle + writer
/// lease) that the commit / flush / close paths drive.
///
/// Dispatch is an enum rather than `dyn` so the database owns one explicit
/// durability state without adding a backend type parameter to `DbInner`.
#[derive(Debug)]
pub(crate) enum DurabilitySubstrate {
    /// Native filesystem: appendable WAL files + a `LOCK` writer lease.
    Filesystem(FilesystemSubstrate),
    /// Object storage: immutable remote WAL segments + a fencing-epoch writer
    /// lease that also stores the remote WAL head.
    ObjectStore(ObjectStoreSubstrate),
}

impl DurabilitySubstrate {
    /// Whether a write-ahead log is present. A read-only open has none.
    pub(crate) fn wal_is_present(&self) -> bool {
        match self {
            Self::Filesystem(substrate) => substrate.wal_is_present(),
            Self::ObjectStore(_) => true,
        }
    }

    /// This writer's fencing epoch for the object-store backend, stamped into
    /// manifest publishes so a stale prior owner is fenced. `None` for the
    /// filesystem backend (mutual exclusion is the `LOCK` file, not an epoch).
    pub(crate) fn object_fencing_epoch(&self) -> Option<u64> {
        match self {
            Self::Filesystem(_) => None,
            Self::ObjectStore(substrate) => Some(substrate.fencing_epoch()),
        }
    }

    /// Append a commit's operations to the WAL (no-op when there is no WAL).
    pub(crate) fn accept_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => {
                substrate.accept_commit(sequence, operations, durability)
            }
            Self::ObjectStore(substrate) => {
                substrate.accept_commit(sequence, operations, durability)
            }
        }
    }

    /// Append a commit's operations to the WAL and await the WAL lane
    /// completion when the substrate has one.
    #[cfg(not(target_os = "wasi"))]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(crate) async fn accept_commit_async(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => {
                substrate
                    .accept_commit_async(sequence, operations, durability)
                    .await
            }
            Self::ObjectStore(substrate) => {
                substrate
                    .enqueue_commit(sequence, operations, durability)?
                    .wait()
                    .await
            }
        }
    }

    #[cfg(not(target_os = "wasi"))]
    pub(crate) fn enqueue_object_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<ObjectWalWaiter> {
        match self {
            Self::ObjectStore(substrate) => {
                substrate.enqueue_commit(sequence, operations, durability)
            }
            Self::Filesystem(_) => Err(Error::unsupported_backend(
                "object WAL enqueue requires object-store persistence",
            )),
        }
    }

    pub(crate) async fn fence_object_mutation_async(&self) -> Result<()> {
        match self {
            Self::Filesystem(_) => Ok(()),
            Self::ObjectStore(substrate) => substrate.fence_mutation_async().await,
        }
    }

    /// Flush WAL durability to the requested level (no-op when there is no WAL).
    pub(crate) fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => substrate.persist_wal(durability),
            Self::ObjectStore(substrate) => substrate.persist_wal(durability),
        }
    }

    /// Flush WAL durability to the requested level and await the WAL lane
    /// completion when the substrate has one.
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(crate) async fn persist_wal_async(&self, durability: DurabilityMode) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => substrate.persist_wal_async(durability).await,
            Self::ObjectStore(substrate) => substrate.persist_wal(durability),
        }
    }

    /// WAL statistics, or `None` when there is no WAL.
    pub(crate) fn wal_stats(&self) -> Option<WalFrontDoorStats> {
        match self {
            Self::Filesystem(substrate) => substrate.wal_stats(),
            Self::ObjectStore(substrate) => Some(substrate.wal_stats()),
        }
    }

    /// Truncate the WAL below a checkpoint after a memtable flush advances the
    /// replay floor (no-op when there is no WAL).
    pub(crate) fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => substrate.rewrite_wal_after_replay_floor(replay_floor),
            Self::ObjectStore(substrate) => substrate.rewrite_wal_after_replay_floor(replay_floor),
        }
    }

    /// Truncate WAL data below a checkpoint and await the WAL lane completion
    /// when the substrate has one.
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(crate) async fn rewrite_wal_after_replay_floor_async(
        &self,
        replay_floor: Sequence,
    ) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => {
                substrate
                    .rewrite_wal_after_replay_floor_async(replay_floor)
                    .await
            }
            Self::ObjectStore(substrate) => substrate.rewrite_wal_after_replay_floor(replay_floor),
        }
    }

    /// Release the single-writer lease (idempotent; called on close).
    ///
    /// The object-store lease is a fencing-epoch object reclaimed by the next
    /// writer's higher epoch (and by TTL in a real deployment), so this is a
    /// no-op there: deleting it would be an async object op, and a stale lease
    /// object does not block reopen (acquire takes over by bumping the epoch).
    pub(crate) fn release_writer_lease(&self) {
        match self {
            Self::Filesystem(substrate) => substrate.release_writer_lease(),
            Self::ObjectStore(substrate) => substrate.release_writer_lease(),
        }
    }
}
