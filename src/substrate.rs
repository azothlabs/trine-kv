//! Durability substrate — the Band 3 seam from `docs/storage-substrate-seam.md`.
//!
//! This isolates the *runtime* durability operations whose semantics genuinely
//! diverge between storage backends:
//!
//! - the **write-ahead log** lifecycle (filesystem appends to one growing file
//!   per shard; object storage has no append and must segment or go WAL-less),
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
//! drive it (slice 2b ③). The `Filesystem` variant wraps the real
//! `WalFrontDoor` + `ProcessLock`; the `ObjectStore` variant (slice 2c) is
//! WAL-less with a fencing-epoch lease and is constructed once the object-store
//! open path is wired (2c-4) — until then its arms + lease primitive are
//! exercised by unit tests.

use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::object_store::{ObjectClient, Precondition, PutIf};
use crate::options::DurabilityMode;
use crate::recovery::ProcessLock;
use crate::types::Sequence;
use crate::wal::{WalFrontDoor, WalFrontDoorStats};
use crate::write_batch::BatchOperation;

/// Backend-specific runtime durability operations (WAL lifecycle + writer
/// lease) that the commit / flush / close paths drive.
///
/// Dispatch is an enum rather than `dyn` to match the house style of
/// [`crate::storage::StorageBackend`] and `ManifestStoreBackend` — no vtable, no
/// viral type parameter on `DbInner`.
#[derive(Debug)]
pub(crate) enum DurabilitySubstrate {
    /// Native filesystem: appendable WAL files + a `LOCK` writer lease.
    Filesystem(FilesystemSubstrate),
    /// Object storage: WAL-less (durability is flush-to-object + manifest CAS)
    /// with a fencing-epoch writer lease.
    // Constructed once the object-store open path is wired (2c-4); the enum arms
    // and lease primitive below are exercised by unit tests until then.
    #[allow(dead_code)]
    ObjectStore(ObjectStoreSubstrate),
}

impl DurabilitySubstrate {
    /// Whether a write-ahead log is present. A read-only open has none; the
    /// object store is WAL-less and always reports `false`.
    pub(crate) fn wal_is_present(&self) -> bool {
        match self {
            Self::Filesystem(substrate) => substrate.wal_is_present(),
            Self::ObjectStore(_) => false,
        }
    }

    /// Append a commit's operations to the WAL (no-op when there is no WAL, and
    /// always a no-op for the WAL-less object store — its durability point is the
    /// memtable flush + manifest CAS, not a WAL append).
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
            Self::ObjectStore(_) => Ok(()),
        }
    }

    /// Flush WAL durability to the requested level (no-op when there is no WAL;
    /// no-op for the WAL-less object store).
    pub(crate) fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => substrate.persist_wal(durability),
            Self::ObjectStore(_) => Ok(()),
        }
    }

    /// WAL statistics, or `None` when there is no WAL (always `None` for the
    /// object store).
    pub(crate) fn wal_stats(&self) -> Option<WalFrontDoorStats> {
        match self {
            Self::Filesystem(substrate) => substrate.wal_stats(),
            Self::ObjectStore(_) => None,
        }
    }

    /// Truncate the WAL below a checkpoint after a memtable flush advances the
    /// replay floor (no-op when there is no WAL; no-op for the object store).
    pub(crate) fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => substrate.rewrite_wal_after_replay_floor(replay_floor),
            Self::ObjectStore(_) => Ok(()),
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
            Self::ObjectStore(_) => {}
        }
    }
}

/// Filesystem durability: an optional appendable [`WalFrontDoor`] and the
/// process [`ProcessLock`] writer lease.
#[derive(Debug)]
pub(crate) struct FilesystemSubstrate {
    wal: Option<WalFrontDoor>,
    process_lock: Mutex<Option<ProcessLock>>,
}

impl FilesystemSubstrate {
    /// Construct from the pieces the open path already discovers. `wal` is
    /// `None` for a read-only open; `process_lock` is `None` when locking is not
    /// in force for this open.
    pub(crate) fn new(wal: Option<WalFrontDoor>, process_lock: Option<ProcessLock>) -> Self {
        Self {
            wal,
            process_lock: Mutex::new(process_lock),
        }
    }

    fn wal_is_present(&self) -> bool {
        self.wal.is_some()
    }

    fn accept_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        if let Some(wal) = &self.wal {
            let accepted = wal.accept_commit(sequence, operations, durability)?;
            debug_assert_eq!(accepted.sequence(), sequence);
        }
        Ok(())
    }

    fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.persist(durability)
        } else {
            Ok(())
        }
    }

    fn wal_stats(&self) -> Option<WalFrontDoorStats> {
        self.wal.as_ref().map(WalFrontDoor::stats)
    }

    fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.rewrite_after_replay_floor(replay_floor)
        } else {
            Ok(())
        }
    }

    fn release_writer_lease(&self) {
        // Mirror `Db::close`: drop the lease, tolerating a poisoned mutex (the
        // lease is released on drop regardless).
        if let Ok(mut guard) = self.process_lock.lock() {
            guard.take();
        }
    }
}

/// Object-storage durability: **WAL-less** (a commit is durable once its
/// memtable is flushed to `SSTable` objects and the manifest CAS publishes
/// them), holding only a fencing-epoch writer lease.
pub(crate) struct ObjectStoreSubstrate {
    #[allow(dead_code)] // held for the fencing epoch; wired into manifest-publish fencing in 2c-4.
    lease: ObjectWriterLease,
}

impl std::fmt::Debug for ObjectStoreSubstrate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreSubstrate")
            .field("epoch", &self.lease.epoch)
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)] // wired into the object-store open path in 2c-4
impl ObjectStoreSubstrate {
    pub(crate) fn new(lease: ObjectWriterLease) -> Self {
        Self { lease }
    }

    /// The fencing epoch of the held lease (stamped into manifest publishes so a
    /// stale writer is fenced out).
    pub(crate) fn fencing_epoch(&self) -> u64 {
        self.lease.epoch
    }
}

/// A writer lease held against an object store, as a **fencing token**.
///
/// Object stores cannot provide a mutual-exclusion lock, so acquisition does not
/// "fail if held"; instead the lease object carries a monotonically increasing
/// epoch, and [`Self::acquire`] takes over by writing `epoch + 1` via a
/// compare-and-swap. A previous holder is fenced out when its lower epoch is
/// rejected at manifest-publish time (wired in a later slice), and in a real
/// deployment a TTL bounds how long a crashed holder's epoch stays "live".
pub(crate) struct ObjectWriterLease {
    // Read when lease release / fencing writes are wired (2c-4); the lease is
    // acquired against it here.
    #[allow(dead_code)]
    client: Arc<dyn ObjectClient>,
    key: String,
    epoch: u64,
}

impl std::fmt::Debug for ObjectWriterLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectWriterLease")
            .field("key", &self.key)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)] // wired into the object-store open path in 2c-4
impl ObjectWriterLease {
    /// Acquire the lease by bumping its fencing epoch via compare-and-swap. The
    /// returned lease carries the new epoch; concurrent acquirers retry until
    /// their CAS lands, so the epoch is strictly monotonic.
    pub(crate) async fn acquire(
        client: Arc<dyn ObjectClient>,
        key: impl Into<String>,
    ) -> Result<Self> {
        let key = key.into();
        loop {
            let (next_epoch, precondition) = match client.head(&key).await? {
                None => (1, Precondition::IfNoneMatch),
                Some(meta) => {
                    let bytes = client.get(&key).await?.ok_or_else(|| Error::Corruption {
                        message: format!("writer lease {key} vanished between head and get"),
                    })?;
                    (
                        decode_epoch(&key, &bytes)? + 1,
                        Precondition::IfMatch(meta.etag),
                    )
                }
            };
            match client
                .put_if(&key, encode_epoch(next_epoch), precondition)
                .await?
            {
                PutIf::Stored { .. } => {
                    return Ok(Self {
                        client,
                        key,
                        epoch: next_epoch,
                    });
                }
                // Lost the CAS to a concurrent acquirer; re-read and try again.
                PutIf::PreconditionFailed { .. } => {}
            }
        }
    }

    /// The fencing epoch this lease acquired.
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[allow(dead_code)] // used by ObjectWriterLease::acquire, wired in 2c-4
fn encode_epoch(epoch: u64) -> Arc<[u8]> {
    Arc::from(epoch.to_le_bytes().as_slice())
}

#[allow(dead_code)] // used by ObjectWriterLease::acquire, wired in 2c-4
fn decode_epoch(key: &str, bytes: &[u8]) -> Result<u64> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| Error::Corruption {
        message: format!("writer lease {key} has a malformed epoch"),
    })?;
    Ok(u64::from_le_bytes(array))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::storage::NativeFileBackend;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "trine-kv-substrate-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn put(key: &str, value: &str) -> BatchOperation {
        BatchOperation::Put {
            bucket: "default".to_owned(),
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    #[test]
    fn filesystem_substrate_drives_wal_and_lease() {
        let dir = temp_dir("wal-and-lease");
        fs::create_dir_all(&dir).expect("create substrate test dir");
        let backend = NativeFileBackend::new();

        let lease =
            ProcessLock::acquire_with_backend(&backend, &dir).expect("acquire writer lease");
        let wal =
            WalFrontDoor::open_sharded_with_backend(&backend, &dir, 1).expect("open sharded WAL");
        let substrate =
            DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(Some(wal), Some(lease)));

        // Drive it exactly as the commit / flush / close paths would.
        assert!(substrate.wal_is_present());
        substrate
            .accept_commit(Sequence::new(1), &[put("k", "v")], DurabilityMode::Flush)
            .expect("accept commit");
        substrate
            .persist_wal(DurabilityMode::Flush)
            .expect("persist WAL");
        let stats = substrate.wal_stats().expect("WAL present");
        assert_eq!(stats.records_accepted, 1);
        assert_eq!(stats.open_shards, 1);
        substrate
            .rewrite_wal_after_replay_floor(Sequence::new(1))
            .expect("rewrite WAL after replay floor");

        // Releasing the lease is idempotent.
        substrate.release_writer_lease();
        substrate.release_writer_lease();

        drop(substrate);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn filesystem_substrate_without_wal_is_inert() {
        let substrate = DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None));
        assert!(!substrate.wal_is_present());
        substrate
            .accept_commit(Sequence::new(1), &[put("k", "v")], DurabilityMode::Flush)
            .expect("no-op accept");
        substrate
            .persist_wal(DurabilityMode::Flush)
            .expect("no-op persist");
        assert!(substrate.wal_stats().is_none());
        substrate
            .rewrite_wal_after_replay_floor(Sequence::new(1))
            .expect("no-op rewrite");
        substrate.release_writer_lease();
    }

    fn poll_ready<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(result) => result,
            std::task::Poll::Pending => panic!("in-memory object future unexpectedly pending"),
        }
    }

    #[test]
    fn object_store_substrate_is_wal_less_and_inert() {
        use crate::object_store::InMemoryObjectStore;

        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let lease = poll_ready(ObjectWriterLease::acquire(client, "LOCK")).expect("acquire lease");
        let substrate = DurabilitySubstrate::ObjectStore(ObjectStoreSubstrate::new(lease));

        // WAL-less: every WAL operation is an inert success.
        assert!(!substrate.wal_is_present());
        substrate
            .accept_commit(Sequence::new(1), &[put("k", "v")], DurabilityMode::Flush)
            .expect("WAL-less accept is a no-op");
        substrate
            .persist_wal(DurabilityMode::Flush)
            .expect("WAL-less persist is a no-op");
        assert!(substrate.wal_stats().is_none());
        substrate
            .rewrite_wal_after_replay_floor(Sequence::new(1))
            .expect("WAL-less rewrite is a no-op");
        substrate.release_writer_lease(); // no-op; idempotent
    }

    #[test]
    fn object_writer_lease_bumps_fencing_epoch_on_takeover() {
        use crate::object_store::InMemoryObjectStore;

        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());

        // First acquire creates the lease object at epoch 1.
        let first = poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK"))
            .expect("first acquire");
        assert_eq!(first.epoch(), 1);

        // A later acquire takes over with a strictly higher epoch (fencing the
        // previous holder).
        let second = poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK"))
            .expect("second acquire");
        assert_eq!(second.epoch(), 2);
        let third = poll_ready(ObjectWriterLease::acquire(client, "LOCK")).expect("third acquire");
        assert_eq!(third.epoch(), 3);
    }
}
