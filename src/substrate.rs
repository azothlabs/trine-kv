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
//! `DbInner` today holds `wal: Option<WalFrontDoor>` and
//! `process_lock: Mutex<Option<ProcessLock>>` (plus parallel `browser_*` fields)
//! directly, and the commit / flush / close paths call them concretely. Slice
//! 2b ③ replaces those fields with a `DurabilitySubstrate`, has the open path
//! construct the right variant, and routes commit / flush / close through it.
//! The object-storage substrate (slice 2c) then becomes a third variant
//! alongside the filesystem one defined here.
//!
//! Until that reroute lands, this module is unconsumed; the smoke test exercises
//! the filesystem variant against a real WAL + lease so the delegation is
//! validated against the actual `WalFrontDoor` / `ProcessLock` code, not just
//! the types.

use std::sync::Mutex;

use crate::error::Result;
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
}

impl DurabilitySubstrate {
    /// Whether a write-ahead log is present. A read-only open has none.
    pub(crate) fn wal_is_present(&self) -> bool {
        match self {
            Self::Filesystem(substrate) => substrate.wal_is_present(),
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
        }
    }

    /// Flush WAL durability to the requested level (no-op when there is no WAL).
    pub(crate) fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => substrate.persist_wal(durability),
        }
    }

    /// WAL statistics, or `None` when there is no WAL.
    pub(crate) fn wal_stats(&self) -> Option<WalFrontDoorStats> {
        match self {
            Self::Filesystem(substrate) => substrate.wal_stats(),
        }
    }

    /// Truncate the WAL below a checkpoint after a memtable flush advances the
    /// replay floor (no-op when there is no WAL).
    pub(crate) fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => substrate.rewrite_wal_after_replay_floor(replay_floor),
        }
    }

    /// Release the single-writer lease (idempotent; called on close).
    pub(crate) fn release_writer_lease(&self) {
        match self {
            Self::Filesystem(substrate) => substrate.release_writer_lease(),
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
}
