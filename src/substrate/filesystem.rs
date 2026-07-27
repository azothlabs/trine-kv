use std::sync::Mutex;

use crate::{
    error::Result,
    options::DurabilityMode,
    recovery::ProcessLock,
    types::Sequence,
    wal::{WalFrontDoor, WalFrontDoorStats},
    write_batch::BatchOperation,
};

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

    pub(super) fn wal_is_present(&self) -> bool {
        self.wal.is_some()
    }

    pub(super) fn accept_commit(
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

    #[cfg(not(target_os = "wasi"))]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(super) async fn accept_commit_async(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        if let Some(wal) = &self.wal {
            let accepted = wal
                .accept_commit_async(sequence, operations, durability)
                .await?;
            debug_assert_eq!(accepted.sequence(), sequence);
        }
        Ok(())
    }

    pub(super) fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.persist(durability)
        } else {
            Ok(())
        }
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(super) async fn persist_wal_async(&self, durability: DurabilityMode) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.persist_async(durability).await
        } else {
            Ok(())
        }
    }

    pub(super) fn wal_stats(&self) -> Option<WalFrontDoorStats> {
        self.wal.as_ref().map(WalFrontDoor::stats)
    }

    pub(super) fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.rewrite_after_replay_floor(replay_floor)
        } else {
            Ok(())
        }
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(super) async fn rewrite_wal_after_replay_floor_async(
        &self,
        replay_floor: Sequence,
    ) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.rewrite_after_replay_floor_async(replay_floor).await
        } else {
            Ok(())
        }
    }

    pub(super) fn release_writer_lease(&self) {
        // Mirror `Db::close`: drop the lease, tolerating a poisoned mutex (the
        // lease is released on drop regardless).
        if let Ok(mut guard) = self.process_lock.lock() {
            guard.take();
        }
    }
}
