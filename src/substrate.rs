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

use std::{
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread,
};

use crate::error::{Error, Result};
use crate::object_store::{ETag, ObjectClient, Precondition, PutIf};
use crate::options::DurabilityMode;
use crate::recovery::ProcessLock;
use crate::types::Sequence;
use crate::wal::{self, WalFrontDoor, WalFrontDoorStats};
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
                block_on_substrate_future(substrate.accept_commit(sequence, operations, durability))
            }
        }
    }

    /// Append a commit's operations to the WAL and await the WAL lane
    /// completion when the substrate has one.
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
                    .accept_commit(sequence, operations, durability)
                    .await
            }
        }
    }

    /// Flush WAL durability to the requested level (no-op when there is no WAL).
    pub(crate) fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => substrate.persist_wal(durability),
            Self::ObjectStore(substrate) => {
                block_on_substrate_future(substrate.persist_wal(durability))
            }
        }
    }

    /// Flush WAL durability to the requested level and await the WAL lane
    /// completion when the substrate has one.
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(crate) async fn persist_wal_async(&self, durability: DurabilityMode) -> Result<()> {
        match self {
            Self::Filesystem(substrate) => substrate.persist_wal_async(durability).await,
            Self::ObjectStore(substrate) => substrate.persist_wal(durability).await,
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
            Self::ObjectStore(substrate) => {
                block_on_substrate_future(substrate.rewrite_wal_after_replay_floor(replay_floor))
            }
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
            Self::ObjectStore(substrate) => {
                substrate.rewrite_wal_after_replay_floor(replay_floor).await
            }
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

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    async fn accept_commit_async(
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

    fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.persist(durability)
        } else {
            Ok(())
        }
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    async fn persist_wal_async(&self, durability: DurabilityMode) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.persist_async(durability).await
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

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    async fn rewrite_wal_after_replay_floor_async(&self, replay_floor: Sequence) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.rewrite_after_replay_floor_async(replay_floor).await
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

/// Object-storage durability: immutable WAL segments plus a lease object that
/// doubles as a fencing token and the published remote WAL head.
pub(crate) struct ObjectStoreSubstrate {
    lease: futures::lock::Mutex<ObjectWriterLease>,
    db_path: PathBuf,
    records_accepted: std::sync::atomic::AtomicU64,
    bytes_accepted: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for ObjectStoreSubstrate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreSubstrate")
            .field("db_path", &self.db_path)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreSubstrate {
    pub(crate) fn new(lease: ObjectWriterLease, db_path: PathBuf) -> Self {
        Self {
            lease: futures::lock::Mutex::new(lease),
            db_path,
            records_accepted: std::sync::atomic::AtomicU64::new(0),
            bytes_accepted: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The fencing epoch of the held lease (stamped into manifest publishes so a
    /// stale writer is fenced out).
    pub(crate) fn fencing_epoch(&self) -> u64 {
        self.lease.try_lock().map_or(0, |lease| lease.state.epoch)
    }

    async fn accept_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        if durability == DurabilityMode::Buffered {
            return Ok(());
        }
        let frame = wal::encode_batch_frame(sequence, operations)?;
        let bytes_accepted = frame.len() as u64;
        let mut lease = self.lease.lock().await;
        lease
            .publish_commit_object(&self.db_path, sequence, frame.into())
            .await?;
        self.records_accepted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_accepted
            .fetch_add(bytes_accepted, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        if durability == DurabilityMode::Buffered {
            return Ok(());
        }
        let lease = self.lease.lock().await;
        lease.ensure_current().await
    }

    async fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        let mut lease = self.lease.lock().await;
        let deleted = lease
            .rewrite_segment_after_replay_floor(&self.db_path, replay_floor)
            .await?;
        for key in deleted {
            lease.client.delete(&key).await?;
        }
        // Also sweep old unindexed WAL objects left by failed commits. These are
        // never part of recovery because recovery reads the lease WAL head.
        wal::delete_object_wal_at_or_below_with_backend_async(
            &crate::object_store::ObjectStoreBackend::new(Arc::clone(&lease.client)),
            &self.db_path,
            replay_floor,
        )
        .await
    }

    fn wal_stats(&self) -> WalFrontDoorStats {
        WalFrontDoorStats {
            shards: 1,
            open_shards: 1,
            queue_capacity: 0,
            records_accepted: self
                .records_accepted
                .load(std::sync::atomic::Ordering::Acquire),
            bytes_accepted: self
                .bytes_accepted
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }
}

/// A writer lease held against an object store, as a **fencing token**.
///
/// Object stores cannot provide a mutual-exclusion lock, so acquisition does not
/// "fail if held"; instead the lease object carries a monotonically increasing
/// epoch, and [`Self::acquire`] takes over by writing `epoch + 1` via a
/// compare-and-swap. A previous holder is fenced out when its lower epoch is
/// rejected before publishing a durable WAL commit or manifest edit, and in a
/// real deployment a TTL bounds how long a crashed holder's epoch stays "live".
pub(crate) struct ObjectWriterLease {
    client: Arc<dyn ObjectClient>,
    key: String,
    etag: ETag,
    state: ObjectLeaseState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectLeaseState {
    pub(crate) epoch: u64,
    pub(crate) committed_sequence: Sequence,
    pub(crate) current_wal_key: Option<String>,
}

impl ObjectLeaseState {
    pub(crate) fn empty() -> Self {
        Self {
            epoch: 0,
            committed_sequence: Sequence::ZERO,
            current_wal_key: None,
        }
    }
}

impl std::fmt::Debug for ObjectWriterLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectWriterLease")
            .field("key", &self.key)
            .field("epoch", &self.state.epoch)
            .field("committed_sequence", &self.state.committed_sequence)
            .finish_non_exhaustive()
    }
}

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
            let (next_state, precondition) = match read_lease_state(&client, &key).await? {
                None => (
                    ObjectLeaseState {
                        epoch: 1,
                        committed_sequence: Sequence::ZERO,
                        current_wal_key: None,
                    },
                    Precondition::IfNoneMatch,
                ),
                Some(meta) => {
                    let mut state = meta.state.clone();
                    state.epoch = state
                        .epoch
                        .checked_add(1)
                        .ok_or_else(|| Error::Corruption {
                            message: "object-store writer epoch overflow".to_owned(),
                        })?;
                    (state, Precondition::IfMatch(meta.etag))
                }
            };
            match client
                .put_if(&key, encode_lease_state(next_state.clone())?, precondition)
                .await?
            {
                PutIf::Stored { etag } => {
                    return Ok(Self {
                        client,
                        key,
                        etag,
                        state: next_state,
                    });
                }
                // Lost the CAS to a concurrent acquirer; re-read and try again.
                PutIf::PreconditionFailed { .. } => {}
            }
        }
    }

    /// The fencing epoch this lease acquired.
    #[cfg(test)]
    pub(crate) fn epoch(&self) -> u64 {
        self.state.epoch
    }

    #[cfg(test)]
    pub(crate) fn committed_sequence(&self) -> Sequence {
        self.state.committed_sequence
    }

    pub(crate) fn lease_state(&self) -> ObjectLeaseState {
        self.state.clone()
    }

    pub(crate) async fn read_current(
        client: Arc<dyn ObjectClient>,
        key: impl Into<String>,
    ) -> Result<Option<ObjectLeaseState>> {
        read_lease_state(&client, &key.into())
            .await
            .map(|state| state.map(|state| state.state))
    }

    async fn publish_commit_object(
        &mut self,
        db_path: &std::path::Path,
        sequence: Sequence,
        frame: Arc<[u8]>,
    ) -> Result<()> {
        self.ensure_current().await?;
        let wal_key = wal::object_wal_commit_path(db_path, self.state.epoch, sequence)
            .to_string_lossy()
            .into_owned();
        let mut segment = match &self.state.current_wal_key {
            Some(key) => self
                .client
                .get(key)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!("object WAL segment {key} is missing"),
                })?,
            None => Arc::from([]),
        }
        .to_vec();
        segment.extend_from_slice(&frame);
        self.client.put(&wal_key, segment.into()).await?;
        self.publish_commit_head(sequence, wal_key).await
    }

    async fn ensure_current(&self) -> Result<()> {
        let Some(current) = read_lease_state(&self.client, &self.key).await? else {
            return Err(Error::Fenced {
                held_epoch: self.state.epoch,
                current_epoch: 0,
            });
        };
        if current.state.epoch > self.state.epoch {
            return Err(Error::Fenced {
                held_epoch: self.state.epoch,
                current_epoch: current.state.epoch,
            });
        }
        if current.state.epoch < self.state.epoch {
            return Err(Error::Corruption {
                message: format!(
                    "writer lease {} moved backward from epoch {} to {}",
                    self.key, self.state.epoch, current.state.epoch
                ),
            });
        }
        Ok(())
    }

    async fn publish_commit_head(&mut self, sequence: Sequence, wal_key: String) -> Result<()> {
        loop {
            if self.state.committed_sequence >= sequence
                && self.state.current_wal_key.as_deref() == Some(wal_key.as_str())
            {
                return Ok(());
            }
            let mut next = self.state.clone();
            if next.committed_sequence < sequence {
                next.committed_sequence = sequence;
            }
            next.current_wal_key = Some(wal_key.clone());
            match self
                .client
                .put_if(
                    &self.key,
                    encode_lease_state(next.clone())?,
                    Precondition::IfMatch(self.etag.clone()),
                )
                .await?
            {
                PutIf::Stored { etag } => {
                    self.etag = etag;
                    self.state = next;
                    return Ok(());
                }
                PutIf::PreconditionFailed { .. } => {
                    let Some(current) = read_lease_state(&self.client, &self.key).await? else {
                        return Err(Error::Fenced {
                            held_epoch: self.state.epoch,
                            current_epoch: 0,
                        });
                    };
                    if current.state.epoch > self.state.epoch {
                        return Err(Error::Fenced {
                            held_epoch: self.state.epoch,
                            current_epoch: current.state.epoch,
                        });
                    }
                    if current.state.epoch < self.state.epoch {
                        return Err(Error::Corruption {
                            message: format!(
                                "writer lease {} moved backward from epoch {} to {}",
                                self.key, self.state.epoch, current.state.epoch
                            ),
                        });
                    }
                    self.etag = current.etag;
                    self.state = current.state;
                }
            }
        }
    }

    async fn rewrite_segment_after_replay_floor(
        &mut self,
        db_path: &std::path::Path,
        replay_floor: Sequence,
    ) -> Result<Vec<String>> {
        self.ensure_current().await?;
        loop {
            let Some(current_key) = self.state.current_wal_key.clone() else {
                return Ok(Vec::new());
            };
            let bytes = self
                .client
                .get(&current_key)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!("object WAL segment {current_key} is missing"),
                })?;
            let batches = wal::decode_frames_after(bytes.as_ref(), replay_floor)?;
            let mut next = self.state.clone();
            let delete_keys = vec![current_key.clone()];
            if batches.is_empty() {
                next.current_wal_key = None;
            } else {
                let rewritten = wal::encode_batches_after(&batches, replay_floor)?;
                let last_sequence = batches.last().map_or(replay_floor, |batch| batch.sequence);
                let next_key =
                    wal::object_wal_rewrite_path(db_path, self.state.epoch, last_sequence)
                        .to_string_lossy()
                        .into_owned();
                self.client.put(&next_key, rewritten.into()).await?;
                next.current_wal_key = Some(next_key);
            }
            match self
                .client
                .put_if(
                    &self.key,
                    encode_lease_state(next.clone())?,
                    Precondition::IfMatch(self.etag.clone()),
                )
                .await?
            {
                PutIf::Stored { etag } => {
                    self.etag = etag;
                    self.state = next;
                    return Ok(delete_keys);
                }
                PutIf::PreconditionFailed { .. } => {
                    let Some(current) = read_lease_state(&self.client, &self.key).await? else {
                        return Err(Error::Fenced {
                            held_epoch: self.state.epoch,
                            current_epoch: 0,
                        });
                    };
                    if current.state.epoch > self.state.epoch {
                        return Err(Error::Fenced {
                            held_epoch: self.state.epoch,
                            current_epoch: current.state.epoch,
                        });
                    }
                    if current.state.epoch < self.state.epoch {
                        return Err(Error::Corruption {
                            message: format!(
                                "writer lease {} moved backward from epoch {} to {}",
                                self.key, self.state.epoch, current.state.epoch
                            ),
                        });
                    }
                    self.etag = current.etag;
                    self.state = current.state;
                }
            }
        }
    }
}

struct ObservedLeaseState {
    etag: ETag,
    state: ObjectLeaseState,
}

async fn read_lease_state(
    client: &Arc<dyn ObjectClient>,
    key: &str,
) -> Result<Option<ObservedLeaseState>> {
    let Some(meta) = client.head(key).await? else {
        return Ok(None);
    };
    let bytes = client.get(key).await?.ok_or_else(|| Error::Corruption {
        message: format!("writer lease {key} vanished between head and get"),
    })?;
    Ok(Some(ObservedLeaseState {
        etag: meta.etag,
        state: decode_lease_state(key, &bytes)?,
    }))
}

fn encode_lease_state(state: ObjectLeaseState) -> Result<Arc<[u8]>> {
    let key_len = state.current_wal_key.as_ref().map_or(0, String::len);
    let key_len = u32::try_from(key_len)
        .map_err(|_| Error::invalid_options("object WAL segment key exceeds u32::MAX"))?;
    let mut bytes = Vec::with_capacity(20 + key_len as usize);
    bytes.extend_from_slice(&state.epoch.to_le_bytes());
    bytes.extend_from_slice(&state.committed_sequence.get().to_le_bytes());
    bytes.extend_from_slice(&key_len.to_le_bytes());
    if let Some(key) = state.current_wal_key {
        bytes.extend_from_slice(key.as_bytes());
    }
    Ok(Arc::from(bytes))
}

fn decode_lease_state(key: &str, bytes: &[u8]) -> Result<ObjectLeaseState> {
    if bytes.len() == 8 {
        let epoch = decode_u64(key, bytes, "epoch")?;
        return Ok(ObjectLeaseState {
            epoch,
            committed_sequence: Sequence::ZERO,
            current_wal_key: None,
        });
    }
    if bytes.len() == 16 {
        let epoch = decode_u64(key, &bytes[..8], "epoch")?;
        let committed_sequence = Sequence::new(decode_u64(key, &bytes[8..], "commit head")?);
        return Ok(ObjectLeaseState {
            epoch,
            committed_sequence,
            current_wal_key: None,
        });
    }
    if bytes.len() < 20 {
        return Err(Error::Corruption {
            message: format!("writer lease {key} has a malformed state"),
        });
    }
    let epoch = decode_u64(key, &bytes[..8], "epoch")?;
    let committed_sequence = Sequence::new(decode_u64(key, &bytes[8..16], "commit head")?);
    let key_len = u32::from_le_bytes(bytes[16..20].try_into().map_err(|_| Error::Corruption {
        message: format!("writer lease {key} has a malformed WAL segment key length"),
    })?);
    let key_len = usize::try_from(key_len).map_err(|_| Error::Corruption {
        message: format!("writer lease {key} WAL segment key length overflow"),
    })?;
    let key_end = 20_usize
        .checked_add(key_len)
        .ok_or_else(|| Error::Corruption {
            message: format!("writer lease {key} WAL segment key offset overflow"),
        })?;
    if key_end != bytes.len() {
        return Err(Error::Corruption {
            message: format!("writer lease {key} has malformed WAL segment key bytes"),
        });
    }
    let current_wal_key = if key_len == 0 {
        None
    } else {
        let key_bytes = bytes.get(20..key_end).ok_or_else(|| Error::Corruption {
            message: format!("writer lease {key} has a truncated WAL segment key"),
        })?;
        Some(
            std::str::from_utf8(key_bytes)
                .map_err(|_| Error::Corruption {
                    message: format!("writer lease {key} WAL segment key is not valid UTF-8"),
                })?
                .to_owned(),
        )
    };
    Ok(ObjectLeaseState {
        epoch,
        committed_sequence,
        current_wal_key,
    })
}

fn decode_u64(key: &str, bytes: &[u8], field: &str) -> Result<u64> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| Error::Corruption {
        message: format!("writer lease {key} has a malformed {field}"),
    })?;
    Ok(u64::from_le_bytes(array))
}

struct SubstrateThreadWake {
    thread: thread::Thread,
}

impl Wake for SubstrateThreadWake {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

fn block_on_substrate_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let waker = Waker::from(Arc::new(SubstrateThreadWake {
        thread: thread::current(),
    }));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => thread::park(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
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
    fn object_store_substrate_publishes_remote_wal_head() {
        use crate::object_store::InMemoryObjectStore;

        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let lease = poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK"))
            .expect("acquire lease");
        let substrate =
            DurabilitySubstrate::ObjectStore(ObjectStoreSubstrate::new(lease, PathBuf::from("db")));

        assert!(substrate.wal_is_present());
        substrate
            .accept_commit(Sequence::new(1), &[put("k", "v")], DurabilityMode::Flush)
            .expect("accept commit");
        substrate
            .persist_wal(DurabilityMode::Flush)
            .expect("persist WAL");
        assert_eq!(
            substrate
                .wal_stats()
                .expect("object WAL stats")
                .records_accepted,
            1
        );
        substrate
            .rewrite_wal_after_replay_floor(Sequence::new(1))
            .expect("rewrite WAL after replay floor");
        let head = poll_ready(ObjectWriterLease::read_current(client, "LOCK"))
            .expect("read lease")
            .expect("lease exists");
        assert_eq!(head.committed_sequence, Sequence::new(1));
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
        assert_eq!(first.committed_sequence(), Sequence::ZERO);

        // A later acquire takes over with a strictly higher epoch (fencing the
        // previous holder).
        let second = poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK"))
            .expect("second acquire");
        assert_eq!(second.epoch(), 2);
        let third = poll_ready(ObjectWriterLease::acquire(client, "LOCK")).expect("third acquire");
        assert_eq!(third.epoch(), 3);
    }
}
