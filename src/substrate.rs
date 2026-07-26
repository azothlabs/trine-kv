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
    collections::HashSet,
    future::Future,
    io,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::Duration,
};

use sha2::{Digest, Sha256};

#[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
use std::task::{Context, Poll, Wake, Waker};

use crate::error::{Error, Result};
use crate::object_store::{ETag, ObjectClient, Precondition, PutIf, canonical_object_key};
use crate::options::DurabilityMode;
use crate::recovery::ProcessLock;
use crate::types::Sequence;
use crate::wal::{self, WalFrontDoor, WalFrontDoorStats};
use crate::write_batch::BatchOperation;

const OBJECT_WAL_QUEUE_CAPACITY: usize = 64;
const OBJECT_WAL_GROUP_COMMIT_DELAY: Duration = Duration::from_millis(5);
const OBJECT_LEASE_TTL: Duration = Duration::from_secs(30);
const OBJECT_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);
const OBJECT_LEASE_MAGIC: u32 = 0x5452_4c53;
const OBJECT_LEASE_VERSION: u16 = 3;
const OBJECT_LEASE_V3_HEADER_LEN: usize = 50;
const OBJECT_LEASE_MAX_BYTES: u64 = 64 * 1024;
const OBJECT_WAL_SEGMENT_MAGIC: &[u8; 8] = b"TRNOWAL1";
const OBJECT_WAL_SEGMENT_HEADER_LEN: usize = 12;
const OBJECT_WAL_MAX_SEGMENT_BYTES: usize = 128 * 1024 * 1024;
const OBJECT_WAL_MAX_GROUP_FRAME_BYTES: usize = OBJECT_WAL_MAX_SEGMENT_BYTES - 64 * 1024;
const OBJECT_WAL_MAX_CHAIN_SEGMENTS: usize = 16_384;
const OBJECT_WAL_MAX_REPLAY_BYTES: usize = 1024 * 1024 * 1024;

pub(crate) fn validate_object_lease_wal_key_capacity(db_path: &std::path::Path) -> Result<()> {
    let longest_key = canonical_object_key(&wal::object_wal_commit_path(
        db_path,
        u64::MAX,
        Sequence::new(u64::MAX),
        &"f".repeat(64),
    ))?;
    let encoded_len = OBJECT_LEASE_V3_HEADER_LEN
        .checked_add(longest_key.len())
        .ok_or_else(|| Error::invalid_options("object writer lease length overflow"))?;
    if u64::try_from(encoded_len).map_or(true, |encoded_len| encoded_len > OBJECT_LEASE_MAX_BYTES) {
        return Err(Error::invalid_options(format!(
            "object-store prefix produces writer lease length {encoded_len}, exceeding maximum {OBJECT_LEASE_MAX_BYTES}"
        )));
    }
    Ok(())
}

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
            Self::ObjectStore(substrate) => substrate.wal_lane.enqueue_persist()?.wait().await,
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

    #[cfg(not(target_os = "wasi"))]
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
    wal_lane: ObjectWalLane,
    buffered: Mutex<Vec<(Sequence, Arc<[u8]>)>>,
    fencing_epoch: u64,
    records_accepted: std::sync::atomic::AtomicU64,
    bytes_accepted: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for ObjectStoreSubstrate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreSubstrate")
            .field("fencing_epoch", &self.fencing_epoch)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreSubstrate {
    pub(crate) fn new(lease: ObjectWriterLease, db_path: PathBuf) -> Result<Self> {
        let fencing_epoch = lease.state.epoch;
        Ok(Self {
            wal_lane: ObjectWalLane::spawn(lease, db_path)?,
            buffered: Mutex::new(Vec::new()),
            fencing_epoch,
            records_accepted: std::sync::atomic::AtomicU64::new(0),
            bytes_accepted: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// The fencing epoch of the held lease (stamped into manifest publishes so a
    /// stale writer is fenced out).
    pub(crate) fn fencing_epoch(&self) -> u64 {
        self.fencing_epoch
    }

    fn accept_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        let frame = wal::encode_batch_frame(sequence, operations)?;
        let bytes_accepted = frame.len() as u64;
        self.buffered
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL buffered commits"))?
            .push((sequence, frame.into()));
        self.records_accepted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_accepted
            .fetch_add(bytes_accepted, std::sync::atomic::Ordering::Relaxed);
        if durability == DurabilityMode::Buffered {
            Ok(())
        } else {
            self.flush_buffered()
        }
    }

    #[cfg(not(target_os = "wasi"))]
    fn enqueue_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<ObjectWalWaiter> {
        let commit_sequence = sequence;
        let frame = wal::encode_batch_frame(sequence, operations)?;
        let bytes_accepted = frame.len() as u64;
        let mut buffered = self
            .buffered
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL buffered commits"))?;
        buffered.push((sequence, frame.into()));
        if durability == DurabilityMode::Buffered {
            self.records_accepted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.bytes_accepted
                .fetch_add(bytes_accepted, std::sync::atomic::Ordering::Relaxed);
            return Ok(ObjectWalWaiter::ready());
        }

        buffered.sort_by_key(|(sequence, _)| *sequence);
        if buffered
            .last()
            .is_none_or(|(sequence, _)| *sequence != commit_sequence)
        {
            buffered.retain(|(sequence, _)| *sequence != commit_sequence);
            return Err(Error::Corruption {
                message:
                    "object WAL buffered sequence order would admit the current commit before an older commit"
                        .to_owned(),
            });
        }
        let mut completions = Vec::with_capacity(buffered.len());
        let mut enqueued = 0;
        for (sequence, frame) in buffered.iter() {
            match self.wal_lane.enqueue_commit(*sequence, Arc::clone(frame)) {
                Ok(completion) => {
                    completions.push(completion);
                    enqueued += 1;
                }
                Err(error) => {
                    buffered.drain(..enqueued);
                    if let Some(index) = buffered
                        .iter()
                        .position(|(buffered_sequence, _)| *buffered_sequence == commit_sequence)
                    {
                        buffered.remove(index);
                    }
                    return Err(error);
                }
            }
        }
        buffered.clear();
        self.records_accepted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_accepted
            .fetch_add(bytes_accepted, std::sync::atomic::Ordering::Relaxed);
        Ok(ObjectWalWaiter { completions })
    }

    fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        if durability == DurabilityMode::Buffered {
            return Ok(());
        }
        self.flush_buffered()?;
        self.wal_lane.persist()
    }

    fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        self.flush_buffered()?;
        self.wal_lane.rewrite_after_replay_floor(replay_floor)
    }

    fn flush_buffered(&self) -> Result<()> {
        let mut buffered = self
            .buffered
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL buffered commits"))?;
        buffered.sort_by_key(|(sequence, _)| *sequence);
        for (published, (sequence, frame)) in buffered.iter().enumerate() {
            if let Err(error) = self.wal_lane.accept_commit(*sequence, Arc::clone(frame)) {
                buffered.drain(..published);
                return Err(error);
            }
        }
        buffered.clear();
        Ok(())
    }

    fn wal_stats(&self) -> WalFrontDoorStats {
        WalFrontDoorStats {
            shards: 1,
            open_shards: 1,
            queue_capacity: OBJECT_WAL_QUEUE_CAPACITY,
            records_accepted: self
                .records_accepted
                .load(std::sync::atomic::Ordering::Acquire),
            bytes_accepted: self
                .bytes_accepted
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }

    fn release_writer_lease(&self) {
        let _ = self.wal_lane.release_writer_lease();
    }
}

struct ObjectWalLane {
    sender: Mutex<Option<mpsc::SyncSender<ObjectWalCommand>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for ObjectWalLane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectWalLane")
            .finish_non_exhaustive()
    }
}

impl ObjectWalLane {
    fn spawn(lease: ObjectWriterLease, db_path: PathBuf) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(OBJECT_WAL_QUEUE_CAPACITY);
        let future_driver = ObjectWalFutureDriver::new()?;
        let worker = thread::Builder::new()
            .name("trine-object-wal".to_owned())
            .spawn(move || run_object_wal_worker(lease, &db_path, &receiver, &future_driver))
            .map_err(Error::Io)?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
        })
    }

    fn accept_commit(&self, sequence: Sequence, frame: Arc<[u8]>) -> Result<()> {
        let completion = self.enqueue_commit(sequence, frame)?;
        completion.wait()
    }

    fn enqueue_commit(
        &self,
        sequence: Sequence,
        frame: Arc<[u8]>,
    ) -> Result<Arc<ObjectWalCompletion>> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.try_send(ObjectWalCommand::Accept(ObjectWalAccept {
            sequence,
            frame,
            completion: Arc::clone(&completion),
        }))?;
        Ok(completion)
    }

    fn persist(&self) -> Result<()> {
        let mut waiter = self.enqueue_persist()?;
        let completion = waiter.completions.pop().ok_or_else(|| Error::Corruption {
            message: "object WAL persist waiter has no completion".to_owned(),
        })?;
        completion.wait()
    }

    fn enqueue_persist(&self) -> Result<ObjectWalWaiter> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.try_send(ObjectWalCommand::Persist {
            completion: Arc::clone(&completion),
        })?;
        Ok(ObjectWalWaiter {
            completions: vec![completion],
        })
    }

    fn rewrite_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.send(ObjectWalCommand::Rewrite {
            replay_floor,
            completion: Arc::clone(&completion),
        })?;
        completion.wait()
    }

    fn release_writer_lease(&self) -> Result<()> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.send(ObjectWalCommand::Release {
            completion: Arc::clone(&completion),
        })?;
        completion.wait()
    }

    fn send(&self, command: ObjectWalCommand) -> Result<()> {
        let sender = self
            .sender
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL sender"))?;
        let Some(sender) = sender.as_ref() else {
            return Err(Error::Closed);
        };
        sender.send(command).map_err(|_| Error::Closed)
    }

    fn try_send(&self, command: ObjectWalCommand) -> Result<()> {
        let sender = self
            .sender
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL sender"))?;
        let Some(sender) = sender.as_ref() else {
            return Err(Error::Closed);
        };
        sender.try_send(command).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => Error::runtime_busy("object WAL queue is full"),
            mpsc::TrySendError::Disconnected(_) => Error::Closed,
        })
    }
}

impl Drop for ObjectWalLane {
    fn drop(&mut self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
        if let Ok(mut worker) = self.worker.lock()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

enum ObjectWalCommand {
    Accept(ObjectWalAccept),
    Persist {
        completion: Arc<ObjectWalCompletion>,
    },
    Rewrite {
        replay_floor: Sequence,
        completion: Arc<ObjectWalCompletion>,
    },
    Release {
        completion: Arc<ObjectWalCompletion>,
    },
}

struct ObjectWalAccept {
    sequence: Sequence,
    frame: Arc<[u8]>,
    completion: Arc<ObjectWalCompletion>,
}

struct ObjectWalCompletion {
    result: Mutex<Option<Result<()>>>,
    completed: std::sync::atomic::AtomicBool,
    ready: Condvar,
    waker: Mutex<Option<std::task::Waker>>,
}

impl ObjectWalCompletion {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            completed: std::sync::atomic::AtomicBool::new(false),
            ready: Condvar::new(),
            waker: Mutex::new(None),
        }
    }

    fn complete(&self, result: Result<()>) {
        if self
            .completed
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        if let Ok(mut slot) = self.result.lock() {
            *slot = Some(result);
            self.ready.notify_all();
        }
        if let Ok(mut waker) = self.waker.lock()
            && let Some(waker) = waker.take()
        {
            waker.wake();
        }
    }

    fn wait(&self) -> Result<()> {
        let mut slot = self
            .result
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL completion"))?;
        loop {
            if let Some(result) = slot.take() {
                return result;
            }
            slot = self
                .ready
                .wait(slot)
                .map_err(|_| lock_poisoned_error("object WAL completion"))?;
        }
    }

    fn poll_result(&self, context: &mut std::task::Context<'_>) -> std::task::Poll<Result<()>> {
        let Ok(mut slot) = self.result.lock() else {
            return std::task::Poll::Ready(Err(lock_poisoned_error("object WAL completion")));
        };
        if let Some(result) = slot.take() {
            return std::task::Poll::Ready(result);
        }
        drop(slot);
        match self.waker.lock() {
            Ok(mut waker) => *waker = Some(context.waker().clone()),
            Err(_) => {
                return std::task::Poll::Ready(Err(lock_poisoned_error(
                    "object WAL completion waker",
                )));
            }
        }
        let Ok(mut slot) = self.result.lock() else {
            return std::task::Poll::Ready(Err(lock_poisoned_error("object WAL completion")));
        };
        match slot.take() {
            Some(result) => std::task::Poll::Ready(result),
            None => std::task::Poll::Pending,
        }
    }
}

pub(crate) struct ObjectWalWaiter {
    completions: Vec<Arc<ObjectWalCompletion>>,
}

impl ObjectWalWaiter {
    #[cfg(not(target_os = "wasi"))]
    fn ready() -> Self {
        Self {
            completions: Vec::new(),
        }
    }

    pub(crate) async fn wait(self) -> Result<()> {
        for completion in self.completions {
            std::future::poll_fn(|context| completion.poll_result(context)).await?;
        }
        Ok(())
    }
}

enum ObjectWalFutureDriver {
    #[cfg(all(feature = "s3", not(target_family = "wasm")))]
    TokioHandle(tokio::runtime::Handle),
    #[cfg(all(feature = "s3", not(target_family = "wasm")))]
    OwnedTokio(tokio::runtime::Runtime),
    #[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
    Inline,
}

impl ObjectWalFutureDriver {
    #[allow(clippy::unnecessary_wraps)]
    fn new() -> Result<Self> {
        #[cfg(all(feature = "s3", not(target_family = "wasm")))]
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                return Ok(Self::TokioHandle(handle));
            }
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name("trine-object-wal-io")
                .build()
                .map_err(Error::Io)?;
            Ok(Self::OwnedTokio(runtime))
        }
        #[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
        {
            Ok(Self::Inline)
        }
    }

    fn block_on<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        match self {
            #[cfg(all(feature = "s3", not(target_family = "wasm")))]
            Self::TokioHandle(handle) => handle.block_on(future),
            #[cfg(all(feature = "s3", not(target_family = "wasm")))]
            Self::OwnedTokio(runtime) => runtime.block_on(future),
            #[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
            Self::Inline => block_on_substrate_future(future),
        }
    }
}

fn run_object_wal_worker(
    mut lease: ObjectWriterLease,
    db_path: &std::path::Path,
    receiver: &mpsc::Receiver<ObjectWalCommand>,
    future_driver: &ObjectWalFutureDriver,
) {
    let mut deferred = None;
    loop {
        let command = match deferred.take() {
            Some(command) => command,
            None => match receiver.recv_timeout(OBJECT_LEASE_RENEW_INTERVAL) {
                Ok(command) => command,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _ = future_driver.block_on(lease.renew());
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            },
        };
        if run_object_wal_command(
            &mut lease,
            db_path,
            receiver,
            future_driver,
            command,
            &mut deferred,
        ) {
            return;
        }
    }
}

fn run_object_wal_command(
    lease: &mut ObjectWriterLease,
    db_path: &std::path::Path,
    receiver: &mpsc::Receiver<ObjectWalCommand>,
    future_driver: &ObjectWalFutureDriver,
    command: ObjectWalCommand,
    deferred: &mut Option<ObjectWalCommand>,
) -> bool {
    match command {
        ObjectWalCommand::Accept(first) => {
            let accepts = collect_object_wal_accepts(first, receiver, deferred);
            let completions = accepts
                .iter()
                .map(|accept| Arc::clone(&accept.completion))
                .collect::<Vec<_>>();
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                complete_object_wal_accepts(lease, db_path, future_driver, accepts)
            })) {
                Ok(false) => {}
                Ok(true) => {
                    fail_object_wal_terminal(
                        receiver,
                        deferred.take(),
                        "object WAL entered a terminal failed state",
                    );
                    return true;
                }
                Err(_) => {
                    complete_object_wal_worker_panic(completions);
                    fail_object_wal_terminal(
                        receiver,
                        deferred.take(),
                        "object WAL worker panicked after durable mutation may have started",
                    );
                    return true;
                }
            }
        }
        ObjectWalCommand::Persist { completion } => {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                future_driver.block_on(lease.renew())
            }));
            let Ok(result) = result else {
                complete_object_wal_worker_panic(vec![completion]);
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL worker panicked while renewing its lease",
                );
                return true;
            };
            let failed = result.is_err();
            completion.complete(result);
            if failed {
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL lease renewal failed and ownership is no longer trusted",
                );
                return true;
            }
        }
        ObjectWalCommand::Rewrite {
            replay_floor,
            completion,
        } => {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rewrite_object_wal(lease, db_path, replay_floor, future_driver)
            }));
            let Ok(result) = result else {
                complete_object_wal_worker_panic(vec![completion]);
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL worker panicked while rewriting the WAL",
                );
                return true;
            };
            let failed = result.is_err();
            completion.complete(result);
            if failed {
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL rewrite failed after storage mutation may have started",
                );
                return true;
            }
        }
        ObjectWalCommand::Release { completion } => {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                future_driver.block_on(lease.release())
            }));
            let Ok(result) = result else {
                complete_object_wal_worker_panic(vec![completion]);
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL worker panicked while releasing its lease",
                );
                return true;
            };
            completion.complete(result);
            fail_object_wal_terminal(receiver, deferred.take(), "object WAL lane was released");
            return true;
        }
    }
    false
}

fn collect_object_wal_accepts(
    first: ObjectWalAccept,
    receiver: &mpsc::Receiver<ObjectWalCommand>,
    deferred: &mut Option<ObjectWalCommand>,
) -> Vec<ObjectWalAccept> {
    let mut accept_bytes = first.frame.len();
    let mut accepts = vec![first];
    while let Ok(command) = receiver.recv_timeout(OBJECT_WAL_GROUP_COMMIT_DELAY) {
        match command {
            ObjectWalCommand::Accept(accept)
                if accept_bytes
                    .checked_add(accept.frame.len())
                    .is_some_and(|bytes| bytes <= OBJECT_WAL_MAX_GROUP_FRAME_BYTES) =>
            {
                accept_bytes += accept.frame.len();
                accepts.push(accept);
            }
            other => {
                *deferred = Some(other);
                break;
            }
        }
        while let Ok(command) = receiver.try_recv() {
            match command {
                ObjectWalCommand::Accept(accept)
                    if accept_bytes
                        .checked_add(accept.frame.len())
                        .is_some_and(|bytes| bytes <= OBJECT_WAL_MAX_GROUP_FRAME_BYTES) =>
                {
                    accept_bytes += accept.frame.len();
                    accepts.push(accept);
                }
                other => {
                    *deferred = Some(other);
                    break;
                }
            }
        }
        if deferred.is_some() {
            break;
        }
    }
    accepts
}

fn rewrite_object_wal(
    lease: &mut ObjectWriterLease,
    db_path: &std::path::Path,
    replay_floor: Sequence,
    future_driver: &ObjectWalFutureDriver,
) -> Result<()> {
    future_driver.block_on(async {
        let deleted = lease
            .rewrite_segment_after_replay_floor(db_path, replay_floor)
            .await?;
        for key in deleted {
            lease.client.delete(&key).await?;
        }
        wal::delete_object_wal_at_or_below_with_backend_async(
            &crate::object_store::ObjectStoreBackend::new(Arc::clone(&lease.client)),
            db_path,
            replay_floor,
        )
        .await
    })
}

fn complete_object_wal_worker_panic(completions: Vec<Arc<ObjectWalCompletion>>) {
    for completion in completions {
        completion.complete(Err(Error::Corruption {
            message:
                "object WAL worker panicked after durable mutation may have started; reopen the database"
                    .to_owned(),
        }));
    }
}

fn fail_object_wal_terminal(
    receiver: &mpsc::Receiver<ObjectWalCommand>,
    deferred: Option<ObjectWalCommand>,
    message: &str,
) {
    if let Some(command) = deferred {
        complete_object_wal_command_terminal(command, message);
    }
    // Stay alive in a terminal failed state until every sender is gone. That
    // closes the race where a sender successfully enqueues after a one-shot
    // drain but before the receiver is dropped, which would otherwise strand
    // its waiter forever.
    while let Ok(command) = receiver.recv() {
        complete_object_wal_command_terminal(command, message);
    }
}

fn complete_object_wal_command_terminal(command: ObjectWalCommand, message: &str) {
    let completion = match command {
        ObjectWalCommand::Accept(accept) => accept.completion,
        ObjectWalCommand::Persist { completion }
        | ObjectWalCommand::Rewrite { completion, .. }
        | ObjectWalCommand::Release { completion } => completion,
    };
    completion.complete(Err(Error::Corruption {
        message: message.to_owned(),
    }));
}

fn complete_object_wal_accepts(
    lease: &mut ObjectWriterLease,
    db_path: &std::path::Path,
    future_driver: &ObjectWalFutureDriver,
    mut accepts: Vec<ObjectWalAccept>,
) -> bool {
    accepts.sort_by_key(|accept| accept.sequence);
    if let Err(error) = future_driver.block_on(lease.refresh_current()) {
        let message = error.to_string();
        let mut accepts = accepts.into_iter();
        if let Some(first) = accepts.next() {
            first.completion.complete(Err(error));
        }
        for accept in accepts {
            accept.completion.complete(Err(Error::runtime_busy(format!(
                "object WAL refresh failed before grouped commit: {message}"
            ))));
        }
        return true;
    }
    let mut previous = lease.state.committed_sequence;
    for accept in &accepts {
        if accept.sequence <= previous {
            let message = format!(
                "object WAL group commit received non-increasing sequence after {}: got {}",
                previous.get(),
                accept.sequence.get()
            );
            for accept in accepts {
                accept.completion.complete(Err(Error::Corruption {
                    message: message.clone(),
                }));
            }
            return true;
        }
        previous = accept.sequence;
    }
    let result = future_driver.block_on(lease.publish_commit_batch(db_path, &accepts));
    match result {
        Ok(()) => {
            for accept in accepts {
                accept.completion.complete(Ok(()));
            }
            false
        }
        Err(error) if accepts.len() == 1 => {
            if let Some(accept) = accepts.pop() {
                accept.completion.complete(Err(error));
            }
            true
        }
        Err(error) => {
            let message = format!("object WAL group commit failed: {error}");
            for accept in accepts {
                accept
                    .completion
                    .complete(Err(Error::Io(io::Error::other(message.clone()))));
            }
            true
        }
    }
}

/// A writer lease held against an object store.
///
/// The lease object carries both a monotonically increasing fencing epoch and a
/// wall-clock expiry. A second writer may acquire only after the observed
/// expiry has passed; while the owner is alive, the WAL worker extends the
/// expiry with CAS writes. A previous holder is fenced out when its lower epoch
/// is rejected before publishing a durable WAL commit or manifest edit.
fn object_wal_group_frame_bytes(
    committed_sequence: Sequence,
    accepts: &[ObjectWalAccept],
) -> Result<usize> {
    let empty_frame_bytes = wal::encode_batch_frame(Sequence::ZERO, &[])?.len();
    let mut expected_sequence =
        committed_sequence
            .get()
            .checked_add(1)
            .ok_or_else(|| Error::Corruption {
                message: "object WAL cannot advance past u64::MAX".to_owned(),
            })?;
    let mut total_bytes = 0usize;
    for (index, accept) in accepts.iter().enumerate() {
        let gap = accept
            .sequence
            .get()
            .checked_sub(expected_sequence)
            .ok_or_else(|| Error::Corruption {
                message: format!(
                    "object WAL expected sequence at least {expected_sequence}, got {}",
                    accept.sequence.get()
                ),
            })?;
        let gap = usize::try_from(gap).map_err(|_| Error::Corruption {
            message: "object WAL skipped sequence count exceeds usize".to_owned(),
        })?;
        let gap_bytes = gap
            .checked_mul(empty_frame_bytes)
            .ok_or_else(|| Error::Corruption {
                message: "object WAL skipped frame size overflow".to_owned(),
            })?;
        total_bytes = total_bytes
            .checked_add(gap_bytes)
            .and_then(|total| total.checked_add(accept.frame.len()))
            .ok_or_else(|| Error::Corruption {
                message: "object WAL group size overflow".to_owned(),
            })?;
        if total_bytes > OBJECT_WAL_MAX_GROUP_FRAME_BYTES {
            return Err(Error::Corruption {
                message: format!(
                    "object WAL group frame length {total_bytes} exceeds maximum {OBJECT_WAL_MAX_GROUP_FRAME_BYTES}"
                ),
            });
        }
        if index + 1 != accepts.len() {
            expected_sequence =
                accept
                    .sequence
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Corruption {
                        message: "object WAL group sequence overflow".to_owned(),
                    })?;
        }
    }
    Ok(total_bytes)
}

pub(crate) struct ObjectWriterLease {
    client: Arc<dyn ObjectClient>,
    key: String,
    etag: ETag,
    state: ObjectLeaseState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectLeaseState {
    pub(crate) epoch: u64,
    pub(crate) owner_id: [u8; 16],
    pub(crate) committed_sequence: Sequence,
    pub(crate) current_wal_key: Option<String>,
    pub(crate) lease_expires_at_ms: u64,
}

impl ObjectLeaseState {
    pub(crate) fn empty() -> Self {
        Self {
            epoch: 0,
            owner_id: [0; 16],
            committed_sequence: Sequence::ZERO,
            current_wal_key: None,
            lease_expires_at_ms: 0,
        }
    }

    fn is_expired_at(&self, now_ms: u64) -> bool {
        self.lease_expires_at_ms <= now_ms
    }
}

impl std::fmt::Debug for ObjectWriterLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectWriterLease")
            .field("key", &self.key)
            .field("epoch", &self.state.epoch)
            .field("committed_sequence", &self.state.committed_sequence)
            .field("lease_expires_at_ms", &self.state.lease_expires_at_ms)
            .finish_non_exhaustive()
    }
}

impl ObjectWriterLease {
    /// Acquire the lease by creating it or by taking over an expired owner. The
    /// returned lease carries a higher fencing epoch than the prior owner.
    pub(crate) async fn acquire(
        client: Arc<dyn ObjectClient>,
        key: impl Into<String>,
    ) -> Result<Self> {
        let key = key.into();
        loop {
            let now_ms = current_epoch_millis()?;
            let lease_expires_at_ms = object_lease_deadline_ms(now_ms);
            let mut owner_id = [0_u8; 16];
            getrandom::fill(&mut owner_id).map_err(|error| {
                Error::Io(io::Error::other(format!(
                    "object-store writer owner randomness failed: {error}"
                )))
            })?;
            let (next_state, precondition) = match read_lease_state(&client, &key).await? {
                None => (
                    ObjectLeaseState {
                        epoch: 1,
                        owner_id,
                        committed_sequence: Sequence::ZERO,
                        current_wal_key: None,
                        lease_expires_at_ms,
                    },
                    Precondition::IfNoneMatch,
                ),
                Some(meta) => {
                    if !meta.state.is_expired_at(now_ms) {
                        return Err(Error::runtime_busy(format!(
                            "object-store writer lease {key} is held until {}",
                            meta.state.lease_expires_at_ms
                        )));
                    }
                    let mut state = meta.state.clone();
                    state.epoch = state
                        .epoch
                        .checked_add(1)
                        .ok_or_else(|| Error::Corruption {
                            message: "object-store writer epoch overflow".to_owned(),
                        })?;
                    state.owner_id = owner_id;
                    state.lease_expires_at_ms = lease_expires_at_ms;
                    (state, Precondition::IfMatch(meta.etag))
                }
            };
            let publish = client
                .put_if(&key, encode_lease_state(next_state.clone())?, precondition)
                .await;
            match publish {
                Ok(PutIf::Stored { etag }) => {
                    return Ok(Self {
                        client,
                        key,
                        etag,
                        state: next_state,
                    });
                }
                // Lost the CAS to a concurrent acquirer; re-read and try again.
                Ok(PutIf::PreconditionFailed { .. }) => {}
                Err(error) => {
                    if let Ok(Some(current)) = read_lease_state(&client, &key).await
                        && current.state == next_state
                    {
                        return Ok(Self {
                            client,
                            key,
                            etag: current.etag,
                            state: current.state,
                        });
                    }
                    return Err(error);
                }
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

    async fn publish_commit_batch(
        &mut self,
        db_path: &std::path::Path,
        accepts: &[ObjectWalAccept],
    ) -> Result<()> {
        let Some(last) = accepts.last() else {
            return Ok(());
        };
        let total_bytes = object_wal_group_frame_bytes(self.state.committed_sequence, accepts)?;
        let mut frames = Vec::with_capacity(total_bytes);
        let mut expected = self
            .state
            .committed_sequence
            .get()
            .checked_add(1)
            .map(Sequence::new)
            .ok_or_else(|| Error::Corruption {
                message: "object WAL cannot advance past u64::MAX".to_owned(),
            })?;
        for accept in accepts {
            while expected < accept.sequence {
                frames.extend_from_slice(&wal::encode_batch_frame(expected, &[])?);
                expected = expected
                    .get()
                    .checked_add(1)
                    .map(Sequence::new)
                    .ok_or_else(|| Error::Corruption {
                        message: "object WAL skipped sequence overflow".to_owned(),
                    })?;
            }
            frames.extend_from_slice(&accept.frame);
            if expected != accept.sequence {
                return Err(Error::Corruption {
                    message: format!(
                        "object WAL expected sequence {}, got {}",
                        expected.get(),
                        accept.sequence.get()
                    ),
                });
            }
            if accept.sequence != last.sequence {
                expected = accept
                    .sequence
                    .get()
                    .checked_add(1)
                    .map(Sequence::new)
                    .ok_or_else(|| Error::Corruption {
                        message: "object WAL group sequence overflow".to_owned(),
                    })?;
            }
        }
        let segment = encode_object_wal_segment(self.state.current_wal_key.as_deref(), &frames)?;
        let identity = object_wal_segment_identity(&segment);
        let wal_key = canonical_object_key(&wal::object_wal_commit_path(
            db_path,
            self.state.epoch,
            last.sequence,
            &identity,
        ))?;
        put_immutable_object(&self.client, &wal_key, Arc::from(segment)).await?;
        self.publish_commit_head(last.sequence, wal_key).await
    }

    async fn refresh_current(&mut self) -> Result<()> {
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
        if current.state.owner_id != self.state.owner_id {
            return Err(Error::Fenced {
                held_epoch: self.state.epoch,
                current_epoch: current.state.epoch,
            });
        }
        self.etag = current.etag;
        self.state = current.state;
        Ok(())
    }

    async fn renew(&mut self) -> Result<()> {
        self.refresh_current().await?;
        let mut next = self.state.clone();
        next.lease_expires_at_ms = object_lease_deadline_ms(current_epoch_millis()?);
        let publish = self
            .client
            .put_if(
                &self.key,
                encode_lease_state(next.clone())?,
                Precondition::IfMatch(self.etag.clone()),
            )
            .await;
        match publish {
            Ok(PutIf::Stored { etag }) => {
                self.etag = etag;
                self.state = next;
                Ok(())
            }
            Ok(PutIf::PreconditionFailed { .. }) => {
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
                if current.state.owner_id != self.state.owner_id {
                    return Err(Error::Fenced {
                        held_epoch: self.state.epoch,
                        current_epoch: current.state.epoch,
                    });
                }
                self.etag = current.etag;
                self.state = current.state;
                Ok(())
            }
            Err(error) => {
                if let Ok(Some(current)) = read_lease_state(&self.client, &self.key).await
                    && current.state == next
                {
                    self.etag = current.etag;
                    self.state = current.state;
                    return Ok(());
                }
                Err(error)
            }
        }
    }

    async fn release(&mut self) -> Result<()> {
        loop {
            let Some(current) = read_lease_state(&self.client, &self.key).await? else {
                return Ok(());
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
            if current.state.owner_id != self.state.owner_id {
                return Err(Error::Fenced {
                    held_epoch: self.state.epoch,
                    current_epoch: current.state.epoch,
                });
            }
            self.etag = current.etag;
            self.state = current.state;
            let mut next = self.state.clone();
            next.lease_expires_at_ms = 0;
            let publish = self
                .client
                .put_if(
                    &self.key,
                    encode_lease_state(next.clone())?,
                    Precondition::IfMatch(self.etag.clone()),
                )
                .await;
            match publish {
                Ok(PutIf::Stored { etag }) => {
                    self.etag = etag;
                    self.state = next;
                    return Ok(());
                }
                Ok(PutIf::PreconditionFailed { .. }) => {}
                Err(error) => {
                    if let Ok(Some(current)) = read_lease_state(&self.client, &self.key).await
                        && current.state == next
                    {
                        self.etag = current.etag;
                        self.state = current.state;
                        return Ok(());
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn publish_commit_head(&mut self, sequence: Sequence, wal_key: String) -> Result<()> {
        loop {
            match self.state.committed_sequence.cmp(&sequence) {
                std::cmp::Ordering::Greater => {
                    return Err(Error::Corruption {
                        message: format!(
                            "object WAL head advanced to sequence {} while publishing older sequence {}",
                            self.state.committed_sequence.get(),
                            sequence.get()
                        ),
                    });
                }
                std::cmp::Ordering::Equal => {
                    if self.state.current_wal_key.as_deref() == Some(wal_key.as_str()) {
                        return Ok(());
                    }
                    return Err(Error::Corruption {
                        message: format!(
                            "object WAL sequence {} names conflicting immutable segment heads",
                            sequence.get()
                        ),
                    });
                }
                std::cmp::Ordering::Less => {}
            }
            let mut next = self.state.clone();
            next.committed_sequence = sequence;
            next.current_wal_key = Some(wal_key.clone());
            next.lease_expires_at_ms = object_lease_deadline_ms(current_epoch_millis()?);
            let publish = self
                .client
                .put_if(
                    &self.key,
                    encode_lease_state(next.clone())?,
                    Precondition::IfMatch(self.etag.clone()),
                )
                .await;
            match publish {
                Ok(PutIf::Stored { etag }) => {
                    self.etag = etag;
                    self.state = next;
                    return Ok(());
                }
                Ok(PutIf::PreconditionFailed { .. }) => {
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
                    if current.state.owner_id != self.state.owner_id {
                        return Err(Error::Fenced {
                            held_epoch: self.state.epoch,
                            current_epoch: current.state.epoch,
                        });
                    }
                    self.etag = current.etag;
                    self.state = current.state;
                }
                Err(error) => {
                    if let Ok(Some(current)) = read_lease_state(&self.client, &self.key).await
                        && current.state.epoch == next.epoch
                        && current.state.owner_id == next.owner_id
                        && current.state.committed_sequence >= sequence
                        && current.state.current_wal_key.as_deref() == Some(wal_key.as_str())
                    {
                        self.etag = current.etag;
                        self.state = current.state;
                        return Ok(());
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn rewrite_segment_after_replay_floor(
        &mut self,
        db_path: &std::path::Path,
        replay_floor: Sequence,
    ) -> Result<Vec<String>> {
        self.refresh_current().await?;
        loop {
            if self.state.current_wal_key.is_none() {
                return Ok(Vec::new());
            }
            let (batches, delete_keys) =
                read_object_wal_chain(&self.client, db_path, &self.state, replay_floor).await?;
            let mut next = self.state.clone();
            if batches.is_empty() {
                next.current_wal_key = None;
            } else {
                let rewritten = wal::encode_batches_after(&batches, replay_floor)?;
                let last_sequence = batches.last().map_or(replay_floor, |batch| batch.sequence);
                let segment = encode_object_wal_segment(None, &rewritten)?;
                let identity = object_wal_segment_identity(&segment);
                let next_key = canonical_object_key(&wal::object_wal_rewrite_path(
                    db_path,
                    self.state.epoch,
                    last_sequence,
                    &identity,
                ))?;
                put_immutable_object(&self.client, &next_key, Arc::from(segment)).await?;
                next.current_wal_key = Some(next_key);
            }
            next.lease_expires_at_ms = object_lease_deadline_ms(current_epoch_millis()?);
            let publish = self
                .client
                .put_if(
                    &self.key,
                    encode_lease_state(next.clone())?,
                    Precondition::IfMatch(self.etag.clone()),
                )
                .await;
            match publish {
                Ok(PutIf::Stored { etag }) => {
                    self.etag = etag;
                    self.state = next;
                    return Ok(delete_keys);
                }
                Ok(PutIf::PreconditionFailed { .. }) => {
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
                    if current.state.owner_id != self.state.owner_id {
                        return Err(Error::Fenced {
                            held_epoch: self.state.epoch,
                            current_epoch: current.state.epoch,
                        });
                    }
                    self.etag = current.etag;
                    self.state = current.state;
                }
                Err(error) => {
                    if let Ok(Some(current)) = read_lease_state(&self.client, &self.key).await
                        && current.state == next
                    {
                        self.etag = current.etag;
                        self.state = current.state;
                        return Ok(delete_keys);
                    }
                    return Err(error);
                }
            }
        }
    }
}

fn encode_object_wal_segment(previous_key: Option<&str>, frames: &[u8]) -> Result<Vec<u8>> {
    let previous_key = previous_key.unwrap_or_default();
    let key_len = u32::try_from(previous_key.len())
        .map_err(|_| Error::invalid_options("object WAL predecessor key exceeds u32::MAX"))?;
    let capacity = OBJECT_WAL_SEGMENT_HEADER_LEN
        .checked_add(previous_key.len())
        .and_then(|size| size.checked_add(frames.len()))
        .ok_or_else(|| Error::invalid_options("object WAL segment size overflow"))?;
    let mut segment = Vec::with_capacity(capacity);
    segment.extend_from_slice(OBJECT_WAL_SEGMENT_MAGIC);
    segment.extend_from_slice(&key_len.to_le_bytes());
    segment.extend_from_slice(previous_key.as_bytes());
    segment.extend_from_slice(frames);
    crate::limits::ensure_corruption_len(
        segment.len(),
        OBJECT_WAL_MAX_SEGMENT_BYTES,
        "object WAL segment length",
    )?;
    Ok(segment)
}

fn object_wal_segment_identity(segment: &[u8]) -> String {
    let digest = Sha256::digest(segment);
    let mut identity = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut identity, "{byte:02x}").expect("writing to String cannot fail");
    }
    identity
}

fn decode_object_wal_segment<'bytes>(
    key: &str,
    bytes: &'bytes [u8],
) -> Result<(Option<String>, &'bytes [u8])> {
    if bytes.get(..OBJECT_WAL_SEGMENT_MAGIC.len()) != Some(OBJECT_WAL_SEGMENT_MAGIC) {
        return Err(Error::Corruption {
            message: format!("object WAL segment {key} has an invalid format marker"),
        });
    }
    let key_len_bytes: [u8; 4] = bytes
        .get(8..12)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} has a truncated chain header"),
        })?
        .try_into()
        .expect("checked object WAL predecessor length bytes");
    let key_len =
        usize::try_from(u32::from_le_bytes(key_len_bytes)).map_err(|_| Error::Corruption {
            message: format!("object WAL segment {key} predecessor length overflow"),
        })?;
    let payload_offset = OBJECT_WAL_SEGMENT_HEADER_LEN
        .checked_add(key_len)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} predecessor offset overflow"),
        })?;
    let predecessor = bytes
        .get(OBJECT_WAL_SEGMENT_HEADER_LEN..payload_offset)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} has a truncated predecessor key"),
        })?;
    let frames = bytes
        .get(payload_offset..)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} has a truncated frame payload"),
        })?;
    let predecessor = if predecessor.is_empty() {
        None
    } else {
        Some(
            std::str::from_utf8(predecessor)
                .map_err(|_| Error::Corruption {
                    message: format!("object WAL segment {key} predecessor is not UTF-8"),
                })?
                .to_owned(),
        )
    };
    Ok((predecessor, frames))
}

async fn put_immutable_object(
    client: &Arc<dyn ObjectClient>,
    key: &str,
    bytes: Arc<[u8]>,
) -> Result<()> {
    let publish = client
        .put_if(key, Arc::clone(&bytes), Precondition::IfNoneMatch)
        .await;
    match publish {
        Ok(PutIf::Stored { .. }) => Ok(()),
        Ok(PutIf::PreconditionFailed { .. }) => {
            let existing = client.get(key).await?;
            if existing.as_deref() == Some(bytes.as_ref()) {
                Ok(())
            } else {
                Err(Error::Corruption {
                    message: format!(
                        "immutable object WAL segment {key} already has different bytes"
                    ),
                })
            }
        }
        Err(error) => {
            if let Ok(Some(existing)) = client.get(key).await
                && existing.as_ref() == bytes.as_ref()
            {
                return Ok(());
            }
            Err(error)
        }
    }
}

async fn read_object_wal_chain(
    client: &Arc<dyn ObjectClient>,
    db_path: &std::path::Path,
    state: &ObjectLeaseState,
    replay_floor: Sequence,
) -> Result<(Vec<wal::WalBatch>, Vec<String>)> {
    if state.committed_sequence <= replay_floor {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut current = state
        .current_wal_key
        .clone()
        .ok_or_else(|| Error::Corruption {
            message: format!(
                "object WAL head reached sequence {} without a segment",
                state.committed_sequence.get()
            ),
        })?;
    let mut visited = HashSet::new();
    let mut keys = Vec::new();
    let mut batches = Vec::new();
    let mut replay_bytes = 0usize;
    loop {
        if !visited.insert(current.clone()) {
            return Err(Error::Corruption {
                message: format!("object WAL chain contains a cycle at {current}"),
            });
        }
        if visited.len() > OBJECT_WAL_MAX_CHAIN_SEGMENTS {
            return Err(Error::Corruption {
                message: format!(
                    "object WAL chain exceeds {OBJECT_WAL_MAX_CHAIN_SEGMENTS} segments"
                ),
            });
        }
        let segment =
            read_verified_object_wal_segment(client, db_path, &current, replay_floor).await?;
        replay_bytes =
            replay_bytes
                .checked_add(segment.byte_len)
                .ok_or_else(|| Error::Corruption {
                    message: "object WAL replay byte count overflow".to_owned(),
                })?;
        crate::limits::ensure_corruption_len(
            replay_bytes,
            OBJECT_WAL_MAX_REPLAY_BYTES,
            "object WAL replay bytes",
        )?;
        batches.extend(
            segment
                .batches
                .into_iter()
                .filter(|batch| batch.sequence <= state.committed_sequence),
        );
        keys.push(current);
        let Some(previous) = segment.previous else {
            break;
        };
        current = previous;
    }
    batches.sort_unstable_by_key(|batch| batch.sequence);
    validate_object_wal_sequences(&batches, replay_floor, state.committed_sequence)?;
    Ok((batches, keys))
}

struct VerifiedObjectWalSegment {
    previous: Option<String>,
    batches: Vec<wal::WalBatch>,
    byte_len: usize,
}

async fn read_verified_object_wal_segment(
    client: &Arc<dyn ObjectClient>,
    db_path: &std::path::Path,
    key: &str,
    replay_floor: Sequence,
) -> Result<VerifiedObjectWalSegment> {
    validate_object_wal_key(db_path, key)?;
    let bytes = client.get(key).await?.ok_or_else(|| Error::Corruption {
        message: format!("object WAL segment {key} is missing"),
    })?;
    crate::limits::ensure_corruption_len(
        bytes.len(),
        OBJECT_WAL_MAX_SEGMENT_BYTES,
        "object WAL segment length",
    )?;
    let identity = key
        .strip_suffix(".trinewal")
        .and_then(|stem| stem.rsplit_once('-'))
        .map(|(_, identity)| identity)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} has no content identity"),
        })?;
    if identity != object_wal_segment_identity(&bytes) {
        return Err(Error::Corruption {
            message: format!("object WAL segment {key} content identity mismatch"),
        });
    }
    let byte_len = bytes.len();
    let (previous, frames) = decode_object_wal_segment(key, &bytes)?;
    let batches = wal::decode_frames_after(frames, replay_floor)?;
    Ok(VerifiedObjectWalSegment {
        previous,
        batches,
        byte_len,
    })
}

fn validate_object_wal_sequences(
    batches: &[wal::WalBatch],
    replay_floor: Sequence,
    committed_sequence: Sequence,
) -> Result<()> {
    let mut previous = replay_floor;
    for batch in batches {
        let expected = previous
            .get()
            .checked_add(1)
            .map(Sequence::new)
            .ok_or_else(|| Error::Corruption {
                message: "object WAL sequence overflow while validating its chain".to_owned(),
            })?;
        if batch.sequence != expected {
            return Err(Error::Corruption {
                message: format!(
                    "object WAL chain expected sequence {}, got {}",
                    expected.get(),
                    batch.sequence.get()
                ),
            });
        }
        previous = batch.sequence;
    }
    if previous != committed_sequence {
        return Err(Error::Corruption {
            message: format!(
                "object WAL chain ended at sequence {}, below committed head {}",
                previous.get(),
                committed_sequence.get()
            ),
        });
    }
    Ok(())
}

fn validate_object_wal_key(db_path: &std::path::Path, key: &str) -> Result<()> {
    let root = canonical_object_key(db_path)?;
    let canonical = crate::object_store::canonical_object_prefix(key)?;
    let expected_parent = if root.is_empty() {
        None
    } else {
        Some(root.as_str())
    };
    let path = std::path::Path::new(&canonical);
    let parent = path
        .parent()
        .and_then(std::path::Path::to_str)
        .filter(|parent| !parent.is_empty());
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default();
    if canonical != key || parent != expected_parent || !crate::is_wal_object_key(name) {
        return Err(Error::Corruption {
            message: format!("object WAL chain key {key:?} is outside database root {root:?}"),
        });
    }
    Ok(())
}

pub(crate) async fn object_store_wal_batches_after_replay_floor(
    client: Arc<dyn ObjectClient>,
    db_path: &std::path::Path,
    state: &ObjectLeaseState,
    replay_floor: Sequence,
) -> Result<Vec<wal::WalBatch>> {
    read_object_wal_chain(&client, db_path, state, replay_floor)
        .await
        .map(|(batches, _)| batches)
}

struct ObservedLeaseState {
    etag: ETag,
    state: ObjectLeaseState,
}

mod lease_state;
#[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
use lease_state::block_on_substrate_future;
use lease_state::{
    current_epoch_millis, encode_lease_state, lock_poisoned_error, object_lease_deadline_ms,
    read_lease_state,
};
#[cfg(test)]
mod tests;
