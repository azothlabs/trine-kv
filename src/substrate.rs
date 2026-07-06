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
    io,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(not(feature = "s3"))]
use std::task::{Context, Poll, Wake, Waker};

use crate::error::{Error, Result};
use crate::object_store::{ETag, ObjectClient, Precondition, PutIf};
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
const OBJECT_LEASE_VERSION: u16 = 2;
const OBJECT_LEASE_V2_HEADER_LEN: usize = 34;
const OBJECT_LEASE_MAX_BYTES: u64 = 64 * 1024;

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
                substrate.accept_commit(sequence, operations, durability)
            }
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
        if durability == DurabilityMode::Buffered {
            return Ok(());
        }
        let frame = wal::encode_batch_frame(sequence, operations)?;
        let bytes_accepted = frame.len() as u64;
        self.wal_lane.accept_commit(sequence, frame.into())?;
        self.records_accepted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_accepted
            .fetch_add(bytes_accepted, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        if durability == DurabilityMode::Buffered {
            return Ok(());
        }
        self.wal_lane.persist()
    }

    fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        self.wal_lane.rewrite_after_replay_floor(replay_floor)
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
            .spawn(move || run_object_wal_worker(lease, db_path, receiver, future_driver))
            .map_err(Error::Io)?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
        })
    }

    fn accept_commit(&self, sequence: Sequence, frame: Arc<[u8]>) -> Result<()> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.send(ObjectWalCommand::Accept(ObjectWalAccept {
            sequence,
            frame,
            completion: Arc::clone(&completion),
        }))?;
        completion.wait()
    }

    fn persist(&self) -> Result<()> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.send(ObjectWalCommand::Persist {
            completion: Arc::clone(&completion),
        })?;
        completion.wait()
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
}

impl Drop for ObjectWalLane {
    fn drop(&mut self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(worker) = worker.take() {
                let _ = worker.join();
            }
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
    ready: Condvar,
}

impl ObjectWalCompletion {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn complete(&self, result: Result<()>) {
        if let Ok(mut slot) = self.result.lock() {
            *slot = Some(result);
            self.ready.notify_all();
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
}

enum ObjectWalFutureDriver {
    #[cfg(feature = "s3")]
    TokioHandle(tokio::runtime::Handle),
    #[cfg(feature = "s3")]
    OwnedTokio(tokio::runtime::Runtime),
    #[cfg(not(feature = "s3"))]
    Inline,
}

impl ObjectWalFutureDriver {
    #[allow(clippy::unnecessary_wraps)]
    fn new() -> Result<Self> {
        #[cfg(feature = "s3")]
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
        #[cfg(not(feature = "s3"))]
        {
            Ok(Self::Inline)
        }
    }

    fn block_on<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        match self {
            #[cfg(feature = "s3")]
            Self::TokioHandle(handle) => handle.block_on(future),
            #[cfg(feature = "s3")]
            Self::OwnedTokio(runtime) => runtime.block_on(future),
            #[cfg(not(feature = "s3"))]
            Self::Inline => block_on_substrate_future(future),
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn run_object_wal_worker(
    mut lease: ObjectWriterLease,
    db_path: PathBuf,
    receiver: mpsc::Receiver<ObjectWalCommand>,
    future_driver: ObjectWalFutureDriver,
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
        match command {
            ObjectWalCommand::Accept(first) => {
                let mut accepts = vec![first];
                while let Ok(command) = receiver.recv_timeout(OBJECT_WAL_GROUP_COMMIT_DELAY) {
                    match command {
                        ObjectWalCommand::Accept(accept) => accepts.push(accept),
                        other => {
                            deferred = Some(other);
                            break;
                        }
                    }
                    while let Ok(command) = receiver.try_recv() {
                        match command {
                            ObjectWalCommand::Accept(accept) => accepts.push(accept),
                            other => {
                                deferred = Some(other);
                                break;
                            }
                        }
                    }
                    if deferred.is_some() {
                        break;
                    }
                }
                complete_object_wal_accepts(&mut lease, &db_path, &future_driver, accepts);
            }
            ObjectWalCommand::Persist { completion } => {
                let result = future_driver.block_on(lease.renew());
                completion.complete(result);
            }
            ObjectWalCommand::Rewrite {
                replay_floor,
                completion,
            } => {
                let result = future_driver.block_on(async {
                    let deleted = lease
                        .rewrite_segment_after_replay_floor(&db_path, replay_floor)
                        .await?;
                    for key in deleted {
                        lease.client.delete(&key).await?;
                    }
                    wal::delete_object_wal_at_or_below_with_backend_async(
                        &crate::object_store::ObjectStoreBackend::new(Arc::clone(&lease.client)),
                        &db_path,
                        replay_floor,
                    )
                    .await
                });
                completion.complete(result);
            }
            ObjectWalCommand::Release { completion } => {
                let result = future_driver.block_on(lease.release());
                completion.complete(result);
                return;
            }
        }
    }
}

fn complete_object_wal_accepts(
    lease: &mut ObjectWriterLease,
    db_path: &std::path::Path,
    future_driver: &ObjectWalFutureDriver,
    mut accepts: Vec<ObjectWalAccept>,
) {
    accepts.sort_by_key(|accept| accept.sequence);
    let Some(mut expected) = lease
        .state
        .committed_sequence
        .get()
        .checked_add(1)
        .map(Sequence::new)
    else {
        let message = "object WAL group commit cannot advance past u64::MAX".to_owned();
        for accept in accepts {
            accept.completion.complete(Err(Error::Corruption {
                message: message.clone(),
            }));
        }
        return;
    };
    for accept in &accepts {
        if accept.sequence != expected {
            let message = format!(
                "object WAL group commit received non-contiguous sequence: expected {}, got {}",
                expected.get(),
                accept.sequence.get()
            );
            for accept in accepts {
                accept.completion.complete(Err(Error::Corruption {
                    message: message.clone(),
                }));
            }
            return;
        }
        let Some(next) = expected.get().checked_add(1).map(Sequence::new) else {
            let message = "object WAL group commit cannot advance past u64::MAX".to_owned();
            for accept in accepts {
                accept.completion.complete(Err(Error::Corruption {
                    message: message.clone(),
                }));
            }
            return;
        };
        expected = next;
    }
    let result = future_driver.block_on(lease.publish_commit_batch(db_path, &accepts));
    match result {
        Ok(()) => {
            for accept in accepts {
                accept.completion.complete(Ok(()));
            }
        }
        Err(error) if accepts.len() == 1 => {
            if let Some(accept) = accepts.pop() {
                accept.completion.complete(Err(error));
            }
        }
        Err(error) => {
            let message = format!("object WAL group commit failed: {error}");
            for accept in accepts {
                accept
                    .completion
                    .complete(Err(Error::Io(io::Error::other(message.clone()))));
            }
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
pub(crate) struct ObjectWriterLease {
    client: Arc<dyn ObjectClient>,
    key: String,
    etag: ETag,
    state: ObjectLeaseState,
    cached_wal_segment: Option<CachedWalSegment>,
}

#[derive(Debug, Clone)]
struct CachedWalSegment {
    key: String,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectLeaseState {
    pub(crate) epoch: u64,
    pub(crate) committed_sequence: Sequence,
    pub(crate) current_wal_key: Option<String>,
    pub(crate) lease_expires_at_ms: u64,
}

impl ObjectLeaseState {
    pub(crate) fn empty() -> Self {
        Self {
            epoch: 0,
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
            let (next_state, precondition) = match read_lease_state(&client, &key).await? {
                None => (
                    ObjectLeaseState {
                        epoch: 1,
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
                    state.lease_expires_at_ms = lease_expires_at_ms;
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
                        cached_wal_segment: None,
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

    async fn publish_commit_batch(
        &mut self,
        db_path: &std::path::Path,
        accepts: &[ObjectWalAccept],
    ) -> Result<()> {
        let Some(last) = accepts.last() else {
            return Ok(());
        };
        let wal_key = wal::object_wal_commit_path(db_path, self.state.epoch, Sequence::ZERO)
            .to_string_lossy()
            .into_owned();
        let mut segment = self.load_cached_wal_segment().await?;
        for accept in accepts {
            segment.extend_from_slice(&accept.frame);
        }
        self.client
            .put(&wal_key, Arc::from(segment.as_slice()))
            .await?;
        self.publish_commit_head(last.sequence, wal_key, segment)
            .await
    }

    async fn load_cached_wal_segment(&mut self) -> Result<Vec<u8>> {
        let Some(key) = self.state.current_wal_key.clone() else {
            return Ok(Vec::new());
        };
        if let Some(cached) = &self.cached_wal_segment {
            if cached.key == key {
                return Ok(cached.bytes.clone());
            }
        }
        let bytes = self
            .client
            .get(&key)
            .await?
            .ok_or_else(|| Error::Corruption {
                message: format!("object WAL segment {key} is missing"),
            })?;
        let bytes = bytes.to_vec();
        self.cached_wal_segment = Some(CachedWalSegment {
            key,
            bytes: bytes.clone(),
        });
        Ok(bytes)
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

    async fn renew(&mut self) -> Result<()> {
        self.ensure_current().await?;
        let mut next = self.state.clone();
        next.lease_expires_at_ms = object_lease_deadline_ms(current_epoch_millis()?);
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
                Ok(())
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
                self.invalidate_wal_cache_if_stale();
                Ok(())
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
            self.etag = current.etag;
            self.state = current.state;
            let mut next = self.state.clone();
            next.lease_expires_at_ms = 0;
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
                PutIf::PreconditionFailed { .. } => {}
            }
        }
    }

    async fn publish_commit_head(
        &mut self,
        sequence: Sequence,
        wal_key: String,
        segment: Vec<u8>,
    ) -> Result<()> {
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
            next.lease_expires_at_ms = object_lease_deadline_ms(current_epoch_millis()?);
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
                    self.cached_wal_segment = Some(CachedWalSegment {
                        key: wal_key,
                        bytes: segment.clone(),
                    });
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
                    self.invalidate_wal_cache_if_stale();
                }
            }
        }
    }

    fn invalidate_wal_cache_if_stale(&mut self) {
        if self.cached_wal_segment.as_ref().is_some_and(|cached| {
            Some(cached.key.as_str()) != self.state.current_wal_key.as_deref()
        }) {
            self.cached_wal_segment = None;
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
            let batches = wal::decode_frames_after(bytes.as_ref(), replay_floor)?
                .into_iter()
                .filter(|batch| batch.sequence <= self.state.committed_sequence)
                .collect::<Vec<_>>();
            let mut next = self.state.clone();
            let mut next_cache = None;
            let delete_keys = vec![current_key.clone()];
            if batches.is_empty() {
                next.current_wal_key = None;
                self.cached_wal_segment = None;
            } else {
                let rewritten = wal::encode_batches_after(&batches, replay_floor)?;
                let last_sequence = batches.last().map_or(replay_floor, |batch| batch.sequence);
                let next_key =
                    wal::object_wal_rewrite_path(db_path, self.state.epoch, last_sequence)
                        .to_string_lossy()
                        .into_owned();
                self.client
                    .put(&next_key, Arc::from(rewritten.as_slice()))
                    .await?;
                next_cache = Some(CachedWalSegment {
                    key: next_key.clone(),
                    bytes: rewritten,
                });
                next.current_wal_key = Some(next_key);
            }
            next.lease_expires_at_ms = object_lease_deadline_ms(current_epoch_millis()?);
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
                    self.cached_wal_segment = next_cache;
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
                    self.invalidate_wal_cache_if_stale();
                }
            }
        }
    }
}

struct ObservedLeaseState {
    etag: ETag,
    state: ObjectLeaseState,
}

mod lease_state;
#[cfg(not(feature = "s3"))]
use lease_state::block_on_substrate_future;
use lease_state::{
    current_epoch_millis, encode_lease_state, lock_poisoned_error, object_lease_deadline_ms,
    read_lease_state,
};
#[cfg(test)]
mod tests;
