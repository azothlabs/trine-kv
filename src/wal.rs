use std::{
    collections::BTreeMap,
    future::Future,
    ops::Bound,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};
#[cfg(not(target_os = "wasi"))]
use std::{
    pin::Pin,
    sync::{
        Condvar,
        mpsc::{self, SyncSender},
    },
    task::Wake,
    thread::{self, JoinHandle},
};

#[cfg(target_os = "wasi")]
use crate::storage::BlockingStorageAppendObject;
use crate::{
    error::{Error, Result},
    limits,
    options::DurabilityMode,
    storage::{
        BlockingStorageAppendBackend, BlockingStorageDirectoryListBackend,
        BlockingStorageObjectDeleteBackend, BlockingStorageObjectReadBackend,
        BlockingStorageObjectWriteBackend, BlockingStorageReadBackend, BlockingStorageReadObject,
        BlockingStorageWalRewriteBackend, NativeFileAppendObject, NativeFileBackend,
        StorageAppendBackend, StorageAppendObject, StorageCapability, StorageDirectoryFile,
        StorageDirectoryId, StorageDirectoryListBackend, StorageObjectDeleteBackend,
        StorageObjectId, StorageObjectKind, StorageObjectListBackend, StorageObjectListRequest,
        StorageObjectReadBackend, StorageReadBackend, StorageReadObject, StorageWalRewriteBackend,
    },
    types::{KeyRange, Sequence},
    write_batch::BatchOperation,
};

pub const WAL_MAGIC: u32 = 0x5452_574c;
pub const WAL_FORMAT_VERSION: u16 = 2;
pub const WAL_FILE_NAME: &str = "trine.wal";
pub const WAL_REWRITE_TMP_FILE_NAME: &str = "trine.wal.tmp";
pub const DEFAULT_WAL_SHARD_COUNT: usize = 4;

const HEADER_LEN: usize = 18;
const WAL_FRONT_DOOR_QUEUE_CAPACITY: usize = 64;
const WAL_SHARD_FILE_PREFIX: &str = "trine.wal.shard-";
const WAL_SHARD_FILE_DIGITS: usize = 4;
const WAL_CONFIRMED_FILE_PREFIX: &str = "trine.wal.confirmed-";
const OBJECT_WAL_FILE_PREFIX: &str = "trine.wal.epoch-";
const OBJECT_WAL_COMMIT_MARKER: &str = ".commit-";
const OBJECT_WAL_REWRITE_PREFIX: &str = "trine.wal.rewrite-epoch-";
const OBJECT_WAL_REWRITE_MARKER: &str = ".after-";
const OBJECT_WAL_FILE_SUFFIX: &str = ".trinewal";
const OBJECT_WAL_SEQUENCE_DIGITS: usize = 20;
const WAL_CONFIRMED_MAGIC: u32 = 0x5452_5743;
const WAL_CONFIRMED_VERSION: u16 = 1;
const WAL_CONFIRMED_LEN: usize = 18;

/// Whether an object-store `key` names a write-ahead-log object: a `trine.wal`
/// shard, WAL rewrite temp, confirmed-marker file, or remote object-store WAL
/// segment, as opposed to an `SSTable`, blob, manifest, or lease object.
///
/// A database opened over object storage writes **all** of these through one
/// [`ObjectClient`](crate::ObjectClient), and a commit is acknowledged only once
/// its WAL write is durable — so the WAL writes are the latency-critical,
/// durability-defining ones. A higher layer that supplies a shared client across
/// many databases (e.g. a multi-tenant service) uses this to recognize those
/// writes and route them to a low-latency durable tier, or coalesce them across
/// databases into one batched durable write (cross-tenant group commit), keeping
/// them distinct from bulk data writes. Matches the final path segment, so it is
/// independent of the database's key prefix.
#[must_use]
pub fn is_wal_object_key(key: &str) -> bool {
    let name = key.rsplit('/').next().unwrap_or(key);
    name.starts_with(WAL_FILE_NAME)
}
const OP_INSERT: u8 = 1;
const OP_REMOVE: u8 = 2;
const OP_REMOVE_RANGE: u8 = 3;
const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;
const MIN_WAL_OPERATION_BYTES: usize = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecordHeader {
    pub commit_sequence: Sequence,
    pub operation_count: u32,
    pub payload_len: u32,
    pub header_checksum: u32,
    pub payload_checksum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalBatch {
    pub sequence: Sequence,
    pub operations: Vec<BatchOperation>,
}

#[derive(Debug)]
pub struct WalWriter {
    append: NativeFileAppendObject,
}

#[derive(Debug)]
pub(crate) struct WalFrontDoor {
    active_shard_count: usize,
    queue_capacity: usize,
    lanes: Vec<WalFrontDoorLane>,
    records_accepted: AtomicU64,
    bytes_accepted: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalFrontDoorAccept {
    sequence: Sequence,
    shard_index: usize,
}

#[derive(Debug)]
struct WalFrontDoorLane {
    shard_index: usize,
    #[cfg(not(target_os = "wasi"))]
    sender: Option<SyncSender<WalLaneCommand>>,
    writer_open: Arc<AtomicBool>,
    #[cfg(not(target_os = "wasi"))]
    worker: Mutex<Option<JoinHandle<()>>>,
    #[cfg(target_os = "wasi")]
    backend: NativeFileBackend,
    #[cfg(target_os = "wasi")]
    path: PathBuf,
    #[cfg(target_os = "wasi")]
    state: Mutex<lane::WalLaneWorkerState>,
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug)]
pub(crate) struct BrowserWalFrontDoor {
    active_shard_count: usize,
    records_accepted: AtomicU64,
    bytes_accepted: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WalFrontDoorStats {
    pub(crate) shards: usize,
    pub(crate) open_shards: usize,
    pub(crate) queue_capacity: usize,
    pub(crate) records_accepted: u64,
    pub(crate) bytes_accepted: u64,
}

#[derive(Debug)]
enum WalLaneCommand {
    Append {
        sequence: Sequence,
        frame: Vec<u8>,
        durability: DurabilityMode,
        reply: WalLaneReply,
    },
    Persist {
        durability: DurabilityMode,
        reply: WalLaneReply,
    },
    Rewrite {
        replay_floor: Sequence,
        reply: WalLaneReply,
    },
}

#[derive(Debug)]
struct PendingWalAppend {
    sequence: Sequence,
    reply: WalLaneReply,
}

#[derive(Debug)]
struct WalLaneReply {
    completion: Arc<WalLaneCompletion>,
}

#[derive(Debug)]
struct WalLaneWaiter {
    completion: Arc<WalLaneCompletion>,
}

#[derive(Debug)]
struct WalLaneCompletion {
    result: Mutex<Option<Result<()>>>,
    #[cfg(not(target_os = "wasi"))]
    ready: Condvar,
    #[cfg(not(target_os = "wasi"))]
    waker: Mutex<Option<Waker>>,
}

#[cfg(not(target_os = "wasi"))]
struct WalStorageThreadWake {
    thread: thread::Thread,
}

impl WalLaneCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            #[cfg(not(target_os = "wasi"))]
            ready: Condvar::new(),
            #[cfg(not(target_os = "wasi"))]
            waker: Mutex::new(None),
        })
    }

    fn pair() -> (WalLaneReply, WalLaneWaiter) {
        let completion = Self::new();
        (
            WalLaneReply {
                completion: Arc::clone(&completion),
            },
            WalLaneWaiter { completion },
        )
    }

    fn complete(&self, result: Result<()>) {
        #[cfg(target_os = "wasi")]
        {
            let mut slot = match self.result.lock() {
                Ok(slot) => slot,
                Err(poisoned) => poisoned.into_inner(),
            };
            *slot = Some(result);
            return;
        }

        #[cfg(not(target_os = "wasi"))]
        {
            {
                let mut slot = match self.result.lock() {
                    Ok(slot) => slot,
                    Err(poisoned) => poisoned.into_inner(),
                };
                *slot = Some(result);
            }
            self.ready.notify_all();

            let waker = match self.waker.lock() {
                Ok(mut waker) => waker.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl WalLaneReply {
    fn complete(self, result: Result<()>) {
        self.completion.complete(result);
    }
}

impl WalLaneWaiter {
    fn wait(self) -> Result<()> {
        #[cfg(target_os = "wasi")]
        {
            return self.take_result()?.ok_or_else(|| {
                Error::runtime_busy("WASI WAL lane command did not finish synchronously")
            })?;
        }

        #[cfg(not(target_os = "wasi"))]
        {
            let mut result = self
                .completion
                .result
                .lock()
                .map_err(|_| wal_front_door_completion_poisoned())?;
            loop {
                if let Some(result) = result.take() {
                    return result;
                }
                result = self
                    .completion
                    .ready
                    .wait(result)
                    .map_err(|_| wal_front_door_completion_poisoned())?;
            }
        }
    }

    #[cfg(not(target_os = "wasi"))]
    fn register_waker(&self, context: &Context<'_>) -> Result<()> {
        let mut waker = self
            .completion
            .waker
            .lock()
            .map_err(|_| wal_front_door_completion_poisoned())?;
        let replace = match waker.as_ref() {
            Some(registered) => !registered.will_wake(context.waker()),
            None => true,
        };
        if replace {
            *waker = Some(context.waker().clone());
        }
        Ok(())
    }

    fn take_result(&self) -> Result<Option<Result<()>>> {
        self.completion
            .result
            .lock()
            .map(|mut result| result.take())
            .map_err(|_| wal_front_door_completion_poisoned())
    }
}

#[cfg(not(target_os = "wasi"))]
impl Future for WalLaneWaiter {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.take_result() {
            Ok(Some(result)) => return Poll::Ready(result),
            Ok(None) => {}
            Err(error) => return Poll::Ready(Err(error)),
        }

        if let Err(error) = self.register_waker(context) {
            return Poll::Ready(Err(error));
        }

        match self.take_result() {
            Ok(Some(result)) => Poll::Ready(result),
            Ok(None) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

#[cfg(not(target_os = "wasi"))]
impl Wake for WalStorageThreadWake {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

impl WalWriter {
    #[cfg(test)]
    pub(crate) fn open_append(path: &Path) -> Result<Self> {
        let backend = NativeFileBackend::new();
        Self::open_append_with_backend(&backend, path)
    }

    pub(crate) fn open_append_with_backend(
        backend: &NativeFileBackend,
        path: &Path,
    ) -> Result<Self> {
        Ok(Self {
            append: open_wal_append_object_with_backend(backend, path)?,
        })
    }

    #[cfg(test)]
    pub(crate) fn append_batch(
        &mut self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        let frame = encode_batch_frame(sequence, operations)?;
        self.append_frame(&frame, durability)
    }

    fn append_frame(&mut self, frame: &[u8], durability: DurabilityMode) -> Result<()> {
        #[cfg(target_os = "wasi")]
        {
            return self.append.append_blocking(frame, durability);
        }

        #[cfg(not(target_os = "wasi"))]
        wait_for_wal_storage_future(self.append.append(frame, durability))
    }

    fn persist(&mut self, durability: DurabilityMode) -> Result<()> {
        #[cfg(target_os = "wasi")]
        {
            return self.append.persist_blocking(durability);
        }

        #[cfg(not(target_os = "wasi"))]
        wait_for_wal_storage_future(self.append.persist(durability))
    }

    pub(crate) fn reopen_append_with_backend(
        &mut self,
        backend: &NativeFileBackend,
        path: &Path,
    ) -> Result<()> {
        self.append = open_wal_append_object_with_backend(backend, path)?;
        Ok(())
    }
}

fn wait_for_wal_storage_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    #[cfg(target_os = "wasi")]
    {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        return match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(Error::unsupported_backend(
                "runtime for pending WAL storage future",
            )),
        };
    }

    #[cfg(not(target_os = "wasi"))]
    {
        let waker = Waker::from(Arc::new(WalStorageThreadWake {
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
}

impl WalFrontDoor {
    #[cfg(test)]
    pub(crate) fn open_single_lane_with_backend(
        backend: &NativeFileBackend,
        path: &Path,
    ) -> Result<Self> {
        Self::from_shard_paths(backend, 1, [(0_usize, path.to_path_buf())])
    }

    #[allow(dead_code)]
    pub(crate) fn open_sharded_with_backend(
        backend: &NativeFileBackend,
        db_path: &Path,
        shard_count: usize,
    ) -> Result<Self> {
        let paths = discover_wal_paths_with_backend(backend, db_path)?;
        Self::open_sharded_with_discovered_paths(backend, db_path, shard_count, paths)
    }

    pub(crate) fn open_sharded_with_discovered_paths<I>(
        backend: &NativeFileBackend,
        db_path: &Path,
        shard_count: usize,
        discovered_paths: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = PathBuf>,
    {
        if shard_count == 0 {
            return Err(Error::invalid_options("WAL shard count must be non-zero"));
        }

        let mut paths = BTreeMap::new();
        for shard_index in 0..shard_count {
            paths.insert(shard_index, wal_shard_path(db_path, shard_index));
        }
        for path in discovered_paths {
            let shard_index = wal_shard_index_from_path(&path)?;
            paths.insert(shard_index, path);
        }

        Self::from_shard_paths(backend, shard_count, paths)
    }

    fn from_shard_paths<I>(
        backend: &NativeFileBackend,
        active_shard_count: usize,
        paths: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = (usize, PathBuf)>,
    {
        if active_shard_count == 0 {
            return Err(Error::invalid_options("WAL shard count must be non-zero"));
        }
        let mut lanes = Vec::new();
        for (shard_index, path) in paths {
            lanes.push(WalFrontDoorLane::spawn(
                backend,
                shard_index,
                &path,
                WAL_FRONT_DOOR_QUEUE_CAPACITY,
            )?);
        }
        if lanes.is_empty() {
            return Err(Error::invalid_options(
                "WAL front door needs at least one lane",
            ));
        }
        Ok(Self {
            active_shard_count,
            queue_capacity: WAL_FRONT_DOOR_QUEUE_CAPACITY,
            lanes,
            records_accepted: AtomicU64::new(0),
            bytes_accepted: AtomicU64::new(0),
        })
    }

    pub(crate) fn accept_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<WalFrontDoorAccept> {
        let shard_index = self.shard_index_for_sequence(sequence);
        let lane = self.lane(shard_index)?;
        let frame = encode_batch_frame(sequence, operations)?;
        let frame_len = usize_to_u64_saturating(frame.len());
        send_wal_lane_command(lane, |reply| WalLaneCommand::Append {
            sequence,
            frame,
            durability,
            reply,
        })?;
        self.records_accepted.fetch_add(1, Ordering::Relaxed);
        self.bytes_accepted.fetch_add(frame_len, Ordering::Relaxed);
        Ok(WalFrontDoorAccept {
            sequence,
            shard_index,
        })
    }

    #[cfg(not(target_os = "wasi"))]
    pub(crate) async fn accept_commit_async(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<WalFrontDoorAccept> {
        let shard_index = self.shard_index_for_sequence(sequence);
        let lane = self.lane(shard_index)?;
        let frame = encode_batch_frame(sequence, operations)?;
        let frame_len = usize_to_u64_saturating(frame.len());
        let waiter = enqueue_wal_lane_command(lane, |reply| WalLaneCommand::Append {
            sequence,
            frame,
            durability,
            reply,
        })?;
        waiter.await?;
        self.records_accepted.fetch_add(1, Ordering::Relaxed);
        self.bytes_accepted.fetch_add(frame_len, Ordering::Relaxed);
        Ok(WalFrontDoorAccept {
            sequence,
            shard_index,
        })
    }

    pub(crate) fn persist(&self, durability: DurabilityMode) -> Result<()> {
        for lane in &self.lanes {
            send_wal_lane_command(lane, |reply| WalLaneCommand::Persist { durability, reply })?;
        }
        Ok(())
    }

    #[cfg(not(target_os = "wasi"))]
    pub(crate) async fn persist_async(&self, durability: DurabilityMode) -> Result<()> {
        for lane in &self.lanes {
            enqueue_wal_lane_command(lane, |reply| WalLaneCommand::Persist { durability, reply })?
                .await?;
        }
        Ok(())
    }

    #[cfg(target_os = "wasi")]
    pub(crate) async fn persist_async(&self, durability: DurabilityMode) -> Result<()> {
        self.persist(durability)
    }

    pub(crate) fn stats(&self) -> WalFrontDoorStats {
        WalFrontDoorStats {
            shards: self.active_shard_count,
            open_shards: self.count_open_lanes(),
            queue_capacity: self.queue_capacity,
            records_accepted: self.records_accepted.load(Ordering::Acquire),
            bytes_accepted: self.bytes_accepted.load(Ordering::Acquire),
        }
    }

    pub(crate) fn rewrite_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        for lane in &self.lanes {
            send_wal_lane_command(lane, |reply| WalLaneCommand::Rewrite {
                replay_floor,
                reply,
            })?;
        }
        Ok(())
    }

    #[cfg(not(target_os = "wasi"))]
    pub(crate) async fn rewrite_after_replay_floor_async(
        &self,
        replay_floor: Sequence,
    ) -> Result<()> {
        for lane in &self.lanes {
            enqueue_wal_lane_command(lane, |reply| WalLaneCommand::Rewrite {
                replay_floor,
                reply,
            })?
            .await?;
        }
        Ok(())
    }

    #[cfg(target_os = "wasi")]
    pub(crate) async fn rewrite_after_replay_floor_async(
        &self,
        replay_floor: Sequence,
    ) -> Result<()> {
        self.rewrite_after_replay_floor(replay_floor)
    }

    fn count_open_lanes(&self) -> usize {
        self.lanes
            .iter()
            .filter(|lane| lane.writer_open.load(Ordering::Acquire))
            .count()
    }

    fn shard_index_for_sequence(&self, sequence: Sequence) -> usize {
        let offset = sequence.get().saturating_sub(1);
        usize::try_from(offset % usize_to_u64_saturating(self.active_shard_count))
            .expect("modulo result fits usize")
    }

    fn lane(&self, shard_index: usize) -> Result<&WalFrontDoorLane> {
        self.lanes
            .iter()
            .find(|lane| lane.shard_index == shard_index)
            .ok_or_else(|| Error::Corruption {
                message: format!("WAL front door lane {shard_index} is missing"),
            })
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl BrowserWalFrontDoor {
    pub(crate) async fn open_sharded_with_backend<B>(
        backend: &B,
        db_path: &Path,
        shard_count: usize,
    ) -> Result<Self>
    where
        B: StorageDirectoryListBackend,
    {
        if shard_count == 0 {
            return Err(Error::invalid_options("WAL shard count must be non-zero"));
        }
        discover_wal_paths_with_backend_async(backend, db_path).await?;
        Ok(Self {
            active_shard_count: shard_count,
            records_accepted: AtomicU64::new(0),
            bytes_accepted: AtomicU64::new(0),
        })
    }

    pub(crate) async fn accept_commit<B>(
        &self,
        backend: &B,
        db_path: &Path,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<WalFrontDoorAccept>
    where
        B: StorageAppendBackend,
    {
        let shard_index = self.shard_index_for_sequence(sequence);
        let path = wal_shard_path(db_path, shard_index);
        let frame = encode_batch_frame(sequence, operations)?;
        let frame_len = usize_to_u64_saturating(frame.len());
        let mut append = open_wal_append_object_with_backend_async(backend, &path).await?;
        append.append(&frame, durability).await?;
        self.records_accepted.fetch_add(1, Ordering::Relaxed);
        self.bytes_accepted.fetch_add(frame_len, Ordering::Relaxed);
        Ok(WalFrontDoorAccept {
            sequence,
            shard_index,
        })
    }

    pub(crate) async fn persist<B>(
        &self,
        backend: &B,
        db_path: &Path,
        durability: DurabilityMode,
    ) -> Result<()>
    where
        B: StorageAppendBackend,
    {
        for shard_index in 0..self.active_shard_count {
            let path = wal_shard_path(db_path, shard_index);
            let mut append = open_wal_append_object_with_backend_async(backend, &path).await?;
            append.persist(durability).await?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn rewrite_after_replay_floor<B>(
        &self,
        backend: &B,
        db_path: &Path,
        replay_floor: Sequence,
    ) -> Result<()>
    where
        B: StorageDirectoryListBackend + StorageObjectReadBackend + StorageWalRewriteBackend,
    {
        let mut paths = BTreeMap::new();
        for shard_index in 0..self.active_shard_count {
            paths.insert(shard_index, wal_shard_path(db_path, shard_index));
        }
        for path in discover_wal_paths_with_backend_async(backend, db_path).await? {
            paths.insert(wal_shard_index_from_path(&path)?, path);
        }
        for path in paths.into_values() {
            rewrite_batches_after_with_backend_async(backend, &path, replay_floor).await?;
        }
        Ok(())
    }

    pub(crate) fn stats(&self) -> WalFrontDoorStats {
        WalFrontDoorStats {
            shards: self.active_shard_count,
            open_shards: self.active_shard_count,
            queue_capacity: 0,
            records_accepted: self.records_accepted.load(Ordering::Acquire),
            bytes_accepted: self.bytes_accepted.load(Ordering::Acquire),
        }
    }

    fn shard_index_for_sequence(&self, sequence: Sequence) -> usize {
        let offset = sequence.get().saturating_sub(1);
        usize::try_from(offset % usize_to_u64_saturating(self.active_shard_count))
            .expect("modulo result fits usize")
    }
}

impl WalFrontDoorLane {
    fn spawn(
        backend: &NativeFileBackend,
        shard_index: usize,
        path: &Path,
        queue_capacity: usize,
    ) -> Result<Self> {
        #[cfg(target_os = "wasi")]
        {
            let _ = queue_capacity;
            return Ok(Self {
                shard_index,
                writer_open: Arc::new(AtomicBool::new(false)),
                backend: backend.clone(),
                path: path.to_path_buf(),
                state: Mutex::new(lane::WalLaneWorkerState::default()),
            });
        }

        #[cfg(not(target_os = "wasi"))]
        {
            let (sender, receiver) = mpsc::sync_channel(queue_capacity);
            let writer_open = Arc::new(AtomicBool::new(false));
            let worker_open = Arc::clone(&writer_open);
            let worker_backend = backend.clone();
            let worker_path = path.to_path_buf();
            let worker = thread::Builder::new()
                .name(format!("trine-wal-shard-{shard_index}"))
                .spawn(move || {
                    run_wal_lane_worker(worker_backend, worker_path, worker_open, receiver);
                })?;

            Ok(Self {
                shard_index,
                sender: Some(sender),
                writer_open,
                worker: Mutex::new(Some(worker)),
            })
        }
    }
}

impl Drop for WalFrontDoorLane {
    fn drop(&mut self) {
        #[cfg(not(target_os = "wasi"))]
        {
            drop(self.sender.take());
            if let Ok(mut worker) = self.worker.lock() {
                if let Some(handle) = worker.take() {
                    let _ = handle.join();
                }
            }
        }
    }
}

impl WalFrontDoorAccept {
    #[must_use]
    pub(crate) const fn sequence(self) -> Sequence {
        self.sequence
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn shard_index(self) -> usize {
        self.shard_index
    }
}

mod codec;
mod lane;
mod recovery;

pub(crate) use codec::*;
#[cfg(not(target_os = "wasi"))]
use lane::enqueue_wal_lane_command;
#[cfg(not(target_os = "wasi"))]
use lane::run_wal_lane_worker;
use lane::{
    send_wal_lane_command, validate_wal_stream_order, wal_front_door_completion_poisoned,
    wal_shard_index_from_file_name, wal_shard_index_from_final_file_name,
    wal_shard_index_from_path,
};
pub(crate) use recovery::*;

#[cfg(test)]
mod tests;
