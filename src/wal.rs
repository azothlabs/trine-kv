use std::{
    collections::BTreeMap,
    future::Future,
    ops::Bound,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender},
    },
    task::{Context, Poll, Wake, Waker},
    thread::{self, JoinHandle},
};

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
    storage::{
        BlockingStorageAppendBackend, BlockingStorageDirectoryListBackend,
        BlockingStorageObjectReadBackend, BlockingStorageReadBackend, BlockingStorageReadObject,
        BlockingStorageWalRewriteBackend, NativeFileAppendObject, NativeFileBackend,
        StorageAppendBackend, StorageAppendObject, StorageCapability, StorageDirectoryFile,
        StorageDirectoryId, StorageDirectoryListBackend, StorageObjectId, StorageObjectKind,
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
    sender: Option<SyncSender<WalLaneCommand>>,
    writer_open: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
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
    ready: Condvar,
    waker: Mutex<Option<Waker>>,
}

struct WalStorageThreadWake {
    thread: thread::Thread,
}

impl WalLaneCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
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

impl WalLaneReply {
    fn complete(self, result: Result<()>) {
        self.completion.complete(result);
    }
}

impl WalLaneWaiter {
    fn wait(self) -> Result<()> {
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
        wait_for_wal_storage_future(self.append.append(frame, durability))
    }

    fn persist(&mut self, durability: DurabilityMode) -> Result<()> {
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

impl Drop for WalFrontDoorLane {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(handle) = worker.take() {
                let _ = handle.join();
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

#[must_use]
pub fn wal_path(db_path: &Path) -> PathBuf {
    db_path.join(WAL_FILE_NAME)
}

#[must_use]
pub fn wal_shard_path(db_path: &Path, shard_index: usize) -> PathBuf {
    if shard_index == 0 {
        return wal_path(db_path);
    }
    let width = WAL_SHARD_FILE_DIGITS;
    db_path.join(format!("{WAL_SHARD_FILE_PREFIX}{shard_index:0width$}"))
}

#[cfg(test)]
pub(crate) fn read_all_batches(db_path: &Path) -> Result<Vec<WalBatch>> {
    read_all_batches_after(db_path, Sequence::ZERO)
}

#[cfg(test)]
fn read_all_batches_after(db_path: &Path, replay_floor: Sequence) -> Result<Vec<WalBatch>> {
    let backend = NativeFileBackend::new();
    let paths = discover_wal_paths_with_backend(&backend, db_path)?;
    let streams = read_recovery_streams_after_paths_with_backend(&backend, &paths, replay_floor)?;
    merge_batch_streams_by_sequence(streams)
}

pub(crate) fn read_batches_after_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
    replay_floor: Sequence,
) -> Result<Vec<WalBatch>> {
    let Some(bytes) = read_wal_object_with_backend(backend, path)? else {
        return Ok(Vec::new());
    };
    decode_frames_after(bytes.as_ref(), replay_floor)
}

#[allow(dead_code)]
pub(crate) async fn read_batches_after_with_backend_async<B>(
    backend: &B,
    path: &Path,
    replay_floor: Sequence,
) -> Result<Vec<WalBatch>>
where
    B: StorageObjectReadBackend,
{
    let Some(bytes) = read_wal_object_with_backend_async(backend, path).await? else {
        return Ok(Vec::new());
    };
    decode_frames_after(bytes.as_ref(), replay_floor)
}

pub(crate) fn read_recovery_streams_after_paths_with_backend(
    backend: &NativeFileBackend,
    paths: &[PathBuf],
    replay_floor: Sequence,
) -> Result<Vec<Vec<WalBatch>>> {
    let mut streams = Vec::new();
    for path in paths {
        streams.push(read_batches_after_with_backend(
            backend,
            path,
            replay_floor,
        )?);
    }
    Ok(streams)
}

pub(crate) async fn read_recovery_streams_after_paths_with_backend_async<B>(
    backend: &B,
    paths: &[PathBuf],
    replay_floor: Sequence,
) -> Result<Vec<Vec<WalBatch>>>
where
    B: StorageObjectReadBackend,
{
    let mut streams = Vec::new();
    for path in paths {
        streams.push(read_batches_after_with_backend_async(backend, path, replay_floor).await?);
    }
    Ok(streams)
}

pub(crate) fn discovered_wal_paths_are_empty_with_backend(
    backend: &NativeFileBackend,
    paths: &[PathBuf],
    directory_files: &[StorageDirectoryFile],
) -> Result<bool> {
    for path in paths {
        let byte_len = if let Some(byte_len) = directory_files
            .iter()
            .find(|file| file.path() == path)
            .and_then(StorageDirectoryFile::byte_len)
        {
            byte_len
        } else {
            backend
                .capabilities()
                .require(StorageCapability::RandomRead)?;
            let object = backend.open_read_blocking(wal_storage_object(path))?;
            object.len_blocking()?
        };
        if byte_len != 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) async fn discovered_wal_paths_are_empty_with_backend_async<B>(
    backend: &B,
    paths: &[PathBuf],
    directory_files: &[StorageDirectoryFile],
) -> Result<bool>
where
    B: StorageReadBackend,
{
    for path in paths {
        let byte_len = if let Some(byte_len) = directory_files
            .iter()
            .find(|file| file.path() == path)
            .and_then(StorageDirectoryFile::byte_len)
        {
            byte_len
        } else {
            backend
                .capabilities()
                .require(StorageCapability::RandomRead)?;
            let object = backend.open_read(wal_storage_object(path)).await?;
            object.len().await?
        };
        if byte_len != 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(dead_code)]
pub(crate) async fn read_recovery_streams_after_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    replay_floor: Sequence,
) -> Result<Vec<Vec<WalBatch>>>
where
    B: StorageDirectoryListBackend + StorageObjectReadBackend,
{
    let paths = discover_wal_paths_with_backend_async(backend, db_path).await?;
    read_recovery_streams_after_paths_with_backend_async(backend, &paths, replay_floor).await
}

#[allow(dead_code)]
pub(crate) fn discover_wal_paths_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<Vec<PathBuf>> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    let files = backend.list_directory_files_blocking(StorageDirectoryId::native_file(db_path))?;
    discover_wal_paths_from_directory_entries(
        files.into_iter().map(|file| file.path().to_path_buf()),
    )
}

#[allow(dead_code)]
pub(crate) async fn discover_wal_paths_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
) -> Result<Vec<PathBuf>>
where
    B: StorageDirectoryListBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    let files = backend
        .list_directory_files(StorageDirectoryId::native_file(db_path))
        .await?;
    discover_wal_paths_from_directory_entries(
        files.into_iter().map(|file| file.path().to_path_buf()),
    )
}

pub(crate) fn discover_wal_paths_from_directory_entries<I>(files: I) -> Result<Vec<PathBuf>>
where
    I: IntoIterator<Item = PathBuf>,
{
    let mut paths_by_shard = BTreeMap::new();
    for path in files {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            return Err(Error::Corruption {
                message: format!("WAL file name is not valid UTF-8: {}", path.display()),
            });
        };
        let Some(shard_index) = wal_shard_index_from_file_name(file_name)? else {
            continue;
        };
        if paths_by_shard.insert(shard_index, path).is_some() {
            return Err(invalid_wal("duplicate WAL shard file"));
        }
    }
    Ok(paths_by_shard.into_values().collect())
}

pub(crate) fn merge_batch_streams_by_sequence<I>(streams: I) -> Result<Vec<WalBatch>>
where
    I: IntoIterator<Item = Vec<WalBatch>>,
{
    let mut merged = Vec::new();
    for stream in streams {
        validate_wal_stream_order(&stream)?;
        merged.extend(stream);
    }

    merged.sort_by_key(|batch| batch.sequence);
    for pair in merged.windows(2) {
        if pair[0].sequence == pair[1].sequence {
            return Err(invalid_wal("duplicate WAL sequence across streams"));
        }
    }

    Ok(merged)
}

#[allow(dead_code)]
pub(crate) fn rewrite_batches_after_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
    replay_floor: Sequence,
) -> Result<()> {
    let batches = read_batches_after_with_backend(backend, path, replay_floor)?;
    let bytes = encode_batches_after(&batches, replay_floor)?;
    rewrite_wal_object_with_backend(backend, path, bytes.into())?;

    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn append_batch_with_backend_async<B>(
    backend: &B,
    path: &Path,
    sequence: Sequence,
    operations: &[BatchOperation],
    durability: DurabilityMode,
) -> Result<()>
where
    B: StorageAppendBackend,
{
    let frame = encode_batch_frame(sequence, operations)?;
    let mut append = open_wal_append_object_with_backend_async(backend, path).await?;
    append.append(&frame, durability).await
}

#[allow(dead_code)]
pub(crate) async fn rewrite_batches_after_with_backend_async<B>(
    backend: &B,
    path: &Path,
    replay_floor: Sequence,
) -> Result<()>
where
    B: StorageObjectReadBackend + StorageWalRewriteBackend,
{
    let batches = read_batches_after_with_backend_async(backend, path, replay_floor).await?;
    let bytes = encode_batches_after(&batches, replay_floor)?;
    rewrite_wal_object_with_backend_async(backend, path, bytes.into()).await
}

fn wal_rewrite_tmp_path(path: &Path) -> PathBuf {
    if path.file_name().and_then(|name| name.to_str()) == Some(WAL_FILE_NAME) {
        return path.with_file_name(WAL_REWRITE_TMP_FILE_NAME);
    }
    let Some(file_name) = path.file_name() else {
        return path.with_extension("tmp");
    };
    let mut temporary_name = file_name.to_os_string();
    temporary_name.push(".tmp");
    path.with_file_name(temporary_name)
}

pub(crate) fn is_wal_rewrite_temporary_file_name(file_name: &str) -> bool {
    if file_name == WAL_REWRITE_TMP_FILE_NAME {
        return true;
    }
    file_name.strip_suffix(".tmp").is_some_and(|final_name| {
        matches!(
            wal_shard_index_from_final_file_name(final_name),
            Ok(Some(_))
        )
    })
}

fn open_wal_append_object_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<NativeFileAppendObject> {
    backend.capabilities().require(StorageCapability::Append)?;
    backend.open_append_blocking(wal_storage_object(path))
}

async fn open_wal_append_object_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<B::AppendObject>
where
    B: StorageAppendBackend,
{
    backend.capabilities().require(StorageCapability::Append)?;
    backend.open_append(wal_storage_object(path)).await
}

fn read_wal_object_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<Option<Arc<[u8]>>> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectRead)?;
    backend.read_object_bytes_blocking(wal_storage_object(path))
}

async fn read_wal_object_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<Option<Arc<[u8]>>>
where
    B: StorageObjectReadBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::ObjectRead)?;
    backend.read_object_bytes(wal_storage_object(path)).await
}

#[allow(dead_code)]
fn rewrite_wal_object_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
    bytes: Arc<[u8]>,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::AtomicWalRewrite)?;
    backend.rewrite_wal_blocking(
        wal_storage_object(path),
        wal_storage_object(&wal_rewrite_tmp_path(path)),
        bytes,
        DurabilityMode::SyncAll,
    )
}

async fn rewrite_wal_object_with_backend_async<B>(
    backend: &B,
    path: &Path,
    bytes: Arc<[u8]>,
) -> Result<()>
where
    B: StorageWalRewriteBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::AtomicWalRewrite)?;
    backend
        .rewrite_wal(
            wal_storage_object(path),
            wal_storage_object(&wal_rewrite_tmp_path(path)),
            bytes,
            DurabilityMode::Flush,
        )
        .await
}

fn wal_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::Wal, path)
}

fn send_wal_lane_command(
    lane: &WalFrontDoorLane,
    command: impl FnOnce(WalLaneReply) -> WalLaneCommand,
) -> Result<()> {
    enqueue_wal_lane_command(lane, command)?.wait()
}

fn enqueue_wal_lane_command(
    lane: &WalFrontDoorLane,
    command: impl FnOnce(WalLaneReply) -> WalLaneCommand,
) -> Result<WalLaneWaiter> {
    let sender = lane
        .sender
        .as_ref()
        .ok_or_else(wal_front_door_worker_stopped)?;
    let (reply, waiter) = WalLaneCompletion::pair();
    sender
        .send(command(reply))
        .map_err(|_| wal_front_door_worker_stopped())?;
    Ok(waiter)
}

#[allow(clippy::needless_pass_by_value)]
fn run_wal_lane_worker(
    backend: NativeFileBackend,
    path: PathBuf,
    writer_open: Arc<AtomicBool>,
    receiver: mpsc::Receiver<WalLaneCommand>,
) {
    let mut writer = None::<WalWriter>;
    while let Ok(command) = receiver.recv() {
        match command {
            WalLaneCommand::Append {
                frame,
                durability,
                reply,
            } => {
                let result = append_wal_lane_frame(
                    &backend,
                    &path,
                    &mut writer,
                    &writer_open,
                    &frame,
                    durability,
                );
                reply.complete(result);
            }
            WalLaneCommand::Persist { durability, reply } => {
                let result = persist_wal_lane(&mut writer, durability);
                reply.complete(result);
            }
            WalLaneCommand::Rewrite {
                replay_floor,
                reply,
            } => {
                let result =
                    rewrite_wal_lane_after_replay_floor(&backend, &path, &mut writer, replay_floor);
                reply.complete(result);
            }
        }
    }
}

fn append_wal_lane_frame(
    backend: &NativeFileBackend,
    path: &Path,
    writer: &mut Option<WalWriter>,
    writer_open: &AtomicBool,
    frame: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    if writer.is_none() {
        *writer = Some(WalWriter::open_append_with_backend(backend, path)?);
        writer_open.store(true, Ordering::Release);
    }
    writer
        .as_mut()
        .expect("writer opens before append")
        .append_frame(frame, durability)
}

fn persist_wal_lane(writer: &mut Option<WalWriter>, durability: DurabilityMode) -> Result<()> {
    if let Some(writer) = writer.as_mut() {
        writer.persist(durability)?;
    }
    Ok(())
}

fn rewrite_wal_lane_after_replay_floor(
    backend: &NativeFileBackend,
    path: &Path,
    writer: &mut Option<WalWriter>,
    replay_floor: Sequence,
) -> Result<()> {
    if let Some(writer) = writer.as_mut() {
        writer.persist(DurabilityMode::SyncAll)?;
    } else if wait_for_wal_storage_future(read_wal_object_with_backend_async(backend, path))?
        .is_none()
    {
        return Ok(());
    }
    wait_for_wal_storage_future(rewrite_batches_after_with_backend_async(
        backend,
        path,
        replay_floor,
    ))?;
    if let Some(writer) = writer.as_mut() {
        writer.reopen_append_with_backend(backend, path)?;
    }
    Ok(())
}

fn wal_front_door_worker_stopped() -> Error {
    Error::Corruption {
        message: "WAL front door worker stopped".to_owned(),
    }
}

fn wal_front_door_completion_poisoned() -> Error {
    Error::runtime_busy("WAL front door completion state is poisoned")
}

fn validate_wal_stream_order(batches: &[WalBatch]) -> Result<()> {
    let mut last_seen = Sequence::ZERO;
    for batch in batches {
        if batch.sequence <= last_seen {
            return Err(invalid_wal("WAL stream sequence did not increase"));
        }
        last_seen = batch.sequence;
    }
    Ok(())
}

fn wal_shard_index_from_path(path: &Path) -> Result<usize> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::Corruption {
            message: format!("WAL file name is not valid UTF-8: {}", path.display()),
        })?;
    wal_shard_index_from_file_name(file_name)?.ok_or_else(|| Error::Corruption {
        message: format!("not a WAL shard file: {}", path.display()),
    })
}

fn wal_shard_index_from_file_name(file_name: &str) -> Result<Option<usize>> {
    if file_name == WAL_FILE_NAME {
        return Ok(Some(0));
    }
    if is_wal_rewrite_temporary_file_name(file_name) {
        return Ok(None);
    }
    wal_shard_index_from_final_file_name(file_name)
}

fn wal_shard_index_from_final_file_name(file_name: &str) -> Result<Option<usize>> {
    let Some(suffix) = file_name.strip_prefix(WAL_SHARD_FILE_PREFIX) else {
        return Ok(None);
    };
    if suffix.len() != WAL_SHARD_FILE_DIGITS || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::Corruption {
            message: format!("malformed WAL shard file name: {file_name}"),
        });
    }
    let shard_index = suffix.parse::<usize>().map_err(|error| Error::Corruption {
        message: format!("malformed WAL shard file name {file_name}: {error}"),
    })?;
    if shard_index == 0 {
        return Err(Error::Corruption {
            message: "WAL shard 0 must use the legacy trine.wal file name".to_owned(),
        });
    }
    Ok(Some(shard_index))
}

fn encode_batches_after(batches: &[WalBatch], replay_floor: Sequence) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for batch in batches.iter().filter(|batch| batch.sequence > replay_floor) {
        bytes.extend_from_slice(&encode_batch_frame(batch.sequence, &batch.operations)?);
    }
    Ok(bytes)
}

fn encode_batch_frame(sequence: Sequence, operations: &[BatchOperation]) -> Result<Vec<u8>> {
    let payload = encode_payload(sequence, operations)?;
    let payload_checksum = checksum(&payload);
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::invalid_options("WAL payload exceeds u32::MAX bytes"))?;
    let header_checksum = header_checksum(payload_len, payload_checksum);

    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    frame.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&header_checksum.to_le_bytes());
    frame.extend_from_slice(&payload_checksum.to_le_bytes());
    frame.extend_from_slice(&payload);

    Ok(frame)
}

fn encode_payload(sequence: Sequence, operations: &[BatchOperation]) -> Result<Vec<u8>> {
    let op_count = u32::try_from(operations.len())
        .map_err(|_| Error::invalid_options("WAL operation count exceeds u32::MAX"))?;
    let mut bytes = Vec::new();

    put_u64(&mut bytes, sequence.get());
    put_u32(&mut bytes, op_count);
    for operation in operations {
        match operation {
            BatchOperation::Put { bucket, key, value } => {
                put_u8(&mut bytes, OP_INSERT);
                put_bytes(&mut bytes, bucket.as_bytes())?;
                put_bytes(&mut bytes, key)?;
                put_bytes(&mut bytes, value)?;
            }
            BatchOperation::Delete { bucket, key } => {
                put_u8(&mut bytes, OP_REMOVE);
                put_bytes(&mut bytes, bucket.as_bytes())?;
                put_bytes(&mut bytes, key)?;
            }
            BatchOperation::DeleteRange { bucket, range } => {
                put_u8(&mut bytes, OP_REMOVE_RANGE);
                put_bytes(&mut bytes, bucket.as_bytes())?;
                put_bound(&mut bytes, &range.start)?;
                put_bound(&mut bytes, &range.end)?;
            }
        }
    }

    Ok(bytes)
}

fn decode_frames_after(bytes: &[u8], replay_floor: Sequence) -> Result<Vec<WalBatch>> {
    let mut batches = Vec::new();
    let mut offset = 0;

    while offset < bytes.len() {
        if bytes.len() - offset < HEADER_LEN {
            break;
        }

        let magic = read_u32_at(bytes, offset)?;
        let version = read_u16_at(bytes, offset + 4)?;
        let payload_len = read_u32_at(bytes, offset + 6)?;
        let actual_header_checksum = read_u32_at(bytes, offset + 10)?;
        let payload_checksum = read_u32_at(bytes, offset + 14)?;
        let expected_header_checksum = header_checksum(payload_len, payload_checksum);

        if magic != WAL_MAGIC {
            return Err(Error::Corruption {
                message: "WAL magic mismatch".to_owned(),
            });
        }
        if version != WAL_FORMAT_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!("unsupported WAL version {version}"),
            });
        }
        if actual_header_checksum != expected_header_checksum {
            return Err(Error::Corruption {
                message: "WAL header checksum mismatch".to_owned(),
            });
        }

        let payload_len = payload_len as usize;
        let payload_start = offset + HEADER_LEN;
        let payload_end = payload_start + payload_len;
        if payload_end > bytes.len() {
            break;
        }

        let payload = &bytes[payload_start..payload_end];
        if checksum(payload) != payload_checksum {
            return Err(Error::Corruption {
                message: "WAL payload checksum mismatch".to_owned(),
            });
        }

        if payload_sequence(payload)? > replay_floor {
            batches.push(decode_payload(payload)?);
        }
        offset = payload_end;
    }

    Ok(batches)
}

fn payload_sequence(payload: &[u8]) -> Result<Sequence> {
    Ok(Sequence::new(read_u64_at(payload, 0)?))
}

fn decode_payload(payload: &[u8]) -> Result<WalBatch> {
    let mut cursor = Cursor::new(payload);
    let sequence = Sequence::new(cursor.read_u64()?);
    let op_count = cursor.read_u32()? as usize;
    if op_count > cursor.remaining_len() / MIN_WAL_OPERATION_BYTES {
        return Err(Error::InvalidFormat {
            message: "WAL operation count exceeds payload bytes".to_owned(),
        });
    }
    let mut operations = Vec::with_capacity(op_count);

    for _ in 0..op_count {
        let tag = cursor.read_u8()?;
        let bucket =
            String::from_utf8(cursor.read_bytes()?.to_vec()).map_err(|_| Error::InvalidFormat {
                message: "WAL bucket name is not valid UTF-8".to_owned(),
            })?;

        let operation = match tag {
            OP_INSERT => {
                let key = cursor.read_bytes()?.to_vec();
                let value = cursor.read_bytes()?.to_vec();
                BatchOperation::Put { bucket, key, value }
            }
            OP_REMOVE => {
                let key = cursor.read_bytes()?.to_vec();
                BatchOperation::Delete { bucket, key }
            }
            OP_REMOVE_RANGE => {
                let start = cursor.read_bound()?;
                let end = cursor.read_bound()?;
                BatchOperation::DeleteRange {
                    bucket,
                    range: KeyRange { start, end },
                }
            }
            _ => {
                return Err(Error::InvalidFormat {
                    message: format!("unknown WAL operation tag {tag}"),
                });
            }
        };

        operations.push(operation);
    }

    if !cursor.is_finished() {
        return Err(Error::InvalidFormat {
            message: "WAL payload has trailing bytes".to_owned(),
        });
    }

    Ok(WalBatch {
        sequence,
        operations,
    })
}

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("WAL byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

fn put_bound(bytes: &mut Vec<u8>, bound: &Bound<Vec<u8>>) -> Result<()> {
    match bound {
        Bound::Unbounded => put_u8(bytes, BOUND_UNBOUNDED),
        Bound::Included(value) => {
            put_u8(bytes, BOUND_INCLUDED);
            put_bytes(bytes, value)?;
        }
        Bound::Excluded(value) => {
            put_u8(bytes, BOUND_EXCLUDED);
            put_bytes(bytes, value)?;
        }
    }
    Ok(())
}

fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| invalid_wal("short u16"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_wal("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid_wal("short u64"))?;
    Ok(u64::from_le_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

fn header_checksum(payload_len: u32, payload_checksum: u32) -> u32 {
    let mut bytes = Vec::with_capacity(14);
    bytes.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    checksum(&bytes)
}

fn checksum(bytes: &[u8]) -> u32 {
    crate::checksum::crc32c(bytes)
}

fn invalid_wal(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid WAL: {message}"),
    }
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

struct Cursor<'payload> {
    payload: &'payload [u8],
    offset: usize,
}

impl<'payload> Cursor<'payload> {
    const fn new(payload: &'payload [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .payload
            .get(self.offset)
            .ok_or_else(|| invalid_wal("short u8"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let value = read_u32_at(self.payload, self.offset)?;
        self.offset += 4;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64> {
        let value = read_u64_at(self.payload, self.offset)?;
        self.offset += 8;
        Ok(value)
    }

    fn read_bytes(&mut self) -> Result<&'payload [u8]> {
        let len = self.read_u32()? as usize;
        let value = self
            .payload
            .get(self.offset..self.offset + len)
            .ok_or_else(|| invalid_wal("short bytes"))?;
        self.offset += len;
        Ok(value)
    }

    fn read_bound(&mut self) -> Result<Bound<Vec<u8>>> {
        match self.read_u8()? {
            BOUND_UNBOUNDED => Ok(Bound::Unbounded),
            BOUND_INCLUDED => Ok(Bound::Included(self.read_bytes()?.to_vec())),
            BOUND_EXCLUDED => Ok(Bound::Excluded(self.read_bytes()?.to_vec())),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown WAL range bound tag {tag}"),
            }),
        }
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.payload.len()
    }

    const fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        future::Future,
        task::{Context, Poll, Waker},
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        options::DurabilityMode, storage::NativeFileBackend, types::Sequence,
        write_batch::BatchOperation,
    };

    use super::{
        DEFAULT_WAL_SHARD_COUNT, WAL_FILE_NAME, WAL_FORMAT_VERSION, WAL_FRONT_DOOR_QUEUE_CAPACITY,
        WAL_MAGIC, WalFrontDoor, append_batch_with_backend_async, checksum, decode_frames_after,
        decode_payload, discover_wal_paths_with_backend, discover_wal_paths_with_backend_async,
        merge_batch_streams_by_sequence, read_all_batches, read_batches_after_with_backend,
        read_batches_after_with_backend_async, read_recovery_streams_after_with_backend_async,
        rewrite_batches_after_with_backend_async, wal_rewrite_tmp_path, wal_shard_path,
    };

    #[test]
    fn wal_front_door_accepts_whole_commit_record() {
        let dir = temp_dir("front-door-accept");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let path = dir.join(WAL_FILE_NAME);
        let backend = NativeFileBackend::new();
        let front_door =
            WalFrontDoor::open_single_lane_with_backend(&backend, &path).expect("front door opens");
        let operations = vec![BatchOperation::Put {
            bucket: "default".to_owned(),
            key: b"a".to_vec(),
            value: b"a1".to_vec(),
        }];

        let accepted = front_door
            .accept_commit(Sequence::new(7), &operations, DurabilityMode::Flush)
            .expect("front door accepts commit");

        assert_eq!(accepted.sequence(), Sequence::new(7));
        let stats = front_door.stats();
        assert_eq!(stats.queue_capacity, WAL_FRONT_DOOR_QUEUE_CAPACITY);
        assert_eq!(stats.open_shards, 1);
        assert_eq!(stats.records_accepted, 1);
        let batches =
            read_batches_after_with_backend(&backend, &path, Sequence::ZERO).expect("WAL reads");
        assert_eq!(
            batches,
            vec![super::WalBatch {
                sequence: Sequence::new(7),
                operations,
            }]
        );
        cleanup_dir(&dir);
    }

    #[test]
    fn wal_front_door_rewrite_reopens_append_lane() {
        let dir = temp_dir("front-door-rewrite");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let path = dir.join(WAL_FILE_NAME);
        let backend = NativeFileBackend::new();
        let front_door =
            WalFrontDoor::open_single_lane_with_backend(&backend, &path).expect("front door opens");

        front_door
            .accept_commit(Sequence::new(1), &[put("a", "old")], DurabilityMode::Flush)
            .expect("first commit accepts");
        front_door
            .accept_commit(Sequence::new(2), &[put("b", "kept")], DurabilityMode::Flush)
            .expect("second commit accepts");
        front_door
            .rewrite_after_replay_floor(Sequence::new(1))
            .expect("front door rewrites WAL");
        front_door
            .accept_commit(Sequence::new(3), &[put("c", "new")], DurabilityMode::Flush)
            .expect("append lane still accepts after rewrite");

        let sequences = read_batches_after_with_backend(&backend, &path, Sequence::ZERO)
            .expect("WAL reads")
            .into_iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![Sequence::new(2), Sequence::new(3)]);
        cleanup_dir(&dir);
    }

    #[test]
    fn wal_front_door_routes_commits_across_shards() {
        let dir = temp_dir("front-door-shards");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let backend = NativeFileBackend::new();
        let front_door =
            WalFrontDoor::open_sharded_with_backend(&backend, &dir, DEFAULT_WAL_SHARD_COUNT)
                .expect("front door opens");
        assert_eq!(front_door.stats().open_shards, 0);
        assert_eq!(
            front_door.stats().queue_capacity,
            WAL_FRONT_DOOR_QUEUE_CAPACITY
        );

        let accepted = (1..=DEFAULT_WAL_SHARD_COUNT)
            .map(|sequence| {
                front_door
                    .accept_commit(
                        Sequence::new(sequence as u64),
                        &[put("k", "v")],
                        DurabilityMode::Flush,
                    )
                    .expect("commit accepts")
            })
            .collect::<Vec<_>>();

        assert_eq!(
            accepted
                .iter()
                .map(|accepted| accepted.shard_index())
                .collect::<Vec<_>>(),
            (0..DEFAULT_WAL_SHARD_COUNT).collect::<Vec<_>>()
        );
        assert_eq!(front_door.stats().open_shards, DEFAULT_WAL_SHARD_COUNT);
        let batches = read_all_batches(&dir).expect("all WAL batches read");
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.sequence)
                .collect::<Vec<_>>(),
            (1..=DEFAULT_WAL_SHARD_COUNT)
                .map(|sequence| Sequence::new(sequence as u64))
                .collect::<Vec<_>>()
        );
        cleanup_dir(&dir);
    }

    #[test]
    fn wal_discovery_orders_legacy_and_shard_files() {
        let dir = temp_dir("wal-discovery");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let backend = NativeFileBackend::new();
        fs::write(wal_shard_path(&dir, 2), b"").expect("shard 2 writes");
        fs::write(wal_shard_path(&dir, 0), b"").expect("legacy WAL writes");
        fs::write(wal_shard_path(&dir, 1), b"").expect("shard 1 writes");

        let paths = discover_wal_paths_with_backend(&backend, &dir).expect("WAL paths discover");

        assert_eq!(
            paths,
            vec![
                wal_shard_path(&dir, 0),
                wal_shard_path(&dir, 1),
                wal_shard_path(&dir, 2),
            ]
        );
        cleanup_dir(&dir);
    }

    #[test]
    fn async_wal_discovery_orders_legacy_and_shard_files() {
        let dir = temp_dir("async-wal-discovery");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let backend = NativeFileBackend::new();
        fs::write(wal_shard_path(&dir, 2), b"").expect("shard 2 writes");
        fs::write(wal_shard_path(&dir, 0), b"").expect("legacy WAL writes");
        fs::write(wal_shard_path(&dir, 1), b"").expect("shard 1 writes");

        let paths = poll_ready(discover_wal_paths_with_backend_async(&backend, &dir))
            .expect("WAL paths discover through async helper");

        assert_eq!(
            paths,
            vec![
                wal_shard_path(&dir, 0),
                wal_shard_path(&dir, 1),
                wal_shard_path(&dir, 2),
            ]
        );
        cleanup_dir(&dir);
    }

    #[test]
    fn async_wal_batch_read_honors_replay_floor() {
        let dir = temp_dir("async-wal-read-floor");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let path = dir.join(WAL_FILE_NAME);
        let backend = NativeFileBackend::new();
        let front_door =
            WalFrontDoor::open_single_lane_with_backend(&backend, &path).expect("front door opens");

        front_door
            .accept_commit(Sequence::new(1), &[put("a", "old")], DurabilityMode::Flush)
            .expect("first commit accepts");
        front_door
            .accept_commit(Sequence::new(2), &[put("b", "new")], DurabilityMode::Flush)
            .expect("second commit accepts");

        let batches = poll_ready(read_batches_after_with_backend_async(
            &backend,
            &path,
            Sequence::new(1),
        ))
        .expect("WAL reads through async helper");
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.sequence)
                .collect::<Vec<_>>(),
            vec![Sequence::new(2)]
        );
        cleanup_dir(&dir);
    }

    #[test]
    fn async_wal_append_helper_writes_batch() {
        let dir = temp_dir("async-wal-append");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let path = dir.join(WAL_FILE_NAME);
        let backend = NativeFileBackend::new();

        poll_ready(append_batch_with_backend_async(
            &backend,
            &path,
            Sequence::new(1),
            &[put("k", "v")],
            DurabilityMode::Flush,
        ))
        .expect("async WAL append helper writes");

        let batches = read_batches_after_with_backend(&backend, &path, Sequence::ZERO)
            .expect("WAL reads after append");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].sequence, Sequence::new(1));
        cleanup_dir(&dir);
    }

    #[test]
    fn async_wal_rewrite_helper_keeps_batches_after_floor() {
        let dir = temp_dir("async-wal-rewrite");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let path = dir.join(WAL_FILE_NAME);
        let backend = NativeFileBackend::new();

        poll_ready(append_batch_with_backend_async(
            &backend,
            &path,
            Sequence::new(1),
            &[put("a", "old")],
            DurabilityMode::Flush,
        ))
        .expect("first async WAL append writes");
        poll_ready(append_batch_with_backend_async(
            &backend,
            &path,
            Sequence::new(2),
            &[put("b", "new")],
            DurabilityMode::Flush,
        ))
        .expect("second async WAL append writes");

        poll_ready(rewrite_batches_after_with_backend_async(
            &backend,
            &path,
            Sequence::new(1),
        ))
        .expect("async WAL rewrite helper rewrites");

        let batches = read_batches_after_with_backend(&backend, &path, Sequence::ZERO)
            .expect("WAL reads after rewrite");
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.sequence)
                .collect::<Vec<_>>(),
            vec![Sequence::new(2)]
        );
        cleanup_dir(&dir);
    }

    #[test]
    fn async_wal_recovery_streams_read_shards() {
        let dir = temp_dir("async-wal-streams");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let backend = NativeFileBackend::new();
        let front_door =
            WalFrontDoor::open_sharded_with_backend(&backend, &dir, DEFAULT_WAL_SHARD_COUNT)
                .expect("front door opens");

        for sequence in 1..=DEFAULT_WAL_SHARD_COUNT {
            front_door
                .accept_commit(
                    Sequence::new(sequence as u64),
                    &[put("k", "v")],
                    DurabilityMode::Flush,
                )
                .expect("commit accepts");
        }

        let streams = poll_ready(read_recovery_streams_after_with_backend_async(
            &backend,
            &dir,
            Sequence::ZERO,
        ))
        .expect("WAL streams read through async helper");
        let batches = merge_batch_streams_by_sequence(streams).expect("streams merge");
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.sequence)
                .collect::<Vec<_>>(),
            (1..=DEFAULT_WAL_SHARD_COUNT)
                .map(|sequence| Sequence::new(sequence as u64))
                .collect::<Vec<_>>()
        );
        cleanup_dir(&dir);
    }

    #[test]
    fn wal_rewrite_temp_paths_keep_shard_identity() {
        let dir = temp_dir("wal-rewrite-temp-paths");
        let legacy_path = wal_shard_path(&dir, 0);
        let shard_path = wal_shard_path(&dir, 1);

        assert_eq!(
            wal_rewrite_tmp_path(&legacy_path),
            dir.join("trine.wal.tmp")
        );
        assert_eq!(
            wal_rewrite_tmp_path(&shard_path),
            dir.join("trine.wal.shard-0001.tmp")
        );
    }

    #[test]
    fn wal_discovery_rejects_malformed_shard_file_name() {
        let dir = temp_dir("wal-discovery-malformed");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let backend = NativeFileBackend::new();
        fs::write(dir.join("trine.wal.shard-bad"), b"bad").expect("bad shard writes");

        let error = discover_wal_paths_with_backend(&backend, &dir)
            .expect_err("malformed shard name fails");

        assert!(
            error.to_string().contains("malformed WAL shard file name"),
            "unexpected error: {error}"
        );
        cleanup_dir(&dir);
    }

    fn poll_ready<T>(future: impl Future<Output = crate::Result<T>>) -> crate::Result<T> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("WAL storage future unexpectedly pending"),
        }
    }

    #[test]
    fn wal_stream_merge_orders_batches_across_sources() {
        let first = vec![batch(Sequence::new(1)), batch(Sequence::new(4))];
        let second = vec![batch(Sequence::new(2)), batch(Sequence::new(3))];

        let sequences = merge_batch_streams_by_sequence([first, second])
            .expect("streams merge")
            .into_iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>();

        assert_eq!(
            sequences,
            vec![
                Sequence::new(1),
                Sequence::new(2),
                Sequence::new(3),
                Sequence::new(4)
            ]
        );
    }

    #[test]
    fn wal_stream_merge_rejects_duplicate_sequence() {
        let error = merge_batch_streams_by_sequence([
            vec![batch(Sequence::new(1)), batch(Sequence::new(3))],
            vec![batch(Sequence::new(2)), batch(Sequence::new(3))],
        ])
        .expect_err("duplicate sequence fails");

        assert!(
            error
                .to_string()
                .contains("duplicate WAL sequence across streams"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn wal_stream_merge_rejects_non_increasing_source() {
        let error = merge_batch_streams_by_sequence([vec![
            batch(Sequence::new(2)),
            batch(Sequence::new(1)),
        ]])
        .expect_err("non-increasing source fails");

        assert!(
            error
                .to_string()
                .contains("WAL stream sequence did not increase"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn wal_decode_rejects_operation_count_before_large_allocation() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1_u64.to_le_bytes());
        payload.extend_from_slice(&u32::MAX.to_le_bytes());

        let error = decode_payload(&payload).expect_err("oversized operation count fails");
        assert!(
            error
                .to_string()
                .contains("operation count exceeds payload bytes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn wal_decode_after_floor_skips_old_operation_payloads() {
        let mut old_payload = Vec::new();
        old_payload.extend_from_slice(&1_u64.to_le_bytes());
        old_payload.extend_from_slice(&u32::MAX.to_le_bytes());

        let new_payload = super::encode_payload(
            Sequence::new(2),
            &[BatchOperation::Put {
                bucket: "default".to_owned(),
                key: b"a".to_vec(),
                value: b"a1".to_vec(),
            }],
        )
        .expect("new payload encodes");

        let mut bytes = frame_for_payload(&old_payload);
        bytes.extend_from_slice(&frame_for_payload(&new_payload));

        let batches =
            decode_frames_after(&bytes, Sequence::new(1)).expect("old payload is skipped");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].sequence, Sequence::new(2));

        let error = decode_frames_after(&bytes, Sequence::ZERO)
            .expect_err("old payload is decoded without a replay floor");
        assert!(
            error
                .to_string()
                .contains("operation count exceeds payload bytes"),
            "unexpected error: {error}"
        );
    }

    fn frame_for_payload(payload: &[u8]) -> Vec<u8> {
        let payload_len = u32::try_from(payload.len()).expect("test payload fits u32");
        let payload_checksum = checksum(payload);
        let header_checksum = super::header_checksum(payload_len, payload_checksum);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&header_checksum.to_le_bytes());
        bytes.extend_from_slice(&payload_checksum.to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn put(key: &str, value: &str) -> BatchOperation {
        BatchOperation::Put {
            bucket: "default".to_owned(),
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn batch(sequence: Sequence) -> super::WalBatch {
        super::WalBatch {
            sequence,
            operations: Vec::new(),
        }
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "trine-kv-wal-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn cleanup_dir(dir: &std::path::Path) {
        match fs::remove_dir_all(dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("failed to cleanup {}: {error}", dir.display()),
        }
    }
}
