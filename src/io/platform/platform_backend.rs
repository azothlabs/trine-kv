use std::{
    fs::File,
    io,
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering, mpsc as std_mpsc},
    thread,
};

use futures::{
    FutureExt, StreamExt, channel::mpsc, future::LocalBoxFuture, stream::FuturesUnordered,
};

use crate::{
    durability::requires_parent_dir_sync_after_rename,
    error::{Error, Result},
    options::DurabilityMode,
    storage::StorageReadBuffer,
};

use super::{
    NativeCompletionExecutor, PLATFORM_IO_FAILED, PLATFORM_IO_MAX_IN_FLIGHT,
    PlatformIoBackendMatrix, PlatformIoParentCreation, PlatformIoPublishPlan,
    ScheduledPlatformIoTask,
};

#[cfg(target_os = "macos")]
mod apple_dispatch;
#[cfg(target_os = "freebsd")]
mod freebsd_backend;
#[cfg(target_os = "linux")]
mod linux_backend;
#[cfg(target_os = "macos")]
mod macos_backend;
#[cfg(any(target_os = "illumos", target_os = "solaris"))]
mod solarish_backend;
#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "freebsd",
        target_os = "illumos",
        target_os = "solaris"
    ))
))]
mod unix_backend;
#[cfg(not(any(target_os = "linux", windows, unix)))]
mod unsupported_backend;
#[cfg(windows)]
mod windows_backend;

pub(super) fn matrix() -> PlatformIoBackendMatrix {
    #[cfg(target_os = "linux")]
    {
        linux_backend::matrix()
    }
    #[cfg(windows)]
    {
        windows_backend::matrix()
    }
    #[cfg(target_os = "macos")]
    {
        macos_backend::matrix()
    }
    #[cfg(target_os = "freebsd")]
    {
        freebsd_backend::matrix()
    }
    #[cfg(any(target_os = "illumos", target_os = "solaris"))]
    {
        solarish_backend::matrix()
    }
    #[cfg(all(
        unix,
        not(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "freebsd",
            target_os = "illumos",
            target_os = "solaris"
        ))
    ))]
    {
        unix_backend::matrix()
    }
    #[cfg(not(any(target_os = "linux", windows, unix)))]
    {
        unsupported_backend::matrix()
    }
}

pub(super) fn start_worker(
    state: Arc<std::sync::atomic::AtomicU8>,
) -> Option<NativeCompletionExecutor> {
    let (sender, receiver) = mpsc::channel(PLATFORM_IO_MAX_IN_FLIGHT);
    let (startup_sender, startup_receiver) = std_mpsc::sync_channel(1);
    let worker = thread::Builder::new()
        .name("trine-kv-platform-io-native".to_owned())
        .spawn(move || {
            let runtime = match compio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = startup_sender.send(Err(error.to_string()));
                    return;
                }
            };
            if !runtime_has_qualified_native_completion(&runtime) {
                let _ = startup_sender.send(Err(
                    "Compio selected a driver without qualified native file completion".to_owned(),
                ));
                return;
            }
            if startup_sender.send(Ok(())).is_err() {
                return;
            }
            runtime.block_on(run_worker(receiver, state));
        })
        .ok()?;

    if let Ok(Ok(())) = startup_receiver.recv() {
        Some(NativeCompletionExecutor {
            sender: Some(sender),
            worker: Some(worker),
        })
    } else {
        drop(sender);
        let _ = worker.join();
        None
    }
}

fn runtime_has_qualified_native_completion(runtime: &compio::runtime::Runtime) -> bool {
    #[cfg(target_os = "linux")]
    {
        runtime.driver_type().is_iouring()
    }
    #[cfg(windows)]
    {
        runtime.driver_type().is_iocp()
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        // macOS file completion is supplied by Trine's DispatchIO bridge.
        // FreeBSD and Solaris-family targets use the platform AIO paths
        // qualified by their operation matrices.
        let _ = runtime;
        true
    }
}

async fn run_worker(
    mut receiver: mpsc::Receiver<ScheduledPlatformIoTask>,
    state: Arc<std::sync::atomic::AtomicU8>,
) {
    let mut in_flight: FuturesUnordered<LocalBoxFuture<'static, ()>> = FuturesUnordered::new();
    let mut input_closed = false;

    loop {
        if input_closed && in_flight.is_empty() {
            break;
        }
        if in_flight.is_empty() {
            match receiver.next().await {
                Some(task) => in_flight.push(run_scheduled_task(task).boxed_local()),
                None => input_closed = true,
            }
            continue;
        }
        if input_closed {
            let _ = in_flight.next().await;
            continue;
        }

        futures::select_biased! {
            _ = in_flight.next().fuse() => {}
            task = receiver.next().fuse() => {
                match task {
                    Some(task) => in_flight.push(run_scheduled_task(task).boxed_local()),
                    None => input_closed = true,
                }
            }
        }
    }

    if state.load(Ordering::Acquire) == PLATFORM_IO_FAILED {
        receiver.close();
    }
}

async fn run_scheduled_task(mut scheduled: ScheduledPlatformIoTask) {
    let native_in_flight = scheduled
        .control_metrics
        .native_in_flight
        .fetch_add(1, Ordering::AcqRel)
        .saturating_add(1);
    scheduled
        .control_metrics
        .native_max_in_flight
        .fetch_max(native_in_flight, Ordering::Relaxed);
    let task = scheduled.take_task();
    let completion = task.failure_completion();
    if task.mark_execution_class(scheduled.class).is_err() {
        completion.complete();
        scheduled.state.store(PLATFORM_IO_FAILED, Ordering::Release);
        scheduled
            .control_metrics
            .native_in_flight
            .fetch_sub(1, Ordering::AcqRel);
        scheduled.finish();
        return;
    }
    if AssertUnwindSafe(task.run()).catch_unwind().await.is_err() {
        completion.complete();
        scheduled.state.store(PLATFORM_IO_FAILED, Ordering::Release);
    }
    scheduled
        .control_metrics
        .native_in_flight
        .fetch_sub(1, Ordering::AcqRel);
    scheduled.finish();
}

pub(super) async fn len(path: PathBuf) -> Result<u64> {
    let file = compio::fs::File::open(path).await.map_err(Error::Io)?;
    let metadata = file.metadata().await.map_err(Error::Io)?;
    Ok(metadata.len())
}

#[allow(clippy::unused_async)]
pub(super) async fn read_exact_at_owned(
    path: PathBuf,
    offset: usize,
    len: usize,
) -> Result<StorageReadBuffer> {
    #[cfg(target_os = "macos")]
    {
        apple_dispatch::read_exact_at_owned(&path, offset, len).await
    }

    #[cfg(not(target_os = "macos"))]
    {
        use compio::io::AsyncReadAtExt;

        let file = compio::fs::File::open(path).await.map_err(Error::Io)?;
        let buffer = vec![0; len];
        let compio::buf::BufResult(result, buffer) =
            file.read_exact_at(buffer, platform_offset(offset)?).await;
        result.map_err(Error::Io)?;
        Ok(StorageReadBuffer::from_vec(offset, buffer))
    }
}

#[allow(clippy::unused_async)]
#[cfg_attr(target_os = "macos", allow(unused_variables))]
pub(super) async fn read_optional(path: PathBuf, max_bytes: usize) -> Result<Option<Arc<[u8]>>> {
    #[cfg(target_os = "macos")]
    {
        // The backend matrix routes this operation to the bounded thread-pool
        // implementation on macOS so metadata and bytes come from one handle.
        unreachable!("macOS optional reads are routed to the thread-pool backend")
    }

    #[cfg(not(target_os = "macos"))]
    {
        use compio::io::AsyncReadAtExt;

        let file = match compio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::Io(error)),
        };
        let len = file.metadata().await.map_err(Error::Io)?.len();
        let len = usize::try_from(len).map_err(|_| Error::Corruption {
            message: format!("object {} length exceeds usize", path.display()),
        })?;
        if len > max_bytes {
            return Err(Error::Corruption {
                message: format!(
                    "object {} length {len} exceeds maximum {max_bytes}",
                    path.display()
                ),
            });
        }
        let buffer = vec![0; len];
        let compio::buf::BufResult(result, buffer) = file.read_exact_at(buffer, 0).await;
        result.map_err(Error::Io)?;
        Ok(Some(Arc::from(buffer)))
    }
}

pub(super) async fn publish(plan: PlatformIoPublishPlan) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        if plan.create_parent == PlatformIoParentCreation::CreateAll
            && let Some(parent) = plan.temporary_path.parent()
        {
            compio::fs::create_dir_all(parent)
                .await
                .map_err(Error::Io)?;
        }

        apple_dispatch::write_truncate(&plan.temporary_path, &plan.bytes, plan.durability).await?;
        compio::fs::rename(&plan.temporary_path, &plan.path)
            .await
            .map_err(|error| rename_error(&plan.temporary_path, &plan.path, &error))?;
        if requires_parent_dir_sync_after_rename(plan.durability) {
            sync_parent_directory(&plan.path).await?;
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        use compio::io::AsyncWriteAtExt;

        if plan.create_parent == PlatformIoParentCreation::CreateAll
            && let Some(parent) = plan.temporary_path.parent()
        {
            compio::fs::create_dir_all(parent)
                .await
                .map_err(Error::Io)?;
        }

        let mut file = compio::fs::File::create(&plan.temporary_path)
            .await
            .map_err(Error::Io)?;
        let compio::buf::BufResult(result, _buffer) =
            file.write_all_at(plan.bytes.to_vec(), 0).await;
        result.map_err(Error::Io)?;
        persist_published_file(&file, plan.durability).await?;
        file.close().await.map_err(Error::Io)?;
        compio::fs::rename(&plan.temporary_path, &plan.path)
            .await
            .map_err(|error| rename_error(&plan.temporary_path, &plan.path, &error))?;
        if requires_parent_dir_sync_after_rename(plan.durability) {
            sync_parent_directory(&plan.path).await?;
        }
        Ok(())
    }
}

pub(super) async fn open_append(path: PathBuf) -> Result<()> {
    if let Some(parent) = path.parent() {
        compio::fs::create_dir_all(parent)
            .await
            .map_err(Error::Io)?;
    }

    #[cfg(target_os = "macos")]
    {
        apple_dispatch::write_existing_or_create(&path, &[], 0, DurabilityMode::Buffered).await
    }

    #[cfg(not(target_os = "macos"))]
    {
        let mut options = compio::fs::OpenOptions::new();
        options.write(true).create(true);
        let file = options.open(path).await.map_err(Error::Io)?;
        file.close().await.map_err(Error::Io)
    }
}

pub(super) async fn append(
    path: PathBuf,
    bytes: Arc<[u8]>,
    durability: DurabilityMode,
) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let offset = match compio::fs::metadata(&path).await {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(Error::Io(error)),
        };
        apple_dispatch::write_existing_or_create(&path, &bytes, offset, durability).await
    }

    #[cfg(not(target_os = "macos"))]
    {
        use compio::io::AsyncWriteAtExt;

        let mut options = compio::fs::OpenOptions::new();
        options.write(true).create(true);
        let mut file = options.open(&path).await.map_err(Error::Io)?;
        let offset = match compio::fs::metadata(&path).await {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(Error::Io(error)),
        };
        let compio::buf::BufResult(result, _buffer) =
            file.write_all_at(bytes.to_vec(), offset).await;
        result.map_err(Error::Io)?;
        persist_wal_file(&file, durability).await?;
        file.close().await.map_err(Error::Io)
    }
}

#[allow(clippy::unused_async)]
pub(super) async fn persist_path(path: PathBuf, durability: DurabilityMode) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        apple_dispatch::sync_path(&path, durability).await
    }

    #[cfg(not(target_os = "macos"))]
    {
        let mut options = compio::fs::OpenOptions::new();
        options.write(true);
        let file = options.open(path).await.map_err(Error::Io)?;
        persist_wal_file(&file, durability).await?;
        file.close().await.map_err(Error::Io)
    }
}

pub(super) async fn delete_path(path: PathBuf) -> Result<()> {
    match compio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::Io(error)),
    }
}

pub(super) async fn create_dir_all(path: PathBuf) -> Result<()> {
    compio::fs::create_dir_all(path).await.map_err(Error::Io)
}

pub(super) async fn list_file_paths(path: PathBuf) -> Result<Vec<PathBuf>> {
    compio::runtime::spawn_blocking(move || list_file_paths_blocking(&path))
        .await
        .unwrap_or_else(|_| {
            Err(Error::runtime_busy(
                "platform directory listing fallback panicked",
            ))
        })
}

fn list_file_paths_blocking(path: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            paths.push(entry.path());
        }
    }
    paths.sort_unstable();
    Ok(paths)
}

pub(super) fn acquire_writer_lease(path: &Path, owner: &[u8]) -> Result<File> {
    super::blocking_fallback::acquire_writer_lease(path, owner)
}

#[cfg(not(target_os = "macos"))]
async fn persist_wal_file(file: &compio::fs::File, durability: DurabilityMode) -> Result<()> {
    match durability {
        DurabilityMode::Buffered | DurabilityMode::Flush => Ok(()),
        DurabilityMode::SyncData => file.sync_data().await.map_err(Error::Io),
        // Non-macOS fsync already flushes durably, so strict maps to a full sync.
        DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict => {
            file.sync_all().await.map_err(Error::Io)
        }
    }
}

#[cfg(not(target_os = "macos"))]
async fn persist_published_file(file: &compio::fs::File, durability: DurabilityMode) -> Result<()> {
    match durability {
        DurabilityMode::Buffered => Ok(()),
        DurabilityMode::Flush | DurabilityMode::SyncData => {
            file.sync_data().await.map_err(Error::Io)
        }
        DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict => {
            file.sync_all().await.map_err(Error::Io)
        }
    }
}

async fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(());
    };

    sync_directory(parent.to_path_buf()).await
}

fn rename_error(from: &Path, to: &Path, error: &io::Error) -> Error {
    Error::Io(io::Error::new(
        error.kind(),
        format!(
            "platform I/O temp rename {} -> {} failed: {error}",
            from.display(),
            to.display()
        ),
    ))
}

#[cfg(target_os = "macos")]
pub(super) async fn sync_directory(path: PathBuf) -> Result<()> {
    apple_dispatch::sync_path(&path, DurabilityMode::SyncAll).await
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(super) async fn sync_directory(path: PathBuf) -> Result<()> {
    let file = compio::fs::File::open(path).await.map_err(Error::Io)?;
    file.sync_all().await.map_err(Error::Io)?;
    file.close().await.map_err(Error::Io)
}

#[cfg(windows)]
pub(super) async fn sync_directory(path: PathBuf) -> Result<()> {
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;

    let mut options = compio::fs::OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    let file = match options.open(path).await {
        Ok(file) => file,
        Err(error) if crate::durability::is_windows_directory_sync_permission_denied(&error) => {
            return Ok(());
        }
        Err(error) => return Err(Error::Io(error)),
    };
    let sync_result = crate::durability::finish_windows_directory_sync(file.sync_all().await);
    file.close().await.map_err(Error::Io).and(sync_result)
}

#[cfg(not(any(unix, windows)))]
pub(super) async fn sync_directory(_path: PathBuf) -> Result<()> {
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn platform_offset(offset: usize) -> Result<u64> {
    u64::try_from(offset).map_err(|_| Error::invalid_options("platform I/O offset overflow"))
}
