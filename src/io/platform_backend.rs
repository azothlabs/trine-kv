use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
};

use crate::{
    durability::requires_parent_dir_sync_after_rename,
    error::{Error, Result},
    options::DurabilityMode,
    storage::StorageReadBuffer,
};

use super::{PlatformIoBackendMatrix, PlatformIoTask};

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

pub(super) fn run_worker(receiver: mpsc::Receiver<PlatformIoTask>) {
    let runtime = match compio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            let message = format!("platform I/O runtime failed to start: {error}");
            for task in receiver {
                task.complete_start_error(&message);
            }
            return;
        }
    };

    for task in receiver {
        runtime.block_on(task.run());
    }
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
        apple_dispatch::read_exact_at_owned(&path, offset, len)
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
pub(super) async fn read_optional(path: PathBuf, max_bytes: usize) -> Result<Option<Arc<[u8]>>> {
    ensure_optional_read_len(&path, max_bytes)?;
    #[cfg(target_os = "macos")]
    {
        apple_dispatch::read_optional(&path, max_bytes)
    }

    #[cfg(not(target_os = "macos"))]
    {
        match compio::fs::read(path).await {
            Ok(bytes) => Ok(Some(Arc::from(bytes))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::Io(error)),
        }
    }
}

fn ensure_optional_read_len(path: &Path, max_bytes: usize) -> Result<()> {
    let len = match std::fs::metadata(path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::Io(error)),
    };
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
    Ok(())
}

pub(super) async fn write_temp_rename(
    path: PathBuf,
    tmp_path: PathBuf,
    bytes: Arc<[u8]>,
    durability: DurabilityMode,
    create_parent: bool,
    sync_parent_after_rename: bool,
) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        if create_parent {
            if let Some(parent) = tmp_path.parent() {
                compio::fs::create_dir_all(parent)
                    .await
                    .map_err(Error::Io)?;
            }
        }

        apple_dispatch::write_truncate(&tmp_path, &bytes, durability)?;
        compio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|error| rename_error(&tmp_path, &path, &error))?;
        if sync_parent_after_rename && requires_parent_dir_sync_after_rename(durability) {
            sync_parent_directory(&path).await?;
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        use compio::io::AsyncWriteAtExt;

        if create_parent {
            if let Some(parent) = tmp_path.parent() {
                compio::fs::create_dir_all(parent)
                    .await
                    .map_err(Error::Io)?;
            }
        }

        let mut file = compio::fs::File::create(&tmp_path)
            .await
            .map_err(Error::Io)?;
        let compio::buf::BufResult(result, _buffer) = file.write_all_at(bytes.to_vec(), 0).await;
        result.map_err(Error::Io)?;
        persist_published_file(&file, durability).await?;
        file.close().await.map_err(Error::Io)?;
        compio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|error| rename_error(&tmp_path, &path, &error))?;
        if sync_parent_after_rename && requires_parent_dir_sync_after_rename(durability) {
            sync_parent_directory(&path).await?;
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
        apple_dispatch::write_existing_or_create(&path, &[], 0, DurabilityMode::Buffered)
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
        apple_dispatch::write_existing_or_create(&path, &bytes, offset, durability)
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
        apple_dispatch::sync_path(&path, durability)
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
    super::platform_threadpool::acquire_writer_lease(path, owner)
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
#[allow(clippy::unused_async)]
pub(super) async fn sync_directory(path: PathBuf) -> Result<()> {
    apple_dispatch::sync_path(&path, DurabilityMode::SyncAll)
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
