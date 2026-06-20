use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
    storage::StorageReadBuffer,
};

use super::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTask, PlatformIoTaskClass};

#[cfg_attr(feature = "platform-io-native", allow(dead_code))]
pub(super) fn matrix() -> PlatformIoBackendMatrix {
    #[cfg(any(unix, windows))]
    {
        use PlatformIoTaskClass::ThreadPoolManagedAsync;

        PlatformIoBackendMatrix {
            kind: PlatformIoBackendKind::ThreadPoolManaged,
            length_lookup: ThreadPoolManagedAsync,
            owned_random_read: ThreadPoolManagedAsync,
            optional_whole_object_read: ThreadPoolManagedAsync,
            temp_write_rename_publish: ThreadPoolManagedAsync,
            append_object_open: ThreadPoolManagedAsync,
            append: ThreadPoolManagedAsync,
            persist: ThreadPoolManagedAsync,
            wal_rewrite: ThreadPoolManagedAsync,
            object_delete: ThreadPoolManagedAsync,
            directory_create: ThreadPoolManagedAsync,
            directory_sync: ThreadPoolManagedAsync,
            directory_listing: ThreadPoolManagedAsync,
            writer_lease_acquire: ThreadPoolManagedAsync,
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        use PlatformIoTaskClass::Unsupported;

        PlatformIoBackendMatrix {
            kind: PlatformIoBackendKind::UnsupportedFallback,
            length_lookup: Unsupported,
            owned_random_read: Unsupported,
            optional_whole_object_read: Unsupported,
            temp_write_rename_publish: Unsupported,
            append_object_open: Unsupported,
            append: Unsupported,
            persist: Unsupported,
            wal_rewrite: Unsupported,
            object_delete: Unsupported,
            directory_create: Unsupported,
            directory_sync: Unsupported,
            directory_listing: Unsupported,
            writer_lease_acquire: Unsupported,
        }
    }
}

pub(super) fn run_worker(receiver: crossbeam_channel::Receiver<PlatformIoTask>) {
    for task in receiver {
        task.run_thread_pool();
    }
}

pub(super) fn len(path: PathBuf) -> Result<u64> {
    Ok(fs::metadata(path)?.len())
}

pub(super) fn read_exact_at_owned(
    path: PathBuf,
    offset: usize,
    len: usize,
) -> Result<StorageReadBuffer> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(platform_offset(offset)?))?;
    let mut buffer = vec![0; len];
    file.read_exact(&mut buffer)?;
    Ok(StorageReadBuffer::from_vec(offset, buffer))
}

pub(super) fn read_optional(path: PathBuf, max_bytes: usize) -> Result<Option<Arc<[u8]>>> {
    let len = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
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
    match fs::read(path) {
        Ok(bytes) => Ok(Some(Arc::from(bytes))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Error::Io(error)),
    }
}

pub(super) fn write_temp_rename(
    path: &Path,
    tmp_path: &Path,
    bytes: &[u8],
    durability: DurabilityMode,
    create_parent: bool,
    sync_parent_on_sync_all: bool,
) -> Result<()> {
    if create_parent {
        if let Some(parent) = tmp_path.parent() {
            fs::create_dir_all(parent)?;
        }
    }

    {
        let mut file = File::create(tmp_path)?;
        file.write_all(bytes)?;
        persist_file(&file, durability)?;
    }

    fs::rename(tmp_path, path)?;
    if sync_parent_on_sync_all && durability == DurabilityMode::SyncAll {
        sync_parent_directory(path)?;
    }
    Ok(())
}

pub(super) fn open_append(path: PathBuf) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .map(drop)
        .map_err(Error::Io)
}

pub(super) fn append(path: PathBuf, bytes: &[u8], durability: DurabilityMode) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().append(true).create(true).open(path)?;
    file.write_all(bytes)?;
    persist_file(&file, durability)
}

pub(super) fn persist_path(path: PathBuf, durability: DurabilityMode) -> Result<()> {
    if !requires_sync(durability) {
        return Ok(());
    }

    let file = OpenOptions::new().read(true).write(true).open(path)?;
    persist_file(&file, durability)
}

pub(super) fn delete_path(path: PathBuf) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::Io(error)),
    }
}

pub(super) fn create_dir_all(path: PathBuf) -> Result<()> {
    fs::create_dir_all(path).map_err(Error::Io)
}

pub(super) fn list_file_paths(path: PathBuf) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            paths.push(entry.path());
        }
    }
    paths.sort_unstable();
    Ok(paths)
}

pub(super) fn acquire_writer_lease(path: &Path, owner: &[u8]) -> Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(Error::Io)?;
    if !fs4::fs_std::FileExt::try_lock_exclusive(&file).map_err(Error::Io)? {
        return Err(Error::Corruption {
            message: format!("database lock is already held: {}", path.display()),
        });
    }

    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    if let Err(error) = file.write_all(owner) {
        return Err(Error::Io(error));
    }
    if let Err(error) = file.flush() {
        return Err(Error::Io(error));
    }
    Ok(file)
}

fn persist_file(file: &File, durability: DurabilityMode) -> Result<()> {
    crate::durability::sync_file_for_durability(file, durability)
}

fn requires_sync(durability: DurabilityMode) -> bool {
    crate::durability::requires_file_sync(durability)
}

fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(());
    };

    sync_directory(parent.to_path_buf())
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(super) fn sync_directory(path: PathBuf) -> Result<()> {
    let file = File::open(path)?;
    file.sync_all().map_err(Error::Io)
}

#[cfg(target_os = "macos")]
pub(super) fn sync_directory(path: PathBuf) -> Result<()> {
    let file = File::open(path)?;
    file.sync_all().map_err(Error::Io)
}

#[cfg(windows)]
pub(super) fn sync_directory(path: PathBuf) -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;

    let file = match OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if crate::durability::is_windows_directory_sync_permission_denied(&error) => {
            return Ok(());
        }
        Err(error) => return Err(Error::Io(error)),
    };
    crate::durability::finish_windows_directory_sync(file.sync_all())
}

#[cfg(not(any(unix, windows)))]
pub(super) fn sync_directory(_path: PathBuf) -> Result<()> {
    Err(Error::unsupported_backend(
        "platform I/O thread-pool storage",
    ))
}

fn platform_offset(offset: usize) -> Result<u64> {
    u64::try_from(offset).map_err(|_| Error::invalid_options("platform I/O offset overflow"))
}
