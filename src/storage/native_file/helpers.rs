use super::{
    Arc, BlockingStorageReadBackend, BlockingStorageReadObject, DurabilityMode, Error, File, Mutex,
    MutexGuard, NativeFileBackend, NativeFileObject, NativeFileStorageMetrics, OpenOptions, Path,
    PathBuf, Read, Result, Seek, SeekFrom, StorageCapabilities, StorageCapability,
    StorageDirectoryFile, StorageDirectoryId, StorageObjectId, StorageObjectKind,
    StorageObjectListRequest, StorageReadBuffer, SystemTime, UNIX_EPOCH, Write,
    allocate_read_buffer, ensure_whole_object_read_len, fs, io,
    requires_parent_dir_sync_after_rename, sync_dir_after_renames, sync_parent_dir_after_rename,
    u64_to_usize, usize_to_u64,
};

pub(in crate::storage) fn read_exact_from_native_file(
    object: &StorageObjectId,
    offset: usize,
    bytes: &mut [u8],
) -> Result<()> {
    let file = NativeFileBackend::new().open_read_blocking(object.clone())?;
    file.read_exact_at_blocking(offset, bytes)
}

pub(in crate::storage) fn read_exact_at_native_file_owned(
    object: &StorageObjectId,
    offset: usize,
    len: usize,
) -> Result<StorageReadBuffer> {
    let mut bytes = allocate_read_buffer(len)?;
    read_exact_from_native_file(object, offset, &mut bytes)?;
    Ok(StorageReadBuffer::from_vec(offset, bytes))
}

pub(in crate::storage) fn lock_native_read_file<'file>(
    file: &'file Mutex<File>,
    object: &StorageObjectId,
) -> Result<MutexGuard<'file, File>> {
    file.lock().map_err(|_| Error::Corruption {
        message: format!(
            "referenced {} {} handle lock poisoned",
            object.kind().as_str(),
            object.path().display()
        ),
    })
}

pub(in crate::storage) fn len_native_file_handle(
    file: &Mutex<File>,
    object: &StorageObjectId,
) -> Result<u64> {
    let file = lock_native_read_file(file, object)?;
    Ok(file.metadata()?.len())
}

pub(in crate::storage) fn read_exact_at_native_file_handle(
    file: &Mutex<File>,
    object: &StorageObjectId,
    offset: usize,
    bytes: &mut [u8],
) -> Result<()> {
    let mut file = lock_native_read_file(file, object)?;
    read_exact_at_native_file(&mut file, offset, bytes)
}

pub(in crate::storage) fn read_exact_at_native_file_handle_owned(
    file: &Mutex<File>,
    object: &StorageObjectId,
    offset: usize,
    len: usize,
) -> Result<StorageReadBuffer> {
    let mut bytes = allocate_read_buffer(len)?;
    read_exact_at_native_file_handle(file, object, offset, &mut bytes)?;
    Ok(StorageReadBuffer::from_vec(offset, bytes))
}

pub(in crate::storage) fn open_native_file(object: &StorageObjectId) -> Result<File> {
    File::open(object.path()).map_err(|error| Error::Corruption {
        message: format!(
            "referenced {} {} cannot be opened: {error}",
            object.kind().as_str(),
            object.path().display()
        ),
    })
}

pub(in crate::storage) fn read_exact_at_native_file(
    file: &mut File,
    offset: usize,
    bytes: &mut [u8],
) -> Result<()> {
    file.seek(SeekFrom::Start(usize_to_u64(
        offset,
        "storage object read offset",
    )?))?;
    file.read_exact(bytes)?;
    Ok(())
}

pub(in crate::storage) fn require_native_file_object_read() -> Result<()> {
    let capabilities = StorageCapabilities::native_file_read();
    capabilities.require(StorageCapability::ObjectRead)
}

pub(in crate::storage) fn require_native_file_object_listing() -> Result<()> {
    let capabilities = StorageCapabilities::native_file_read();
    capabilities.require(StorageCapability::ObjectListing)
}

pub(in crate::storage) fn require_native_file_directory_listing() -> Result<()> {
    let capabilities = StorageCapabilities::native_file_read();
    capabilities.require(StorageCapability::DirectoryListing)
}

pub(in crate::storage) fn require_native_file_append(object: &StorageObjectId) -> Result<()> {
    if object.kind() != StorageObjectKind::Wal {
        return Err(Error::invalid_options(
            "append storage objects must use WAL object kind",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::Append)
}

pub(in crate::storage) fn require_native_file_manifest_read(
    object: &StorageObjectId,
) -> Result<()> {
    if object.kind() != StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "current manifest read requires a manifest storage object",
        ));
    }
    Ok(())
}

pub(in crate::storage) fn prepare_native_file_manifest_publish(
    object: &StorageObjectId,
    durability: DurabilityMode,
) -> Result<(PathBuf, PathBuf)> {
    if object.kind() != StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "manifest publish requires a manifest storage object",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::AtomicManifestPublish)?;
    capabilities.require_durability(durability)?;

    let path = object.path().to_path_buf();
    let tmp_path = path.with_extension("tmp");
    Ok((path, tmp_path))
}

pub(in crate::storage) fn prepare_native_file_object_write(
    object: &StorageObjectId,
    durability: DurabilityMode,
) -> Result<(PathBuf, PathBuf)> {
    match object.kind() {
        StorageObjectKind::Manifest => {
            return Err(Error::invalid_options(
                "manifest storage objects must use manifest publish",
            ));
        }
        StorageObjectKind::Temporary => {
            return Err(Error::invalid_options(
                "temporary storage objects must use their owning publish operation",
            ));
        }
        StorageObjectKind::Blob
        | StorageObjectKind::ContentChunk
        | StorageObjectKind::ContentDescriptor
        | StorageObjectKind::ContentUpload
        | StorageObjectKind::RecoveryReport
        | StorageObjectKind::Table
        | StorageObjectKind::Wal
        | StorageObjectKind::WriterLease => {}
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::ObjectWrite)?;
    capabilities.require_durability(durability)?;

    let path = object.path().to_path_buf();
    let tmp_path = if object.kind() == StorageObjectKind::Wal {
        native_file_tmp_path_by_appending_suffix(&path)?
    } else {
        path.with_extension("tmp")
    };
    Ok((path, tmp_path))
}

fn native_file_tmp_path_by_appending_suffix(path: &Path) -> Result<PathBuf> {
    let Some(file_name) = path.file_name() else {
        return Err(Error::invalid_options(format!(
            "native file object path has no file name: {}",
            path.display()
        )));
    };

    let mut tmp_name = file_name.to_os_string();
    tmp_name.push(".tmp");
    Ok(path.with_file_name(tmp_name))
}

pub(in crate::storage) fn require_native_file_object_delete(
    object: &StorageObjectId,
) -> Result<()> {
    if object.kind() == StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "manifest storage objects must use manifest publish",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::ObjectDelete)
}

pub(in crate::storage) fn prepare_native_file_wal_rewrite(
    object: &StorageObjectId,
    temporary_object: &StorageObjectId,
    durability: DurabilityMode,
) -> Result<(PathBuf, PathBuf)> {
    if object.kind() != StorageObjectKind::Wal || temporary_object.kind() != StorageObjectKind::Wal
    {
        return Err(Error::invalid_options(
            "WAL rewrite requires WAL storage objects",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::AtomicWalRewrite)?;
    capabilities.require_durability(durability)?;

    let path = object.path().to_path_buf();
    let tmp_path = temporary_object.path().to_path_buf();
    if path == tmp_path {
        return Err(Error::invalid_options(
            "WAL rewrite temporary object must differ from final object",
        ));
    }
    if path.parent() != tmp_path.parent() {
        return Err(Error::invalid_options(
            "WAL rewrite temporary object must share the final object's parent directory",
        ));
    }

    Ok((path, tmp_path))
}

#[cfg(any(unix, windows, target_os = "wasi"))]
pub(in crate::storage) fn require_native_file_writer_lease(object: &StorageObjectId) -> Result<()> {
    if object.kind() != StorageObjectKind::WriterLease {
        return Err(Error::invalid_options(
            "writer lease requires a writer lease storage object",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::WriterLease)
}

pub(in crate::storage) fn require_native_file_directory_create() -> Result<()> {
    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::DirectoryCreate)
}

pub(in crate::storage) fn require_native_file_directory_sync() -> Result<()> {
    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::DirectorySync)?;
    capabilities.require(StorageCapability::StrictMetadataSync)
}

pub(in crate::storage) fn read_native_file_object_bytes(
    object: &StorageObjectId,
) -> Result<Option<Arc<[u8]>>> {
    require_native_file_object_read()?;

    let file = match File::open(object.path()) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::Io(error)),
    };
    let object = NativeFileObject {
        object: object.clone(),
        file: Arc::new(Mutex::new(file)),
        runtime: None,
        #[cfg(feature = "platform-io")]
        platform_io: None,
        metrics: Arc::new(NativeFileStorageMetrics::default()),
    };
    let len = u64_to_usize(object.len_blocking()?, "storage object length")?;
    ensure_whole_object_read_len(&object.object, len)?;
    let buffer = object.read_exact_at_owned_blocking(0, len)?;
    debug_assert_eq!(buffer.offset(), 0);
    debug_assert_eq!(buffer.len(), len);
    debug_assert_eq!(buffer.is_empty(), len == 0);
    Ok(Some(buffer.into_arc_bytes()))
}

pub(in crate::storage) fn open_native_append_file(object: &StorageObjectId) -> Result<File> {
    require_native_file_append(object)?;

    if let Some(parent) = object.path().parent() {
        fs::create_dir_all(parent)?;
    }

    #[cfg(target_os = "wasi")]
    {
        return OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(object.path())
            .map_err(Error::from);
    }

    #[cfg(not(target_os = "wasi"))]
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(object.path())
        .map_err(Error::from)
}

pub(in crate::storage) fn lock_native_append_file<'file>(
    file: &'file Mutex<File>,
    object: &StorageObjectId,
) -> Result<MutexGuard<'file, File>> {
    file.lock().map_err(|_| Error::Corruption {
        message: format!(
            "referenced {} {} append handle lock poisoned",
            object.kind().as_str(),
            object.path().display()
        ),
    })
}

pub(in crate::storage) fn append_native_file_object(
    file: &mut File,
    object: &StorageObjectId,
    bytes: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::Append)?;
    capabilities.require_durability(durability)?;

    #[cfg(test)]
    crate::storage::fault_injection::check(
        crate::storage::fault_injection::StorageFaultPoint::WalAppend,
        Some(object.kind()),
        object.path(),
    )?;

    #[cfg(target_os = "wasi")]
    file.seek(SeekFrom::End(0))?;
    file.write_all(bytes)?;
    persist_native_append_file(file, object, durability)
}

pub(in crate::storage) fn persist_native_append_file(
    file: &mut File,
    object: &StorageObjectId,
    durability: DurabilityMode,
) -> Result<()> {
    let capabilities = StorageCapabilities::native_file();
    capabilities.require_durability(durability)?;

    #[cfg(not(test))]
    let _ = object;

    #[cfg(test)]
    crate::storage::fault_injection::check(
        crate::storage::fault_injection::StorageFaultPoint::WalPersist,
        Some(object.kind()),
        object.path(),
    )?;

    match durability {
        DurabilityMode::Buffered => Ok(()),
        DurabilityMode::Flush => {
            file.flush()?;
            Ok(())
        }
        DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict => {
            crate::durability::sync_file_for_durability(file, durability)
        }
    }
}

pub(in crate::storage) fn rewrite_native_file_wal(
    object: &StorageObjectId,
    temporary_object: &StorageObjectId,
    bytes: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    let (path, tmp_path) = prepare_native_file_wal_rewrite(object, temporary_object, durability)?;
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent)?;
    }

    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        sync_native_file_for_durability(&file, durability)?;
    }
    #[cfg(test)]
    crate::storage::fault_injection::check(
        crate::storage::fault_injection::StorageFaultPoint::WalRewritePublish,
        Some(object.kind()),
        object.path(),
    )?;
    fs::rename(&tmp_path, &path)?;
    if requires_parent_dir_sync_after_rename(durability) {
        sync_native_file_parent_directory_after_rename(&path)?;
    }

    Ok(())
}

#[cfg(any(unix, windows))]
pub(in crate::storage) fn acquire_native_file_writer_lease(
    object: &StorageObjectId,
) -> Result<File> {
    require_native_file_writer_lease(object)?;

    if let Some(parent) = object.path().parent() {
        fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(object.path())
        .map_err(Error::Io)?;
    if fs4::fs_std::FileExt::try_lock_exclusive(&file).map_err(Error::Io)? {
        Ok(file)
    } else {
        Err(Error::Corruption {
            message: format!("database lock is already held: {}", object.path().display()),
        })
    }
}

#[cfg(target_os = "wasi")]
pub(in crate::storage) fn acquire_native_file_writer_lease(
    object: &StorageObjectId,
) -> Result<File> {
    require_native_file_writer_lease(object)?;

    if let Some(parent) = object.path().parent() {
        fs::create_dir_all(parent)?;
    }

    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(object.path())
        .map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                Error::Corruption {
                    message: format!("database lock is already held: {}", object.path().display()),
                }
            } else {
                Error::Io(error)
            }
        })
}

#[cfg(not(any(unix, windows)))]
#[cfg(not(target_os = "wasi"))]
pub(in crate::storage) fn acquire_native_file_writer_lease(
    _object: &StorageObjectId,
) -> Result<File> {
    Err(Error::unsupported_backend("native file writer lease"))
}

pub(in crate::storage) fn writer_lease_owner_text() -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("pid={}\nnonce={nonce}\n", writer_lease_process_owner())
}

#[cfg(not(target_os = "wasi"))]
fn writer_lease_process_owner() -> String {
    std::process::id().to_string()
}

#[cfg(target_os = "wasi")]
fn writer_lease_process_owner() -> String {
    "wasi".to_owned()
}

pub(in crate::storage) fn write_native_file_writer_lease_owner(
    file: &mut File,
    owner: &str,
) -> Result<()> {
    // The exclusive lease is the OS file lock held by this open handle. The
    // owner text is only a release-time guard and diagnostic aid, so it does not
    // need a storage sync.
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(owner.as_bytes())?;
    file.flush()?;
    Ok(())
}

pub(in crate::storage) fn clear_native_file_writer_lease_owner(file: &mut File) -> Result<()> {
    #[cfg(target_os = "wasi")]
    {
        let _ = file;
        return Ok(());
    }

    #[cfg(not(target_os = "wasi"))]
    {
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.flush()?;
        Ok(())
    }
}

pub(in crate::storage) fn create_native_file_directory_all(
    directory: &StorageDirectoryId,
) -> Result<()> {
    require_native_file_directory_create()?;

    fs::create_dir_all(directory.path()).map_err(Error::from)
}

pub(in crate::storage) fn list_native_file_directory_files(
    directory: &StorageDirectoryId,
) -> Result<Vec<StorageDirectoryFile>> {
    require_native_file_directory_listing()?;

    list_native_file_directory_entries(directory)
}

fn list_native_file_directory_entries(
    directory: &StorageDirectoryId,
) -> Result<Vec<StorageDirectoryFile>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(directory.path())? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        files.push(StorageDirectoryFile::native_file_with_len(
            entry.path(),
            metadata.len(),
        ));
    }

    files.sort_unstable();
    Ok(files)
}

pub(in crate::storage) fn sync_native_file_directory_after_renames(
    directory: &StorageDirectoryId,
) -> Result<()> {
    require_native_file_directory_sync()?;

    #[cfg(test)]
    crate::storage::fault_injection::check(
        crate::storage::fault_injection::StorageFaultPoint::DirectorySync,
        None,
        directory.path(),
    )?;

    sync_dir_after_renames(directory.path())
}

pub(in crate::storage) fn sync_native_file_parent_directory_after_rename(
    path: &Path,
) -> Result<()> {
    require_native_file_directory_sync()?;

    #[cfg(test)]
    crate::storage::fault_injection::check(
        crate::storage::fault_injection::StorageFaultPoint::DirectorySync,
        None,
        path,
    )?;

    sync_parent_dir_after_rename(path)
}

pub(in crate::storage) fn read_current_manifest_from_native_file(
    object: &StorageObjectId,
) -> Result<Option<Arc<[u8]>>> {
    require_native_file_manifest_read(object)?;

    match fs::read(object.path()) {
        Ok(bytes) => Ok(Some(Arc::from(bytes))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(in crate::storage) fn list_native_file_objects(
    request: &StorageObjectListRequest,
) -> Result<Vec<StorageObjectId>> {
    require_native_file_object_listing()?;

    let mut paths = Vec::new();
    for entry in fs::read_dir(request.root())? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        paths.push(entry.path());
    }
    Ok(native_file_objects_from_paths(request, paths))
}

pub(in crate::storage) fn native_file_objects_from_paths(
    request: &StorageObjectListRequest,
    paths: Vec<PathBuf>,
) -> Vec<StorageObjectId> {
    let mut objects = paths
        .into_iter()
        .filter(|path| native_file_matches_list_request(request, path))
        .map(|path| StorageObjectId::native_file(request.kind(), path))
        .collect::<Vec<_>>();
    objects.sort_unstable();
    objects
}

pub(in crate::storage) fn native_file_matches_list_request(
    request: &StorageObjectListRequest,
    path: &Path,
) -> bool {
    request.file_extension().is_none_or(|expected| {
        path.extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
    })
}

pub(in crate::storage) fn write_native_file_object(
    object: &StorageObjectId,
    bytes: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    let (path, tmp_path) = prepare_native_file_object_write(object, durability)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        sync_native_file_for_durability(&file, durability)?;
    }
    #[cfg(test)]
    crate::storage::fault_injection::check(
        crate::storage::fault_injection::StorageFaultPoint::ObjectPublish,
        Some(object.kind()),
        object.path(),
    )?;
    fs::rename(&tmp_path, &path)?;
    if requires_parent_dir_sync_after_rename(durability) {
        sync_native_file_parent_directory_after_rename(&path)?;
    }

    Ok(())
}

pub(in crate::storage) fn delete_native_file_object(object: &StorageObjectId) -> Result<()> {
    require_native_file_object_delete(object)?;

    #[cfg(test)]
    crate::storage::fault_injection::check(
        crate::storage::fault_injection::StorageFaultPoint::ObjectDelete,
        Some(object.kind()),
        object.path(),
    )?;

    match fs::remove_file(object.path()) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::Io(error)),
    }
}

pub(in crate::storage) fn publish_manifest_to_native_file(
    object: &StorageObjectId,
    bytes: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    let (path, tmp_path) = prepare_native_file_manifest_publish(object, durability)?;
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        sync_native_file_for_durability(&file, durability)?;
    }
    #[cfg(test)]
    crate::storage::fault_injection::check(
        crate::storage::fault_injection::StorageFaultPoint::ManifestPublish,
        Some(object.kind()),
        object.path(),
    )?;
    fs::rename(&tmp_path, &path)?;
    if requires_parent_dir_sync_after_rename(durability) {
        sync_native_file_parent_directory_after_rename(&path)?;
    }

    Ok(())
}

pub(in crate::storage) fn sync_native_file_for_durability(
    file: &File,
    durability: DurabilityMode,
) -> Result<()> {
    match durability {
        DurabilityMode::Buffered => Ok(()),
        // This path treats `Flush` as a data sync (it publishes a file whose
        // bytes must be on disk before the rename is made durable).
        DurabilityMode::Flush | DurabilityMode::SyncData => {
            crate::durability::sync_file_for_durability(file, DurabilityMode::SyncData)
        }
        DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict => {
            crate::durability::sync_file_for_durability(file, durability)
        }
    }
}
