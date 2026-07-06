use super::*;

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

#[must_use]
pub(crate) fn object_wal_commit_path(db_path: &Path, epoch: u64, sequence: Sequence) -> PathBuf {
    let width = OBJECT_WAL_SEQUENCE_DIGITS;
    db_path.join(format!(
        "{OBJECT_WAL_FILE_PREFIX}{epoch:0width$}{OBJECT_WAL_COMMIT_MARKER}{:0width$}{OBJECT_WAL_FILE_SUFFIX}",
        sequence.get()
    ))
}

#[must_use]
pub(crate) fn object_wal_rewrite_path(db_path: &Path, epoch: u64, sequence: Sequence) -> PathBuf {
    let width = OBJECT_WAL_SEQUENCE_DIGITS;
    db_path.join(format!(
        "{OBJECT_WAL_REWRITE_PREFIX}{epoch:0width$}{OBJECT_WAL_REWRITE_MARKER}{:0width$}{OBJECT_WAL_FILE_SUFFIX}",
        sequence.get()
    ))
}

#[cfg(test)]
pub(crate) fn read_all_batches(db_path: &Path) -> Result<Vec<WalBatch>> {
    read_all_batches_after(db_path, Sequence::ZERO)
}

#[cfg(test)]
pub(super) fn read_all_batches_after(
    db_path: &Path,
    replay_floor: Sequence,
) -> Result<Vec<WalBatch>> {
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
    let confirmed_sequence = read_confirmed_wal_marker_with_backend(backend, path)?;
    let Some(bytes) = read_wal_object_with_backend(backend, path)? else {
        validate_confirmed_wal_coverage(path, replay_floor, confirmed_sequence, &[])?;
        return Ok(Vec::new());
    };
    let batches = decode_frames_after(bytes.as_ref(), replay_floor)?;
    validate_confirmed_wal_coverage(path, replay_floor, confirmed_sequence, &batches)?;
    Ok(batches)
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
    let confirmed_sequence = read_confirmed_wal_marker_with_backend_async(backend, path).await?;
    let Some(bytes) = read_wal_object_with_backend_async(backend, path).await? else {
        validate_confirmed_wal_coverage(path, replay_floor, confirmed_sequence, &[])?;
        return Ok(Vec::new());
    };
    let batches = decode_frames_after(bytes.as_ref(), replay_floor)?;
    validate_confirmed_wal_coverage(path, replay_floor, confirmed_sequence, &batches)?;
    Ok(batches)
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
pub(crate) async fn read_object_wal_batches_after_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    replay_floor: Sequence,
) -> Result<Vec<WalBatch>>
where
    B: StorageObjectListBackend + StorageObjectReadBackend,
{
    let mut batches = Vec::new();
    for path in discover_object_wal_paths_with_backend_async(backend, db_path).await? {
        let Some(sequence) = object_wal_sequence_from_path(&path)? else {
            continue;
        };
        if sequence <= replay_floor {
            continue;
        }
        let object_batches =
            read_batches_after_with_backend_async(backend, &path, replay_floor).await?;
        for batch in object_batches {
            if batch.sequence != sequence {
                return Err(Error::Corruption {
                    message: format!(
                        "object WAL path {} contains commit sequence {}",
                        path.display(),
                        batch.sequence.get()
                    ),
                });
            }
            batches.push(batch);
        }
    }
    validate_wal_stream_order(&batches)?;
    Ok(batches)
}

pub(crate) async fn delete_object_wal_at_or_below_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    replay_floor: Sequence,
) -> Result<()>
where
    B: StorageObjectDeleteBackend + StorageObjectListBackend,
{
    for path in discover_object_wal_paths_with_backend_async(backend, db_path).await? {
        let Some(sequence) = object_wal_sequence_from_path(&path)? else {
            continue;
        };
        if sequence <= replay_floor {
            backend.delete_object(wal_storage_object(&path)).await?;
        }
    }
    Ok(())
}

pub(super) async fn discover_object_wal_paths_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
) -> Result<Vec<PathBuf>>
where
    B: StorageObjectListBackend,
{
    let request = StorageObjectListRequest::native_file(StorageObjectKind::Wal, db_path)
        .with_file_extension("trinewal");
    let mut paths = Vec::new();
    for object in backend.list_objects(request).await? {
        let path = object.path().to_path_buf();
        if object_wal_sequence_from_path(&path)?.is_some() {
            paths.push(path);
        }
    }
    paths.sort_unstable();
    Ok(paths)
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

pub(super) fn wal_rewrite_tmp_path(path: &Path) -> PathBuf {
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

pub(super) fn open_wal_append_object_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<NativeFileAppendObject> {
    backend.capabilities().require(StorageCapability::Append)?;
    backend.open_append_blocking(wal_storage_object(path))
}

pub(super) async fn open_wal_append_object_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<B::AppendObject>
where
    B: StorageAppendBackend,
{
    backend.capabilities().require(StorageCapability::Append)?;
    backend.open_append(wal_storage_object(path)).await
}

pub(super) fn read_wal_object_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<Option<Arc<[u8]>>> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectRead)?;
    backend.read_object_bytes_blocking(wal_storage_object(path))
}

pub(super) async fn read_wal_object_with_backend_async<B>(
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

pub(super) fn write_confirmed_wal_marker_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
    sequence: Sequence,
    durability: DurabilityMode,
) -> Result<()> {
    let marker_path = wal_confirmed_marker_path(path)?
        .ok_or_else(|| invalid_wal("confirmed WAL marker needs a shard WAL path"))?;
    backend
        .capabilities()
        .require(StorageCapability::ObjectWrite)?;
    backend.write_object_blocking(
        wal_storage_object(&marker_path),
        encode_confirmed_wal_marker(sequence),
        durability,
    )
}

pub(super) fn read_confirmed_wal_marker_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<Option<Sequence>> {
    let Some(marker_path) = wal_confirmed_marker_path(path)? else {
        return Ok(None);
    };
    let Some(bytes) = backend.read_object_bytes_blocking(wal_storage_object(&marker_path))? else {
        return Ok(None);
    };
    decode_confirmed_wal_marker(bytes.as_ref()).map(Some)
}

pub(super) fn delete_confirmed_wal_marker_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    let Some(marker_path) = wal_confirmed_marker_path(path)? else {
        return Ok(());
    };
    backend.delete_object_blocking(wal_storage_object(&marker_path))
}

pub(super) async fn read_confirmed_wal_marker_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<Option<Sequence>>
where
    B: StorageObjectReadBackend,
{
    let Some(marker_path) = wal_confirmed_marker_path(path)? else {
        return Ok(None);
    };
    let Some(bytes) = backend
        .read_object_bytes(wal_storage_object(&marker_path))
        .await?
    else {
        return Ok(None);
    };
    decode_confirmed_wal_marker(bytes.as_ref()).map(Some)
}

pub(super) fn validate_confirmed_wal_coverage(
    path: &Path,
    replay_floor: Sequence,
    confirmed_sequence: Option<Sequence>,
    batches: &[WalBatch],
) -> Result<()> {
    let Some(confirmed_sequence) = confirmed_sequence else {
        return Ok(());
    };
    if confirmed_sequence <= replay_floor {
        return Ok(());
    }
    let last_sequence = batches
        .iter()
        .map(|batch| batch.sequence)
        .max()
        .unwrap_or(replay_floor);
    if last_sequence >= confirmed_sequence {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "WAL {} ended before confirmed sequence {}",
            path.display(),
            confirmed_sequence.get()
        ),
    })
}

pub(super) fn wal_confirmed_marker_path(path: &Path) -> Result<Option<PathBuf>> {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(Error::Corruption {
            message: format!("WAL file name is not valid UTF-8: {}", path.display()),
        });
    };
    let Some(shard_index) = wal_shard_index_from_file_name(file_name)? else {
        return Ok(None);
    };
    let width = WAL_SHARD_FILE_DIGITS;
    Ok(Some(path.with_file_name(format!(
        "{WAL_CONFIRMED_FILE_PREFIX}{shard_index:0width$}"
    ))))
}

pub(super) fn encode_confirmed_wal_marker(sequence: Sequence) -> Arc<[u8]> {
    let mut bytes = Vec::with_capacity(WAL_CONFIRMED_LEN);
    bytes.extend_from_slice(&WAL_CONFIRMED_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&WAL_CONFIRMED_VERSION.to_le_bytes());
    bytes.extend_from_slice(&sequence.get().to_le_bytes());
    let marker_checksum = checksum(&bytes);
    bytes.extend_from_slice(&marker_checksum.to_le_bytes());
    Arc::from(bytes.into_boxed_slice())
}

pub(super) fn decode_confirmed_wal_marker(bytes: &[u8]) -> Result<Sequence> {
    if bytes.len() != WAL_CONFIRMED_LEN {
        return Err(invalid_wal("confirmed WAL marker length mismatch"));
    }
    let magic = read_u32_at(bytes, 0)?;
    let version = read_u16_at(bytes, 4)?;
    let sequence = read_u64_at(bytes, 6)?;
    let actual_checksum = read_u32_at(bytes, 14)?;
    if magic != WAL_CONFIRMED_MAGIC {
        return Err(invalid_wal("confirmed WAL marker magic mismatch"));
    }
    if version != WAL_CONFIRMED_VERSION {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported confirmed WAL marker version {version}"),
        });
    }
    if checksum(&bytes[..14]) != actual_checksum {
        return Err(invalid_wal("confirmed WAL marker checksum mismatch"));
    }
    Ok(Sequence::new(sequence))
}

#[allow(dead_code)]
pub(super) fn rewrite_wal_object_with_backend(
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

pub(super) async fn rewrite_wal_object_with_backend_async<B>(
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

pub(super) fn wal_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::Wal, path)
}
