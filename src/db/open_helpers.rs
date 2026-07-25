use super::{
    Arc, BTreeMap, BTreeSet, BlobGcRewriteRecord, BlobGcRewriteTable, BlobLevelMergePolicy,
    BlockingStorageDirectoryCreateBackend, BlockingStorageDirectoryListBackend,
    BlockingStorageDirectorySyncBackend, BlockingStorageObjectDeleteBackend,
    BlockingStorageReadBackend, BlockingStorageReadObject, BucketOptions, CancellationToken,
    DEFAULT_BUCKET_NAME, Db, DbInner, DbOptions, DbStats, DurabilityMode, Error,
    FailOnCorruptionPolicy, FilterPolicy, HostStorageBackend, LsmCompactionInput,
    LsmCompactionOutput, LsmCompactionTablePayload, LsmTree, MaintenanceCoordinator, ManifestState,
    ManifestStore, Mutex, NamedCompactionOutput, NativeFileBackend, Ordering, Path,
    PrefixFilterPolicy, Result, Sequence, SnapshotTracker, StorageCapability,
    StorageDirectoryCreateBackend, StorageDirectoryFile, StorageDirectoryId,
    StorageDirectoryListBackend, StorageDirectorySyncBackend, StorageManifestReadBackend,
    StorageMode, StorageObjectDeleteBackend, StorageObjectId, StorageObjectKind,
    StorageObjectListBackend, StorageObjectReadBackend, StorageReadBackend, Table, ValueRef,
    WalBatch, Weak, blob, compaction, io, manifest, record_maintenance_success, recovery, runtime,
    table,
};

pub(super) fn validate_options(options: &DbOptions) -> Result<()> {
    if let StorageMode::HostPersistent {
        backend: HostStorageBackend::Browser { .. },
    } = &options.storage_mode
    {
        return Err(Error::unsupported_backend(
            "browser persistent storage backend",
        ));
    }
    validate_common_options(options)
}

pub(super) fn object_store_committed_wal_batches(
    batches: Vec<WalBatch>,
    replay_floor: Sequence,
    committed_sequence: Sequence,
) -> Result<Vec<WalBatch>> {
    let batches = batches
        .into_iter()
        .filter(|batch| batch.sequence <= committed_sequence)
        .collect::<Vec<_>>();
    validate_object_store_committed_wal_coverage(&batches, replay_floor, committed_sequence)?;
    Ok(batches)
}

pub(super) fn validate_object_store_committed_wal_coverage(
    batches: &[WalBatch],
    replay_floor: Sequence,
    committed_sequence: Sequence,
) -> Result<()> {
    if committed_sequence <= replay_floor {
        return Ok(());
    }

    let mut expected = replay_floor
        .get()
        .checked_add(1)
        .map(Sequence::new)
        .ok_or_else(|| Error::Corruption {
            message: "object-store WAL replay floor cannot advance past u64::MAX".to_owned(),
        })?;
    for batch in batches {
        if batch.sequence != expected {
            return Err(Error::Corruption {
                message: format!(
                    "object-store WAL is missing confirmed commit sequence {} before head {}",
                    expected.get(),
                    committed_sequence.get()
                ),
            });
        }
        if batch.sequence == committed_sequence {
            return Ok(());
        }
        expected = expected
            .get()
            .checked_add(1)
            .map(Sequence::new)
            .ok_or_else(|| Error::Corruption {
                message: "object-store WAL committed sequence cannot advance past u64::MAX"
                    .to_owned(),
            })?;
    }

    Err(Error::Corruption {
        message: format!(
            "object-store WAL ended before confirmed head {}",
            committed_sequence.get()
        ),
    })
}

pub(super) fn validate_common_options(options: &DbOptions) -> Result<()> {
    runtime::validate_runtime_options(
        options.runtime,
        &options.storage_mode,
        options.read_only,
        options.background_worker_count,
    )?;
    validate_bucket_options(&options.default_bucket_options)?;
    if options.write_buffer_bytes == 0 {
        return Err(Error::invalid_options("write buffer must be non-zero"));
    }
    if options.max_immutable_memtables == 0 {
        return Err(Error::invalid_options(
            "max immutable memtables must be non-zero",
        ));
    }
    if options.target_table_bytes == 0 {
        return Err(Error::invalid_options("target table size must be non-zero"));
    }
    if options.level_size_multiplier < 2 {
        return Err(Error::invalid_options("level size multiplier must be >= 2"));
    }
    if options.max_l0_files == 0 {
        return Err(Error::invalid_options("max L0 files must be non-zero"));
    }
    if options.max_key_bytes == 0 {
        return Err(Error::invalid_options("max key size must be non-zero"));
    }
    if options.max_key_bytes > DbOptions::MAX_WRITE_FIELD_BYTES {
        return Err(Error::invalid_options(format!(
            "max key size {} exceeds maximum {}",
            options.max_key_bytes,
            DbOptions::MAX_WRITE_FIELD_BYTES
        )));
    }
    if options.max_value_bytes == 0 {
        return Err(Error::invalid_options("max value size must be non-zero"));
    }
    if options.max_value_bytes > DbOptions::MAX_WRITE_FIELD_BYTES {
        return Err(Error::invalid_options(format!(
            "max value size {} exceeds maximum {}",
            options.max_value_bytes,
            DbOptions::MAX_WRITE_FIELD_BYTES
        )));
    }
    if options.keep_last_read_versions == 0 {
        return Err(Error::invalid_options(
            "keep_last_read_versions must be non-zero",
        ));
    }
    let blob_gc_ratio = options.blob_gc_discardable_ratio.millionths();
    if blob_gc_ratio == 0 || blob_gc_ratio > 1_000_000 {
        return Err(Error::invalid_options(
            "blob GC discardable ratio must be in (0.0, 1.0]",
        ));
    }
    if options.blob_gc_enabled && options.blob_gc_min_file_bytes == 0 {
        return Err(Error::invalid_options(
            "blob GC minimum file size must be non-zero",
        ));
    }

    Ok(())
}

pub(super) fn validate_checkpoint_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::invalid_options("checkpoint name cannot be empty"));
    }
    if name.starts_with(crate::bucket::INTERNAL_BUCKET_PREFIX) {
        return Err(Error::invalid_options(
            "checkpoint name uses Trine's reserved internal namespace",
        ));
    }
    if name.len() > 1_024 {
        return Err(Error::invalid_options(
            "checkpoint name exceeds the 1024-byte limit",
        ));
    }
    Ok(())
}

pub(super) fn require_internal_checkpoint_name(name: &str) -> Result<()> {
    if name.starts_with(crate::bucket::INTERNAL_BUCKET_PREFIX) {
        Ok(())
    } else {
        Err(Error::Corruption {
            message: format!("internal checkpoint name {name:?} is outside the reserved namespace"),
        })
    }
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(super) fn background_worker_loop(
    inner: &Weak<DbInner>,
    maintenance: &MaintenanceCoordinator,
    runtime_shutdown: &CancellationToken,
) {
    while let Some(request) = maintenance.wait_for_request() {
        if runtime_shutdown.is_cancelled() {
            break;
        }
        let Some(inner) = inner.upgrade() else {
            break;
        };
        if inner.closed.load(Ordering::Acquire) || runtime_shutdown.is_cancelled() {
            break;
        }

        let db = Db {
            inner,
            counts_as_user_handle: false,
        };
        match db.run_background_maintenance(request) {
            Ok(()) => record_maintenance_success(maintenance),
            Err(Error::Closed) => break,
            Err(error) => maintenance.record_error(error),
        }
    }
}

pub(super) fn acquire_persistent_process_lock(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
) -> Result<Option<recovery::ProcessLock>> {
    if options.read_only {
        return Ok(None);
    }
    recovery::ProcessLock::acquire_with_backend(backend, db_path).map(Some)
}

pub(super) async fn acquire_persistent_process_lock_async(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
) -> Result<Option<recovery::ProcessLock>> {
    if options.read_only {
        return Ok(None);
    }
    recovery::ProcessLock::acquire_with_backend_async(backend, db_path)
        .await
        .map(Some)
}

pub(super) fn list_persistent_directory_files(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<Vec<StorageDirectoryFile>> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    backend.list_directory_files_blocking(StorageDirectoryId::native_file(db_path))
}

pub(super) async fn list_persistent_directory_files_async(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<Vec<StorageDirectoryFile>> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    backend
        .list_directory_files(StorageDirectoryId::native_file(db_path))
        .await
}

pub(super) fn repair_safe_temporary_files_for_open(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
    directory_files: &[StorageDirectoryFile],
) -> Result<()> {
    let policy = if options.read_only {
        FailOnCorruptionPolicy::FailClosed
    } else {
        options.fail_on_corruption
    };
    recovery::repair_safe_temporary_files_from_directory_files_with_backend(
        backend,
        db_path,
        policy,
        directory_files,
    )?;
    Ok(())
}

pub(super) async fn repair_safe_temporary_files_for_open_from_directory_files_async(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
    directory_files: &[StorageDirectoryFile],
) -> Result<()> {
    let policy = if options.read_only {
        FailOnCorruptionPolicy::FailClosed
    } else {
        options.fail_on_corruption
    };
    recovery::repair_safe_temporary_files_from_directory_files_with_backend_async(
        backend,
        db_path,
        policy,
        directory_files,
    )
    .await?;
    Ok(())
}

pub(super) fn run_persistent_recovery_checks(
    backend: &NativeFileBackend,
    db_path: &Path,
    manifest: &ManifestState,
    directory_files: &[StorageDirectoryFile],
) -> Result<()> {
    let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest);
    let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest);
    recovery::fail_on_missing_referenced_blob_files_with_backend(
        backend,
        db_path,
        &referenced_blob_ids,
    )?;
    recovery::fail_on_invalid_referenced_blob_files_with_backend(backend, db_path, manifest)?;
    recovery::fail_on_unreferenced_storage_files_from_directory_files(
        db_path,
        directory_files,
        &referenced_table_file_ids(manifest),
        &allowed_blob_ids,
    )
}

pub(super) async fn run_persistent_recovery_checks_from_directory_files_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
    directory_files: &[StorageDirectoryFile],
) -> Result<()>
where
    B: StorageReadBackend + StorageObjectReadBackend,
{
    let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest);
    let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest);
    recovery::fail_on_missing_referenced_blob_files_with_backend_async(
        backend,
        db_path,
        &referenced_blob_ids,
    )
    .await?;
    recovery::fail_on_invalid_referenced_blob_files_with_backend_async(backend, db_path, manifest)
        .await?;
    recovery::fail_on_unreferenced_storage_files_from_directory_files(
        db_path,
        directory_files,
        &referenced_table_file_ids(manifest),
        &allowed_blob_ids,
    )
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    allow(dead_code)
)]
pub(super) async fn run_persistent_recovery_checks_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<()>
where
    B: StorageReadBackend
        + StorageObjectReadBackend
        + StorageObjectListBackend
        + StorageDirectoryListBackend,
{
    let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest);
    let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest);
    recovery::fail_on_missing_referenced_blob_files_with_backend_async(
        backend,
        db_path,
        &referenced_blob_ids,
    )
    .await?;
    recovery::fail_on_invalid_referenced_blob_files_with_backend_async(backend, db_path, manifest)
        .await?;
    recovery::fail_on_unreferenced_storage_files_with_backend_async(
        backend,
        db_path,
        &referenced_table_file_ids(manifest),
        &allowed_blob_ids,
    )
    .await
}

pub(super) fn buckets_from_manifest(
    backend: &NativeFileBackend,
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<BTreeMap<String, Arc<LsmTree>>> {
    let mut buckets = BTreeMap::new();

    for (name, options) in manifest.buckets() {
        validate_bucket_options(options)?;
        let mut tables = Vec::new();
        for properties in manifest.tables().get(name).into_iter().flatten() {
            let table_path = table::table_path(db_path, properties.id);
            let table = table::read_table_with_backend(backend, &table_path)?
                .with_manifest_properties(properties)?;
            tables.push(Arc::new(table));
        }

        buckets.insert(
            name.clone(),
            Arc::new(LsmTree::new(options.clone(), tables)?),
        );
    }

    Ok(buckets)
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    allow(dead_code)
)]
pub(super) async fn buckets_from_manifest_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
    inline_blob_values: bool,
) -> Result<BTreeMap<String, Arc<LsmTree>>>
where
    B: StorageReadBackend + StorageObjectReadBackend,
{
    let mut buckets = BTreeMap::new();

    for (name, options) in manifest.buckets() {
        validate_bucket_options(options)?;
        let mut tables = Vec::new();
        for properties in manifest.tables().get(name).into_iter().flatten() {
            let table_path = table::table_path(db_path, properties.id);
            let mut table = table::read_table_with_backend_async(backend, &table_path)
                .await?
                .with_manifest_properties(properties)?;
            if inline_blob_values {
                table =
                    table::inline_blob_values_with_backend_async(backend, db_path, table).await?;
            }
            tables.push(Arc::new(table));
        }

        buckets.insert(
            name.clone(),
            Arc::new(LsmTree::new(options.clone(), tables)?),
        );
    }

    Ok(buckets)
}

#[allow(dead_code)]
pub(super) async fn read_manifest_or_empty_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<ManifestState>
where
    B: StorageManifestReadBackend,
{
    match manifest::read_manifest_with_backend_async(backend, path).await {
        Ok(manifest) => Ok(manifest),
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            Ok(ManifestState::empty())
        }
        Err(error) => Err(error),
    }
}

pub(super) fn ensure_default_bucket_in_manifest(
    manifest: &mut ManifestStore,
    options: &DbOptions,
) -> Result<()> {
    if manifest.state().buckets().contains_key(DEFAULT_BUCKET_NAME) || options.read_only {
        return Ok(());
    }

    manifest.create_bucket(
        DEFAULT_BUCKET_NAME.to_owned(),
        options.default_bucket_options.clone(),
    )
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    allow(dead_code)
)]
pub(super) async fn ensure_default_bucket_in_manifest_async(
    manifest: &mut ManifestStore,
    options: &DbOptions,
) -> Result<()> {
    if manifest.state().buckets().contains_key(DEFAULT_BUCKET_NAME) || options.read_only {
        return Ok(());
    }

    manifest
        .create_bucket_async(
            DEFAULT_BUCKET_NAME.to_owned(),
            options.default_bucket_options.clone(),
        )
        .await
}

pub(super) fn ensure_default_bucket_loaded(
    buckets: &mut BTreeMap<String, Arc<LsmTree>>,
    options: &DbOptions,
) -> Result<()> {
    if buckets.contains_key(DEFAULT_BUCKET_NAME) {
        return Ok(());
    }

    // Read-only opens cannot publish a missing manifest entry, but the public
    // API still treats the default bucket as always present.
    buckets.insert(
        DEFAULT_BUCKET_NAME.to_owned(),
        Arc::new(LsmTree::new(
            options.default_bucket_options.clone(),
            Vec::new(),
        )?),
    );
    Ok(())
}

pub(super) fn table_file_bytes(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_id: table::TableId,
) -> u64 {
    storage_object_file_bytes(
        backend,
        StorageObjectKind::Table,
        &table::table_path(db_path, table_id),
    )
}

pub(super) fn add_obsolete_blob_stats(
    backend: &NativeFileBackend,
    db_path: &Path,
    live_blob_bytes_by_file: &BTreeMap<u64, u64>,
    stats: &mut DbStats,
) {
    for (file_id, live_bytes) in live_blob_bytes_by_file {
        let Ok(properties) =
            blob::read_blob_file_properties_with_backend(backend, db_path, *file_id)
        else {
            continue;
        };
        if properties.encoded_bytes > *live_bytes {
            stats.stale_blob_files = stats.stale_blob_files.saturating_add(1);
            stats.stale_blob_bytes = stats
                .stale_blob_bytes
                .saturating_add(properties.encoded_bytes - *live_bytes);
        }
    }

    let Ok(blob_file_ids) = blob::list_blob_file_ids_with_backend(backend, db_path) else {
        return;
    };

    for file_id in blob_file_ids {
        if live_blob_bytes_by_file.contains_key(&file_id) {
            continue;
        }
        stats.obsolete_blob_files += 1;
        let bytes = storage_object_file_bytes(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, file_id),
        );
        stats.obsolete_blob_bytes = stats.obsolete_blob_bytes.saturating_add(bytes);
        stats.stale_blob_files = stats.stale_blob_files.saturating_add(1);
        stats.stale_blob_bytes = stats.stale_blob_bytes.saturating_add(bytes);
    }
}

pub(super) fn storage_object_file_bytes(
    backend: &NativeFileBackend,
    kind: StorageObjectKind,
    path: &Path,
) -> u64 {
    if backend
        .capabilities()
        .require(StorageCapability::RandomRead)
        .is_err()
    {
        return 0;
    }

    let object_id = StorageObjectId::native_file(kind, path);
    let Ok(object) = backend.open_read_blocking(object_id) else {
        return 0;
    };

    object.len_blocking().unwrap_or(0)
}

pub(super) fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn referenced_table_file_ids(manifest: &ManifestState) -> BTreeSet<table::TableId> {
    manifest
        .tables()
        .values()
        .flat_map(|tables| tables.iter().map(|properties| properties.id))
        .collect()
}

pub(super) fn referenced_blob_file_ids_from_manifest(manifest: &ManifestState) -> BTreeSet<u64> {
    manifest
        .tables()
        .values()
        .flat_map(|tables| {
            tables
                .iter()
                .flat_map(|properties| properties.blob_file_ids.iter().copied())
        })
        .collect()
}

pub(super) fn allowed_blob_file_ids_from_manifest(manifest: &ManifestState) -> BTreeSet<u64> {
    let mut file_ids = referenced_blob_file_ids_from_manifest(manifest);
    file_ids.extend(manifest.pending_blob_deletions().keys().copied());
    file_ids
}

pub(super) fn should_rewrite_blob_indexes_for_compaction(
    input: &LsmCompactionInput,
    payloads: &[LsmCompactionTablePayload],
    policy: BlobLevelMergePolicy,
) -> bool {
    match policy {
        BlobLevelMergePolicy::Disabled => false,
        BlobLevelMergePolicy::Always => payloads_have_blob_references(payloads),
        BlobLevelMergePolicy::Auto => {
            let output_bytes = payload_blob_bytes_by_file(payloads);
            if output_bytes.is_empty() {
                return false;
            }
            if output_bytes.len() > 1 {
                return true;
            }

            let input_bytes = input_blob_bytes_by_file(input);
            input_bytes.iter().any(|(file_id, input_bytes)| {
                let output_bytes = output_bytes.get(file_id).copied().unwrap_or(0);
                *input_bytes > output_bytes
            })
        }
    }
}

pub(super) fn payloads_have_blob_references(payloads: &[LsmCompactionTablePayload]) -> bool {
    payloads.iter().any(|payload| {
        payload
            .point_records
            .iter()
            .any(|(_, value)| matches!(value, Some(ValueRef::BlobIndex(_) | ValueRef::Blob { .. })))
    })
}

pub(super) fn input_blob_bytes_by_file(input: &LsmCompactionInput) -> BTreeMap<u64, u64> {
    let mut bytes_by_file = BTreeMap::new();
    for table in &input.input_tables {
        for reference in table.properties().blob_references() {
            bytes_by_file
                .entry(reference.file_id)
                .and_modify(|bytes: &mut u64| {
                    *bytes = bytes.saturating_add(reference.referenced_bytes);
                })
                .or_insert(reference.referenced_bytes);
        }
    }
    bytes_by_file
}

pub(super) fn payload_blob_bytes_by_file(
    payloads: &[LsmCompactionTablePayload],
) -> BTreeMap<u64, u64> {
    let mut bytes_by_file = BTreeMap::new();
    for payload in payloads {
        for (_, value) in &payload.point_records {
            let Some((file_id, referenced_bytes)) = blob_reference_bytes(value.as_ref()) else {
                continue;
            };
            bytes_by_file
                .entry(file_id)
                .and_modify(|bytes: &mut u64| {
                    *bytes = bytes.saturating_add(referenced_bytes);
                })
                .or_insert(referenced_bytes);
        }
    }
    bytes_by_file
}

pub(super) fn blob_reference_bytes(value: Option<&ValueRef>) -> Option<(u64, u64)> {
    match value {
        Some(ValueRef::BlobIndex(index)) => Some((index.file_id, index.encoded_len)),
        Some(ValueRef::Blob { file_id, len, .. }) => Some((*file_id, *len)),
        Some(ValueRef::Inline(_)) | None => None,
    }
}

pub(super) fn blob_gc_table_write_options(options: &BucketOptions) -> table::TableWriteOptions {
    table::TableWriteOptions {
        codec: options.compression.codec_id(),
        block_bytes: options.block_bytes,
        filter_policy: options.filter_policy,
        prefix_extractor: options.prefix_extractor.clone(),
        prefix_filter_policy: options.prefix_filter_policy,
        filter_depth_curve: options.filter_depth_curve,
        blob_threshold_bytes: usize::MAX,
        rewrite_blob_indexes: false,
    }
}

pub(super) fn blob_gc_blob_records(records: &[BlobGcRewriteRecord]) -> Vec<blob::BlobRecord> {
    records
        .iter()
        .map(|record| blob::BlobRecord {
            internal_key: record.internal_key.clone(),
            value: record.value.clone(),
            compression: record.compression,
        })
        .collect()
}

pub(super) fn apply_blob_gc_indexes(
    tables: &mut [BlobGcRewriteTable],
    records: Vec<BlobGcRewriteRecord>,
    indexes: Vec<blob::BlobIndex>,
) -> Result<u64> {
    if records.len() != indexes.len() {
        return Err(Error::Corruption {
            message: "blob GC rewrite record count does not match blob indexes".to_owned(),
        });
    }

    let output_bytes = indexes.iter().fold(0_u64, |bytes, index| {
        bytes.saturating_add(index.encoded_len)
    });
    for (rewrite, index) in records.into_iter().zip(indexes) {
        let record = tables
            .get_mut(rewrite.table_index)
            .and_then(|table| table.point_records.get_mut(rewrite.record_index))
            .ok_or_else(|| Error::Corruption {
                message: "blob GC rewrite record position is invalid".to_owned(),
            })?;
        record.value = Some(ValueRef::BlobIndex(index));
    }

    Ok(output_bytes)
}

pub(super) fn write_blob_gc_replacement_tables(
    backend: &NativeFileBackend,
    db_path: &Path,
    tables: Vec<BlobGcRewriteTable>,
    durability: DurabilityMode,
) -> Result<Vec<NamedCompactionOutput>> {
    let mut outputs = Vec::with_capacity(tables.len());
    for rewrite_table in tables {
        let table_path = table::table_path(db_path, rewrite_table.output_table_id);
        let point_records = rewrite_table
            .point_records
            .iter()
            .map(|record| (record.internal_key.clone(), record.value.clone()))
            .collect::<Vec<_>>();
        let table = Arc::new(table::write_table_with_backend_with_durability(
            backend,
            &table_path,
            rewrite_table.output_table_id,
            rewrite_table.level,
            &rewrite_table.options,
            &point_records,
            &rewrite_table.range_tombstones,
            durability,
        )?);

        outputs.push(NamedCompactionOutput {
            bucket: rewrite_table.bucket,
            trigger: None,
            output: LsmCompactionOutput {
                input_table_ids: vec![rewrite_table.input_table_id],
                tables: vec![table],
            },
        });
    }

    Ok(outputs)
}

pub(super) fn validate_bucket_options(options: &BucketOptions) -> Result<()> {
    if options.block_bytes == 0 {
        return Err(Error::invalid_options("block size must be non-zero"));
    }
    if matches!(
        options.filter_policy,
        FilterPolicy::Bloom { bits_per_key: 0 }
    ) {
        return Err(Error::invalid_options(
            "bits_per_key must be non-zero for Bloom filters",
        ));
    }
    if matches!(
        options.prefix_filter_policy,
        PrefixFilterPolicy::Bloom { bits_per_prefix: 0 }
    ) {
        return Err(Error::invalid_options(
            "bits_per_prefix must be non-zero for Bloom filters",
        ));
    }
    if options.blob_threshold_bytes == 0 {
        return Err(Error::invalid_options("blob threshold must be non-zero"));
    }

    Ok(())
}

pub(super) fn compaction_options(
    options: &DbOptions,
    local_l0_compaction: bool,
) -> compaction::CompactionOptions {
    compaction::CompactionOptions {
        target_table_bytes: usize_to_u64_saturating(options.target_table_bytes),
        level_size_multiplier: usize_to_u64_saturating(options.level_size_multiplier),
        local_l0_compaction,
    }
}

pub(super) fn validate_batch_len(len: usize) -> Result<()> {
    if len > u32::MAX as usize {
        return Err(Error::InvalidOptions {
            message: "write batch operation count exceeds u32::MAX".to_owned(),
        });
    }

    Ok(())
}

pub(super) fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}

pub(super) fn is_level_layout_compaction_error(error: &Error) -> bool {
    let Error::Corruption { message } = error else {
        return false;
    };
    message.contains("has overlapping tables")
        || message.contains("unbounded table mixed with other tables")
}

pub(super) fn persistent_path_from_options(options: &DbOptions) -> Option<&Path> {
    options.storage_mode.persistent_path()
}

pub(super) fn cleanup_pending_obsolete_table_files(
    backend: &NativeFileBackend,
    db_path: Option<&Path>,
    pending_tables: &Mutex<Vec<Arc<Table>>>,
) -> Result<()> {
    let Some(db_path) = db_path else {
        return Ok(());
    };

    let deletable = take_deletable_obsolete_tables(pending_tables)?;
    if deletable.is_empty() {
        return Ok(());
    }

    let table_ids = deletable
        .iter()
        .map(|table| table.properties().id)
        .collect::<Vec<_>>();
    if let Err(error) = remove_table_files(backend, db_path, &table_ids) {
        pending_tables
            .lock()
            .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?
            .extend(deletable);
        return Err(error);
    }

    Ok(())
}

/// Takes the obsolete tables whose files are safe to delete now: those the
/// cleanup queue is the sole owner of (`Arc::strong_count == 1`). Once a table
/// leaves the current version no new reader can acquire it, so its strong count
/// only falls; a count of 1 proves no in-flight reader still pins the file.
/// Tables still pinned stay queued and are retried on the next pass.
pub(super) fn take_deletable_obsolete_tables(
    pending_tables: &Mutex<Vec<Arc<Table>>>,
) -> Result<Vec<Arc<Table>>> {
    let mut pending = pending_tables
        .lock()
        .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?;
    if pending.is_empty() {
        return Ok(Vec::new());
    }

    let mut deletable = Vec::new();
    let mut retained = Vec::with_capacity(pending.len());
    for table in pending.drain(..) {
        if Arc::strong_count(&table) == 1 {
            deletable.push(table);
        } else {
            retained.push(table);
        }
    }
    *pending = retained;
    Ok(deletable)
}

pub(super) fn cleanup_pending_obsolete_blob_files(
    backend: &NativeFileBackend,
    db_path: Option<&Path>,
    snapshots: &SnapshotTracker,
    manifest: Option<&Mutex<ManifestStore>>,
) -> Result<()> {
    if db_path.is_none() {
        return Ok(());
    }
    let (manifest, pending_file_ids) =
        delete_pending_obsolete_blob_files(backend, db_path, snapshots, manifest)?;
    if pending_file_ids.is_empty() {
        return Ok(());
    }

    manifest
        .lock()
        .map_err(|_| lock_poisoned("manifest store"))?
        .clear_pending_blob_deletions(&pending_file_ids)
}

pub(super) fn delete_pending_obsolete_blob_files<'manifest>(
    backend: &NativeFileBackend,
    db_path: Option<&Path>,
    snapshots: &SnapshotTracker,
    manifest: Option<&'manifest Mutex<ManifestStore>>,
) -> Result<(&'manifest Mutex<ManifestStore>, Vec<u64>)> {
    let Some(db_path) = db_path else {
        return Err(Error::Corruption {
            message: "persistent blob cleanup is missing database path".to_owned(),
        });
    };
    let manifest = manifest.ok_or_else(|| Error::Corruption {
        message: "persistent database is missing manifest store".to_owned(),
    })?;
    if snapshots.active_count() != 0 {
        return Ok((manifest, Vec::new()));
    }

    let pending_file_ids = {
        let manifest = manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?;
        let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest.state());
        // Manifest metadata is the deletion authority. A pending entry that is
        // still referenced is inconsistent, so leave it on disk instead of
        // risking a read-visible blob file.
        manifest
            .state()
            .pending_blob_deletions()
            .keys()
            .copied()
            .filter(|file_id| !referenced_blob_ids.contains(file_id))
            .collect::<Vec<_>>()
    };
    if pending_file_ids.is_empty() {
        return Ok((manifest, Vec::new()));
    }

    for file_id in &pending_file_ids {
        delete_storage_object(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, *file_id),
        )?;
    }

    Ok((manifest, pending_file_ids))
}

pub(super) fn remove_table_files(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    for table_id in table_ids {
        delete_storage_object(
            backend,
            StorageObjectKind::Table,
            &table::table_path(db_path, *table_id),
        )?;
    }

    Ok(())
}

pub(super) fn remove_blob_files(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    for table_id in table_ids {
        delete_storage_object(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, table_id.get()),
        )?;
    }

    Ok(())
}

pub(super) fn delete_storage_object(
    backend: &NativeFileBackend,
    kind: StorageObjectKind,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectDelete)?;
    backend.delete_object_blocking(StorageObjectId::native_file(kind, path))
}

pub(super) async fn delete_storage_object_async(
    backend: &impl StorageObjectDeleteBackend,
    kind: StorageObjectKind,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectDelete)?;
    backend
        .delete_object(StorageObjectId::native_file(kind, path))
        .await
}

pub(super) fn sync_storage_directory_after_renames(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectorySync)?;
    backend.sync_directory_after_renames_blocking(StorageDirectoryId::native_file(path))
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(super) async fn sync_storage_directory_after_renames_async(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectorySync)?;
    backend
        .sync_directory_after_renames(StorageDirectoryId::native_file(path))
        .await
}

pub(super) fn create_storage_directory_all(backend: &NativeFileBackend, path: &Path) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryCreate)?;
    backend.create_directory_all_blocking(StorageDirectoryId::native_file(path))
}

pub(super) async fn create_storage_directory_all_async(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryCreate)?;
    backend
        .create_directory_all(StorageDirectoryId::native_file(path))
        .await
}

pub(super) fn remove_storage_files(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    // A table write uses the table id as the blob file id for large values.
    // Before manifest publish succeeds, both files are unpublished output and
    // can be removed together after a failed flush or compaction attempt.
    remove_table_files(backend, db_path, table_ids)?;
    remove_blob_files(backend, db_path, table_ids)
}

pub(super) async fn remove_storage_files_async(
    backend: &impl StorageObjectDeleteBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    for table_id in table_ids {
        delete_storage_object_async(
            backend,
            StorageObjectKind::Table,
            &table::table_path(db_path, *table_id),
        )
        .await?;
        delete_storage_object_async(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, table_id.get()),
        )
        .await?;
    }

    Ok(())
}
