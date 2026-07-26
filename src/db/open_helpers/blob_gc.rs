use super::{
    Arc, BTreeMap, BTreeSet, BlobGcRewriteRecord, BlobGcRewriteTable, BlobLevelMergePolicy,
    BlockingStorageReadBackend, BlockingStorageReadObject, BucketOptions, DbStats, DurabilityMode,
    Error, LsmCompactionInput, LsmCompactionOutput, LsmCompactionTablePayload, ManifestState,
    NamedCompactionOutput, NativeFileBackend, Path, Result, StorageCapability, StorageObjectId,
    StorageObjectKind, StorageReadBackend, ValueRef, blob, table,
};

pub(in crate::db) fn table_file_bytes(
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

pub(in crate::db) fn add_obsolete_blob_stats(
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

pub(in crate::db) fn storage_object_file_bytes(
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

pub(in crate::db) fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(in crate::db) fn referenced_table_file_ids(
    manifest: &ManifestState,
) -> BTreeSet<table::TableId> {
    manifest
        .tables()
        .values()
        .flat_map(|tables| tables.iter().map(|properties| properties.id))
        .collect()
}

pub(in crate::db) fn referenced_blob_file_ids_from_manifest(
    manifest: &ManifestState,
) -> BTreeSet<u64> {
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

/// Returns only pending blob files that no published table can still reach.
///
/// Every cleanup adapter uses this policy so native sync, native async, and
/// browser paths cannot disagree about the reachability safety rule.
pub(in crate::db) fn deletable_pending_blob_file_ids(manifest: &ManifestState) -> Vec<u64> {
    let referenced = referenced_blob_file_ids_from_manifest(manifest);
    manifest
        .pending_blob_deletions()
        .keys()
        .copied()
        .filter(|file_id| crate::invariants::gc_delete_allowed(true, referenced.contains(file_id)))
        .collect()
}

pub(in crate::db) fn allowed_blob_file_ids_from_manifest(
    manifest: &ManifestState,
) -> BTreeSet<u64> {
    let mut file_ids = referenced_blob_file_ids_from_manifest(manifest);
    file_ids.extend(manifest.pending_blob_deletions().keys().copied());
    file_ids
}

pub(in crate::db) fn should_rewrite_blob_indexes_for_compaction(
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

pub(in crate::db) fn payloads_have_blob_references(payloads: &[LsmCompactionTablePayload]) -> bool {
    payloads.iter().any(|payload| {
        payload
            .point_records
            .iter()
            .any(|(_, value)| matches!(value, Some(ValueRef::BlobIndex(_))))
    })
}

pub(in crate::db) fn input_blob_bytes_by_file(input: &LsmCompactionInput) -> BTreeMap<u64, u64> {
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

pub(in crate::db) fn payload_blob_bytes_by_file(
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

pub(in crate::db) fn blob_reference_bytes(value: Option<&ValueRef>) -> Option<(u64, u64)> {
    match value {
        Some(ValueRef::BlobIndex(index)) => Some((index.file_id, index.encoded_len)),
        Some(ValueRef::Inline(_)) | None => None,
    }
}

pub(in crate::db) fn blob_gc_table_write_options(
    options: &BucketOptions,
) -> table::TableWriteOptions {
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

pub(in crate::db) fn blob_gc_blob_records(
    records: &[BlobGcRewriteRecord],
) -> Vec<blob::BlobRecord> {
    records
        .iter()
        .map(|record| blob::BlobRecord {
            internal_key: record.internal_key.clone(),
            value: record.value.clone(),
            compression: record.compression,
        })
        .collect()
}

pub(in crate::db) fn apply_blob_gc_indexes(
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

pub(in crate::db) fn write_blob_gc_replacement_tables(
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
