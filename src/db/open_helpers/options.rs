use super::{
    BucketOptions, DbOptions, Error, FilterPolicy, HostStorageBackend, Path, PrefixFilterPolicy,
    Result, Sequence, StorageMode, WalBatch, compaction, runtime, usize_to_u64_saturating,
};

pub(in crate::db) fn validate_options(options: &DbOptions) -> Result<()> {
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

pub(in crate::db) fn object_store_committed_wal_batches(
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

pub(in crate::db) fn validate_object_store_committed_wal_coverage(
    batches: &[WalBatch],
    replay_floor: Sequence,
    committed_sequence: Sequence,
) -> Result<()> {
    if committed_sequence <= replay_floor {
        return Ok(());
    }

    let mut expected = replay_floor
        .checked_next()
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
        expected = expected.checked_next().ok_or_else(|| Error::Corruption {
            message: "object-store WAL committed sequence cannot advance past u64::MAX".to_owned(),
        })?;
    }

    Err(Error::Corruption {
        message: format!(
            "object-store WAL ended before confirmed head {}",
            committed_sequence.get()
        ),
    })
}

pub(in crate::db) fn validate_common_options(options: &DbOptions) -> Result<()> {
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
    if options.max_key_bytes > DbOptions::MAX_KEY_BYTES {
        return Err(Error::invalid_options(format!(
            "max key size {} exceeds maximum {}",
            options.max_key_bytes,
            DbOptions::MAX_KEY_BYTES
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

pub(in crate::db) fn validate_checkpoint_name(name: &str) -> Result<()> {
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

pub(in crate::db) fn require_internal_checkpoint_name(name: &str) -> Result<()> {
    if name.starts_with(crate::bucket::INTERNAL_BUCKET_PREFIX) {
        Ok(())
    } else {
        Err(Error::Corruption {
            message: format!("internal checkpoint name {name:?} is outside the reserved namespace"),
        })
    }
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(in crate::db) fn validate_bucket_options(options: &BucketOptions) -> Result<()> {
    if options.block_bytes == 0 {
        return Err(Error::invalid_options("block size must be non-zero"));
    }
    if options.block_bytes > crate::limits::MAX_DECODED_BLOCK_BYTES {
        return Err(Error::invalid_options(format!(
            "block size {} exceeds maximum {}",
            options.block_bytes,
            crate::limits::MAX_DECODED_BLOCK_BYTES
        )));
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

pub(in crate::db) fn compaction_options(
    options: &DbOptions,
    local_l0_compaction: bool,
) -> compaction::CompactionOptions {
    compaction::CompactionOptions {
        target_table_bytes: usize_to_u64_saturating(options.target_table_bytes),
        level_size_multiplier: usize_to_u64_saturating(options.level_size_multiplier),
        local_l0_compaction,
    }
}

pub(in crate::db) fn validate_batch_len(len: usize) -> Result<()> {
    if len > u32::MAX as usize {
        return Err(Error::InvalidOptions {
            message: "write batch operation count exceeds u32::MAX".to_owned(),
        });
    }

    Ok(())
}

pub(in crate::db) fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}

pub(in crate::db) fn is_level_layout_compaction_error(error: &Error) -> bool {
    let Error::Corruption { message } = error else {
        return false;
    };
    message.contains("has overlapping tables")
        || message.contains("unbounded table mixed with other tables")
}

pub(in crate::db) fn persistent_path_from_options(options: &DbOptions) -> Option<&Path> {
    options.storage_mode.persistent_path()
}
