use super::{
    Arc, BatchOperation, Bound, DbOptions, DurabilityMode, Error, LsmTree, Result, Sequence,
    WalBatch, usize_to_u64_saturating, validate_batch_len,
};

pub(in crate::db) fn replay_wal_batches_into_buckets(
    buckets: &std::collections::BTreeMap<String, Arc<LsmTree>>,
    batches: Vec<WalBatch>,
    replay_floor: Sequence,
) -> Result<Sequence> {
    let mut last_seen = Sequence::ZERO;
    let mut last_committed = replay_floor;

    for batch in batches {
        if batch.sequence <= last_seen {
            return Err(Error::Corruption {
                message: "WAL sequence did not increase".to_owned(),
            });
        }
        last_seen = batch.sequence;
        validate_batch_len(batch.operations.len())?;

        if batch.sequence <= replay_floor {
            continue;
        }

        for (batch_index, operation) in batch.operations.into_iter().enumerate() {
            let batch_index = u32::try_from(batch_index)
                .map_err(|_| Error::invalid_options("WAL operation count exceeds u32::MAX"))?;
            let state = buckets
                .get(operation.bucket())
                .ok_or_else(|| Error::Corruption {
                    message: format!(
                        "WAL references bucket missing from manifest: {}",
                        operation.bucket()
                    ),
                })?;
            state.apply_operation(operation, batch.sequence, batch_index)?;
        }

        last_committed = batch.sequence;
    }

    Ok(last_committed)
}

pub(super) fn include_min_key(slot: &mut Option<Vec<u8>>, key: &[u8]) {
    if slot
        .as_ref()
        .is_none_or(|existing| key < existing.as_slice())
    {
        *slot = Some(key.to_vec());
    }
}

pub(super) fn include_max_key(slot: &mut Option<Vec<u8>>, key: &[u8]) {
    if slot
        .as_ref()
        .is_none_or(|existing| key > existing.as_slice())
    {
        *slot = Some(key.to_vec());
    }
}

pub(super) fn operation_estimated_bytes(operation: &BatchOperation) -> u64 {
    const PREPARED_OPERATION_OVERHEAD_BYTES: u64 = 32;

    let payload_bytes = match operation {
        BatchOperation::Put { bucket, key, value } => usize_to_u64_saturating(bucket.len())
            .saturating_add(usize_to_u64_saturating(key.len()))
            .saturating_add(usize_to_u64_saturating(value.len())),
        BatchOperation::Delete { bucket, key } => {
            usize_to_u64_saturating(bucket.len()).saturating_add(usize_to_u64_saturating(key.len()))
        }
        BatchOperation::DeleteRange { bucket, range } => usize_to_u64_saturating(bucket.len())
            .saturating_add(bound_estimated_bytes(&range.start))
            .saturating_add(bound_estimated_bytes(&range.end)),
    };

    PREPARED_OPERATION_OVERHEAD_BYTES.saturating_add(payload_bytes)
}

pub(super) fn bound_estimated_bytes(bound: &Bound<Vec<u8>>) -> u64 {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => usize_to_u64_saturating(key.len()),
        Bound::Unbounded => 0,
    }
}

pub(super) fn unique_lsm_trees(
    states: impl IntoIterator<Item = Arc<LsmTree>>,
) -> Vec<Arc<LsmTree>> {
    let mut unique = Vec::<Arc<LsmTree>>::new();
    for state in states {
        if unique.iter().any(|existing| Arc::ptr_eq(existing, &state)) {
            continue;
        }
        unique.push(state);
    }
    unique
}

pub(super) fn effective_durability(
    default: DurabilityMode,
    requested: DurabilityMode,
) -> DurabilityMode {
    // The database option is a safety floor for all writes. Per-write options
    // can ask for a stronger WAL sync, but cannot quietly weaken the database
    // level selected at open time.
    if durability_rank(requested) >= durability_rank(default) {
        requested
    } else {
        default
    }
}

const fn durability_rank(mode: DurabilityMode) -> u8 {
    match mode {
        DurabilityMode::Buffered => 0,
        DurabilityMode::Flush => 1,
        DurabilityMode::SyncData => 2,
        DurabilityMode::SyncAll => 3,
        DurabilityMode::SyncAllStrict => 4,
    }
}

pub(super) fn validate_operation_resource_bounds(
    operations: &[BatchOperation],
    max_key_bytes: usize,
    max_value_bytes: usize,
) -> Result<()> {
    for operation in operations {
        match operation {
            BatchOperation::Put { key, value, .. } => {
                validate_write_byte_field(key.len(), max_key_bytes, "write key length")?;
                validate_write_byte_field(value.len(), max_value_bytes, "write value length")?;
            }
            BatchOperation::Delete { key, .. } => {
                validate_write_byte_field(key.len(), max_key_bytes, "write key length")?;
            }
            BatchOperation::DeleteRange { range, .. } => {
                validate_bound_len(&range.start, max_key_bytes)?;
                validate_bound_len(&range.end, max_key_bytes)?;
            }
        }
    }
    Ok(())
}

pub(super) fn validate_bound_len(bound: &Bound<Vec<u8>>, max_key_bytes: usize) -> Result<()> {
    match bound {
        Bound::Included(bytes) | Bound::Excluded(bytes) => {
            validate_write_byte_field(bytes.len(), max_key_bytes, "write range bound length")
        }
        Bound::Unbounded => Ok(()),
    }
}

pub(super) fn validate_write_byte_field(
    len: usize,
    max_bytes: usize,
    field: &'static str,
) -> Result<()> {
    let max_bytes = max_bytes.min(DbOptions::MAX_WRITE_FIELD_BYTES);
    if len <= max_bytes {
        return Ok(());
    }

    Err(Error::invalid_options(format!(
        "{field} {len} exceeds maximum {max_bytes}"
    )))
}
