use super::{
    Arc, Error, OBJECT_LEASE_MAGIC, OBJECT_LEASE_MAX_BYTES, OBJECT_LEASE_TTL,
    OBJECT_LEASE_V3_HEADER_LEN, OBJECT_LEASE_VERSION, ObjectClient, ObjectLeaseState,
    ObservedLeaseState, Result, Sequence,
};
#[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
use super::{Context, Future, Poll, Wake, Waker, thread};

pub(super) async fn read_lease_state(
    client: &Arc<dyn ObjectClient>,
    key: &str,
) -> Result<Option<ObservedLeaseState>> {
    let Some(meta) = client.head(key).await? else {
        return Ok(None);
    };
    if meta.size > OBJECT_LEASE_MAX_BYTES {
        return Err(Error::Corruption {
            message: format!(
                "writer lease {key} length {} exceeds maximum {OBJECT_LEASE_MAX_BYTES}",
                meta.size
            ),
        });
    }
    let bytes = if meta.size == 0 {
        Arc::from([])
    } else {
        client.get_range(key, 0, meta.size, &meta.etag).await?
    };
    Ok(Some(ObservedLeaseState {
        etag: meta.etag,
        state: decode_lease_state(key, &bytes)?,
    }))
}

pub(super) fn encode_lease_state(state: ObjectLeaseState) -> Result<Arc<[u8]>> {
    let key_len = state.current_wal_key.as_ref().map_or(0, String::len);
    let key_len = u32::try_from(key_len)
        .map_err(|_| Error::invalid_options("object WAL segment key exceeds u32::MAX"))?;
    let mut bytes = Vec::with_capacity(OBJECT_LEASE_V3_HEADER_LEN + key_len as usize);
    bytes.extend_from_slice(&OBJECT_LEASE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&OBJECT_LEASE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&state.epoch.to_le_bytes());
    bytes.extend_from_slice(&state.owner_id);
    bytes.extend_from_slice(&state.committed_sequence.get().to_le_bytes());
    bytes.extend_from_slice(&state.lease_expires_at_ms.to_le_bytes());
    bytes.extend_from_slice(&key_len.to_le_bytes());
    if let Some(key) = state.current_wal_key {
        bytes.extend_from_slice(key.as_bytes());
    }
    if u64::try_from(bytes.len()).map_or(true, |encoded_len| encoded_len > OBJECT_LEASE_MAX_BYTES) {
        return Err(Error::invalid_options(format!(
            "writer lease encoded length {} exceeds maximum {OBJECT_LEASE_MAX_BYTES}",
            bytes.len()
        )));
    }
    Ok(Arc::from(bytes))
}

pub(super) fn decode_lease_state(key: &str, bytes: &[u8]) -> Result<ObjectLeaseState> {
    if bytes.len() < OBJECT_LEASE_V3_HEADER_LEN {
        return Err(Error::Corruption {
            message: format!("writer lease {key} has a malformed state"),
        });
    }
    if read_u32_le(key, &bytes[..4], "magic")? != OBJECT_LEASE_MAGIC {
        return Err(Error::Corruption {
            message: format!("writer lease {key} has invalid magic"),
        });
    }
    let version = read_u16_le(key, &bytes[4..6], "version")?;
    if version != OBJECT_LEASE_VERSION {
        return Err(Error::Corruption {
            message: format!("writer lease {key} has unsupported version {version}"),
        });
    }
    let epoch = decode_u64(key, &bytes[6..14], "epoch")?;
    let owner_id: [u8; 16] = bytes[14..30].try_into().map_err(|_| Error::Corruption {
        message: format!("writer lease {key} has malformed owner id"),
    })?;
    let committed_sequence = Sequence::new(decode_u64(key, &bytes[30..38], "commit head")?);
    let lease_expires_at_ms = decode_u64(key, &bytes[38..46], "lease expiry")?;
    let key_len = read_u32_le(key, &bytes[46..50], "WAL segment key length")?;
    decode_lease_state_with_key(
        key,
        bytes,
        OBJECT_LEASE_V3_HEADER_LEN,
        key_len,
        ObjectLeaseState {
            epoch,
            owner_id,
            committed_sequence,
            current_wal_key: None,
            lease_expires_at_ms,
        },
    )
}

pub(super) fn decode_lease_state_with_key(
    key: &str,
    bytes: &[u8],
    key_offset: usize,
    key_len: u32,
    mut state: ObjectLeaseState,
) -> Result<ObjectLeaseState> {
    let key_len = usize::try_from(key_len).map_err(|_| Error::Corruption {
        message: format!("writer lease {key} WAL segment key length overflow"),
    })?;
    let key_end = key_offset
        .checked_add(key_len)
        .ok_or_else(|| Error::Corruption {
            message: format!("writer lease {key} WAL segment key offset overflow"),
        })?;
    if key_end != bytes.len() {
        return Err(Error::Corruption {
            message: format!("writer lease {key} has malformed WAL segment key bytes"),
        });
    }
    state.current_wal_key = if key_len == 0 {
        None
    } else {
        let key_bytes = bytes
            .get(key_offset..key_end)
            .ok_or_else(|| Error::Corruption {
                message: format!("writer lease {key} has a truncated WAL segment key"),
            })?;
        Some(
            std::str::from_utf8(key_bytes)
                .map_err(|_| Error::Corruption {
                    message: format!("writer lease {key} WAL segment key is not valid UTF-8"),
                })?
                .to_owned(),
        )
    };
    Ok(state)
}

pub(super) fn read_u16_le(key: &str, bytes: &[u8], field: &str) -> Result<u16> {
    let array: [u8; 2] = bytes.try_into().map_err(|_| Error::Corruption {
        message: format!("writer lease {key} has a malformed {field}"),
    })?;
    Ok(u16::from_le_bytes(array))
}

pub(super) fn read_u32_le(key: &str, bytes: &[u8], field: &str) -> Result<u32> {
    let array: [u8; 4] = bytes.try_into().map_err(|_| Error::Corruption {
        message: format!("writer lease {key} has a malformed {field}"),
    })?;
    Ok(u32::from_le_bytes(array))
}

pub(super) fn decode_u64(key: &str, bytes: &[u8], field: &str) -> Result<u64> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| Error::Corruption {
        message: format!("writer lease {key} has a malformed {field}"),
    })?;
    Ok(u64::from_le_bytes(array))
}

pub(super) fn current_epoch_millis() -> Result<u64> {
    crate::platform::now_unix_millis()
}

pub(super) fn object_lease_deadline_ms(now_ms: u64) -> u64 {
    let ttl_ms = u64::try_from(OBJECT_LEASE_TTL.as_millis()).unwrap_or(u64::MAX);
    now_ms.saturating_add(ttl_ms)
}

pub(super) fn lock_poisoned_error(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}

#[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
pub(super) struct SubstrateThreadWake {
    thread: thread::Thread,
}

#[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
impl Wake for SubstrateThreadWake {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

#[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
pub(super) fn block_on_substrate_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let waker = Waker::from(Arc::new(SubstrateThreadWake {
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
