use std::{collections::HashSet, sync::Arc};

use sha2::{Digest, Sha256};

use crate::{
    error::{Error, Result},
    object_store::{ObjectClient, Precondition, PutIf, canonical_object_key},
    types::Sequence,
    wal,
};

use super::{
    OBJECT_WAL_MAX_CHAIN_SEGMENTS, OBJECT_WAL_MAX_REPLAY_BYTES, OBJECT_WAL_MAX_SEGMENT_BYTES,
    OBJECT_WAL_SEGMENT_HEADER_LEN, OBJECT_WAL_SEGMENT_MAGIC, ObjectLeaseState,
};

pub(super) fn encode_object_wal_segment(
    previous_key: Option<&str>,
    frames: &[u8],
) -> Result<Vec<u8>> {
    let previous_key = previous_key.unwrap_or_default();
    let key_len = u32::try_from(previous_key.len())
        .map_err(|_| Error::invalid_options("object WAL predecessor key exceeds u32::MAX"))?;
    let capacity = OBJECT_WAL_SEGMENT_HEADER_LEN
        .checked_add(previous_key.len())
        .and_then(|size| size.checked_add(frames.len()))
        .ok_or_else(|| Error::invalid_options("object WAL segment size overflow"))?;
    let mut segment = Vec::with_capacity(capacity);
    segment.extend_from_slice(OBJECT_WAL_SEGMENT_MAGIC);
    segment.extend_from_slice(&key_len.to_le_bytes());
    segment.extend_from_slice(previous_key.as_bytes());
    segment.extend_from_slice(frames);
    crate::limits::ensure_corruption_len(
        segment.len(),
        OBJECT_WAL_MAX_SEGMENT_BYTES,
        "object WAL segment length",
    )?;
    Ok(segment)
}

pub(super) fn object_wal_segment_identity(segment: &[u8]) -> String {
    let digest = Sha256::digest(segment);
    let mut identity = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut identity, "{byte:02x}").expect("writing to String cannot fail");
    }
    identity
}

pub(super) fn decode_object_wal_segment<'bytes>(
    key: &str,
    bytes: &'bytes [u8],
) -> Result<(Option<String>, &'bytes [u8])> {
    if bytes.get(..OBJECT_WAL_SEGMENT_MAGIC.len()) != Some(OBJECT_WAL_SEGMENT_MAGIC) {
        return Err(Error::Corruption {
            message: format!("object WAL segment {key} has an invalid format marker"),
        });
    }
    let key_len_bytes: [u8; 4] = bytes
        .get(8..12)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} has a truncated chain header"),
        })?
        .try_into()
        .expect("checked object WAL predecessor length bytes");
    let key_len =
        usize::try_from(u32::from_le_bytes(key_len_bytes)).map_err(|_| Error::Corruption {
            message: format!("object WAL segment {key} predecessor length overflow"),
        })?;
    let payload_offset = OBJECT_WAL_SEGMENT_HEADER_LEN
        .checked_add(key_len)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} predecessor offset overflow"),
        })?;
    let predecessor = bytes
        .get(OBJECT_WAL_SEGMENT_HEADER_LEN..payload_offset)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} has a truncated predecessor key"),
        })?;
    let frames = bytes
        .get(payload_offset..)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} has a truncated frame payload"),
        })?;
    let predecessor = if predecessor.is_empty() {
        None
    } else {
        Some(
            std::str::from_utf8(predecessor)
                .map_err(|_| Error::Corruption {
                    message: format!("object WAL segment {key} predecessor is not UTF-8"),
                })?
                .to_owned(),
        )
    };
    Ok((predecessor, frames))
}

pub(super) async fn put_immutable_object(
    client: &Arc<dyn ObjectClient>,
    key: &str,
    bytes: Arc<[u8]>,
) -> Result<()> {
    let publish = client
        .put_if(key, Arc::clone(&bytes), Precondition::IfNoneMatch)
        .await;
    match publish {
        Ok(PutIf::Stored { .. }) => Ok(()),
        Ok(PutIf::PreconditionFailed { .. }) => {
            let existing = client.get(key).await?;
            if existing.as_deref() == Some(bytes.as_ref()) {
                Ok(())
            } else {
                Err(Error::Corruption {
                    message: format!(
                        "immutable object WAL segment {key} already has different bytes"
                    ),
                })
            }
        }
        Err(error) => {
            if let Ok(Some(existing)) = client.get(key).await
                && existing.as_ref() == bytes.as_ref()
            {
                return Ok(());
            }
            Err(error)
        }
    }
}

pub(super) async fn read_object_wal_chain(
    client: &Arc<dyn ObjectClient>,
    db_path: &std::path::Path,
    state: &ObjectLeaseState,
    replay_floor: Sequence,
) -> Result<(Vec<wal::WalBatch>, Vec<String>)> {
    if state.committed_sequence <= replay_floor {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut current = state
        .current_wal_key
        .clone()
        .ok_or_else(|| Error::Corruption {
            message: format!(
                "object WAL head reached sequence {} without a segment",
                state.committed_sequence.get()
            ),
        })?;
    let mut visited = HashSet::new();
    let mut keys = Vec::new();
    let mut batches = Vec::new();
    let mut replay_bytes = 0usize;
    loop {
        if !visited.insert(current.clone()) {
            return Err(Error::Corruption {
                message: format!("object WAL chain contains a cycle at {current}"),
            });
        }
        if visited.len() > OBJECT_WAL_MAX_CHAIN_SEGMENTS {
            return Err(Error::Corruption {
                message: format!(
                    "object WAL chain exceeds {OBJECT_WAL_MAX_CHAIN_SEGMENTS} segments"
                ),
            });
        }
        let segment =
            read_verified_object_wal_segment(client, db_path, &current, replay_floor).await?;
        replay_bytes =
            replay_bytes
                .checked_add(segment.byte_len)
                .ok_or_else(|| Error::Corruption {
                    message: "object WAL replay byte count overflow".to_owned(),
                })?;
        crate::limits::ensure_corruption_len(
            replay_bytes,
            OBJECT_WAL_MAX_REPLAY_BYTES,
            "object WAL replay bytes",
        )?;
        batches.extend(
            segment
                .batches
                .into_iter()
                .filter(|batch| batch.sequence <= state.committed_sequence),
        );
        keys.push(current);
        let Some(previous) = segment.previous else {
            break;
        };
        current = previous;
    }
    batches.sort_unstable_by_key(|batch| batch.sequence);
    validate_object_wal_sequences(&batches, replay_floor, state.committed_sequence)?;
    Ok((batches, keys))
}

struct VerifiedObjectWalSegment {
    previous: Option<String>,
    batches: Vec<wal::WalBatch>,
    byte_len: usize,
}

async fn read_verified_object_wal_segment(
    client: &Arc<dyn ObjectClient>,
    db_path: &std::path::Path,
    key: &str,
    replay_floor: Sequence,
) -> Result<VerifiedObjectWalSegment> {
    validate_object_wal_key(db_path, key)?;
    let bytes = client.get(key).await?.ok_or_else(|| Error::Corruption {
        message: format!("object WAL segment {key} is missing"),
    })?;
    crate::limits::ensure_corruption_len(
        bytes.len(),
        OBJECT_WAL_MAX_SEGMENT_BYTES,
        "object WAL segment length",
    )?;
    let identity = key
        .strip_suffix(".trinewal")
        .and_then(|stem| stem.rsplit_once('-'))
        .map(|(_, identity)| identity)
        .ok_or_else(|| Error::Corruption {
            message: format!("object WAL segment {key} has no content identity"),
        })?;
    if identity != object_wal_segment_identity(&bytes) {
        return Err(Error::Corruption {
            message: format!("object WAL segment {key} content identity mismatch"),
        });
    }
    let byte_len = bytes.len();
    let (previous, frames) = decode_object_wal_segment(key, &bytes)?;
    let batches = wal::decode_frames_after(frames, replay_floor)?;
    Ok(VerifiedObjectWalSegment {
        previous,
        batches,
        byte_len,
    })
}

fn validate_object_wal_sequences(
    batches: &[wal::WalBatch],
    replay_floor: Sequence,
    committed_sequence: Sequence,
) -> Result<()> {
    let mut previous = replay_floor;
    for batch in batches {
        let expected = previous
            .get()
            .checked_add(1)
            .map(Sequence::new)
            .ok_or_else(|| Error::Corruption {
                message: "object WAL sequence overflow while validating its chain".to_owned(),
            })?;
        if batch.sequence != expected {
            return Err(Error::Corruption {
                message: format!(
                    "object WAL chain expected sequence {}, got {}",
                    expected.get(),
                    batch.sequence.get()
                ),
            });
        }
        previous = batch.sequence;
    }
    if previous != committed_sequence {
        return Err(Error::Corruption {
            message: format!(
                "object WAL chain ended at sequence {}, below committed head {}",
                previous.get(),
                committed_sequence.get()
            ),
        });
    }
    Ok(())
}

fn validate_object_wal_key(db_path: &std::path::Path, key: &str) -> Result<()> {
    let root = canonical_object_key(db_path)?;
    let canonical = crate::object_store::canonical_object_prefix(key)?;
    let expected_parent = if root.is_empty() {
        None
    } else {
        Some(root.as_str())
    };
    let path = std::path::Path::new(&canonical);
    let parent = path
        .parent()
        .and_then(std::path::Path::to_str)
        .filter(|parent| !parent.is_empty());
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default();
    if canonical != key || parent != expected_parent || !crate::is_wal_object_key(name) {
        return Err(Error::Corruption {
            message: format!("object WAL chain key {key:?} is outside database root {root:?}"),
        });
    }
    Ok(())
}

pub(crate) async fn object_store_wal_batches_after_replay_floor(
    client: Arc<dyn ObjectClient>,
    db_path: &std::path::Path,
    state: &ObjectLeaseState,
    replay_floor: Sequence,
) -> Result<Vec<wal::WalBatch>> {
    read_object_wal_chain(&client, db_path, state, replay_floor)
        .await
        .map(|(batches, _)| batches)
}
