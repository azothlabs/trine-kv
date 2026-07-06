use super::{
    BOUND_EXCLUDED, BOUND_INCLUDED, BOUND_UNBOUNDED, BatchOperation, Bound, Error, HEADER_LEN,
    KeyRange, MIN_WAL_OPERATION_BYTES, OBJECT_WAL_COMMIT_MARKER, OBJECT_WAL_FILE_PREFIX,
    OBJECT_WAL_FILE_SUFFIX, OBJECT_WAL_REWRITE_MARKER, OBJECT_WAL_REWRITE_PREFIX,
    OBJECT_WAL_SEQUENCE_DIGITS, OP_INSERT, OP_REMOVE, OP_REMOVE_RANGE, Path, Result, Sequence,
    WAL_FORMAT_VERSION, WAL_MAGIC, WalBatch, limits,
};

pub(crate) fn encode_batches_after(
    batches: &[WalBatch],
    replay_floor: Sequence,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for batch in batches.iter().filter(|batch| batch.sequence > replay_floor) {
        bytes.extend_from_slice(&encode_batch_frame(batch.sequence, &batch.operations)?);
    }
    Ok(bytes)
}

pub(crate) fn encode_batch_frame(
    sequence: Sequence,
    operations: &[BatchOperation],
) -> Result<Vec<u8>> {
    let payload = encode_payload(sequence, operations)?;
    if payload.len() > limits::MAX_WAL_FRAME_PAYLOAD_BYTES {
        return Err(Error::invalid_options(format!(
            "WAL payload length {} exceeds maximum {}",
            payload.len(),
            limits::MAX_WAL_FRAME_PAYLOAD_BYTES
        )));
    }
    let payload_checksum = checksum(&payload);
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::invalid_options("WAL payload exceeds u32::MAX bytes"))?;
    let header_checksum = header_checksum(payload_len, payload_checksum);

    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    frame.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&header_checksum.to_le_bytes());
    frame.extend_from_slice(&payload_checksum.to_le_bytes());
    frame.extend_from_slice(&payload);

    Ok(frame)
}

pub(super) fn object_wal_sequence_from_path(path: &Path) -> Result<Option<Sequence>> {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(Error::Corruption {
            message: format!(
                "object WAL file name is not valid UTF-8: {}",
                path.display()
            ),
        });
    };
    let sequence = if let Some(rest) = file_name.strip_prefix(OBJECT_WAL_FILE_PREFIX) {
        let Some((_, sequence)) = rest.split_once(OBJECT_WAL_COMMIT_MARKER) else {
            return Ok(None);
        };
        sequence
    } else if let Some(rest) = file_name.strip_prefix(OBJECT_WAL_REWRITE_PREFIX) {
        let Some((_, sequence)) = rest.split_once(OBJECT_WAL_REWRITE_MARKER) else {
            return Ok(None);
        };
        sequence
    } else {
        return Ok(None);
    };
    let Some(sequence) = sequence.strip_suffix(OBJECT_WAL_FILE_SUFFIX) else {
        return Ok(None);
    };
    if sequence.len() != OBJECT_WAL_SEQUENCE_DIGITS
        || !sequence.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(Error::Corruption {
            message: format!("malformed object WAL file name: {file_name}"),
        });
    }
    let sequence = sequence.parse::<u64>().map_err(|error| Error::Corruption {
        message: format!("malformed object WAL file name {file_name}: {error}"),
    })?;
    Ok(Some(Sequence::new(sequence)))
}

pub(super) fn encode_payload(sequence: Sequence, operations: &[BatchOperation]) -> Result<Vec<u8>> {
    let op_count = u32::try_from(operations.len())
        .map_err(|_| Error::invalid_options("WAL operation count exceeds u32::MAX"))?;
    let mut bytes = Vec::new();

    put_u64(&mut bytes, sequence.get());
    put_u32(&mut bytes, op_count);
    for operation in operations {
        match operation {
            BatchOperation::Put { bucket, key, value } => {
                put_u8(&mut bytes, OP_INSERT);
                put_bytes(&mut bytes, bucket.as_bytes())?;
                put_bytes(&mut bytes, key)?;
                put_bytes(&mut bytes, value)?;
            }
            BatchOperation::Delete { bucket, key } => {
                put_u8(&mut bytes, OP_REMOVE);
                put_bytes(&mut bytes, bucket.as_bytes())?;
                put_bytes(&mut bytes, key)?;
            }
            BatchOperation::DeleteRange { bucket, range } => {
                put_u8(&mut bytes, OP_REMOVE_RANGE);
                put_bytes(&mut bytes, bucket.as_bytes())?;
                put_bound(&mut bytes, &range.start)?;
                put_bound(&mut bytes, &range.end)?;
            }
        }
    }

    Ok(bytes)
}

pub(crate) fn decode_frames_after(bytes: &[u8], replay_floor: Sequence) -> Result<Vec<WalBatch>> {
    let mut batches = Vec::new();
    let mut offset = 0;

    while offset < bytes.len() {
        if bytes.len() - offset < HEADER_LEN {
            break;
        }

        let magic = read_u32_at(bytes, offset)?;
        let version = read_u16_at(bytes, offset + 4)?;
        let payload_len = read_u32_at(bytes, offset + 6)?;
        let actual_header_checksum = read_u32_at(bytes, offset + 10)?;
        let payload_checksum = read_u32_at(bytes, offset + 14)?;
        let expected_header_checksum = header_checksum(payload_len, payload_checksum);

        if magic != WAL_MAGIC {
            return Err(Error::Corruption {
                message: "WAL magic mismatch".to_owned(),
            });
        }
        if version != WAL_FORMAT_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!("unsupported WAL version {version}"),
            });
        }
        if actual_header_checksum != expected_header_checksum {
            return Err(Error::Corruption {
                message: "WAL header checksum mismatch".to_owned(),
            });
        }

        let payload_len = payload_len as usize;
        limits::ensure_invalid_format_len(
            payload_len,
            limits::MAX_WAL_FRAME_PAYLOAD_BYTES,
            "WAL payload length",
        )?;
        let payload_start =
            limits::checked_add_invalid_format(offset, HEADER_LEN, "WAL payload start")?;
        let payload_end =
            limits::checked_add_invalid_format(payload_start, payload_len, "WAL payload end")?;
        if payload_end > bytes.len() {
            break;
        }

        let payload = &bytes[payload_start..payload_end];
        if checksum(payload) != payload_checksum {
            return Err(Error::Corruption {
                message: "WAL payload checksum mismatch".to_owned(),
            });
        }

        if payload_sequence(payload)? > replay_floor {
            batches.push(decode_payload(payload)?);
        }
        offset = payload_end;
    }

    Ok(batches)
}

pub(super) fn payload_sequence(payload: &[u8]) -> Result<Sequence> {
    Ok(Sequence::new(read_u64_at(payload, 0)?))
}

pub(super) fn decode_payload(payload: &[u8]) -> Result<WalBatch> {
    let mut cursor = Cursor::new(payload);
    let sequence = Sequence::new(cursor.read_u64()?);
    let op_count = cursor.read_u32()? as usize;
    if op_count > cursor.remaining_len() / MIN_WAL_OPERATION_BYTES {
        return Err(Error::InvalidFormat {
            message: "WAL operation count exceeds payload bytes".to_owned(),
        });
    }
    let mut operations = Vec::with_capacity(op_count);

    for _ in 0..op_count {
        let tag = cursor.read_u8()?;
        let bucket =
            String::from_utf8(cursor.read_bytes()?.to_vec()).map_err(|_| Error::InvalidFormat {
                message: "WAL bucket name is not valid UTF-8".to_owned(),
            })?;

        let operation = match tag {
            OP_INSERT => {
                let key = cursor.read_bytes()?.to_vec();
                let value = cursor.read_bytes()?.to_vec();
                BatchOperation::Put { bucket, key, value }
            }
            OP_REMOVE => {
                let key = cursor.read_bytes()?.to_vec();
                BatchOperation::Delete { bucket, key }
            }
            OP_REMOVE_RANGE => {
                let start = cursor.read_bound()?;
                let end = cursor.read_bound()?;
                BatchOperation::DeleteRange {
                    bucket,
                    range: KeyRange { start, end },
                }
            }
            _ => {
                return Err(Error::InvalidFormat {
                    message: format!("unknown WAL operation tag {tag}"),
                });
            }
        };

        operations.push(operation);
    }

    if !cursor.is_finished() {
        return Err(Error::InvalidFormat {
            message: "WAL payload has trailing bytes".to_owned(),
        });
    }

    Ok(WalBatch {
        sequence,
        operations,
    })
}

pub(super) fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

pub(super) fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("WAL byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

pub(super) fn put_bound(bytes: &mut Vec<u8>, bound: &Bound<Vec<u8>>) -> Result<()> {
    match bound {
        Bound::Unbounded => put_u8(bytes, BOUND_UNBOUNDED),
        Bound::Included(value) => {
            put_u8(bytes, BOUND_INCLUDED);
            put_bytes(bytes, value)?;
        }
        Bound::Excluded(value) => {
            put_u8(bytes, BOUND_EXCLUDED);
            put_bytes(bytes, value)?;
        }
    }
    Ok(())
}

pub(super) fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let end = limits::checked_add_invalid_format(offset, 2, "u16 offset")?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_wal("short u16"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

pub(super) fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = limits::checked_add_invalid_format(offset, 4, "u32 offset")?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_wal("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

pub(super) fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = limits::checked_add_invalid_format(offset, 8, "u64 offset")?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_wal("short u64"))?;
    Ok(u64::from_le_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

pub(super) fn header_checksum(payload_len: u32, payload_checksum: u32) -> u32 {
    let mut bytes = Vec::with_capacity(14);
    bytes.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    checksum(&bytes)
}

pub(super) fn checksum(bytes: &[u8]) -> u32 {
    crate::checksum::crc32c(bytes)
}

pub(super) fn invalid_wal(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid WAL: {message}"),
    }
}

pub(super) fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) struct Cursor<'payload> {
    payload: &'payload [u8],
    offset: usize,
}

impl<'payload> Cursor<'payload> {
    const fn new(payload: &'payload [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .payload
            .get(self.offset)
            .ok_or_else(|| invalid_wal("short u8"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let value = read_u32_at(self.payload, self.offset)?;
        self.offset += 4;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64> {
        let value = read_u64_at(self.payload, self.offset)?;
        self.offset += 8;
        Ok(value)
    }

    fn read_bytes(&mut self) -> Result<&'payload [u8]> {
        let len = self.read_u32()? as usize;
        let end = limits::checked_add_invalid_format(self.offset, len, "WAL byte field length")?;
        let value = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| invalid_wal("short bytes"))?;
        self.offset = end;
        Ok(value)
    }

    fn read_bound(&mut self) -> Result<Bound<Vec<u8>>> {
        match self.read_u8()? {
            BOUND_UNBOUNDED => Ok(Bound::Unbounded),
            BOUND_INCLUDED => Ok(Bound::Included(self.read_bytes()?.to_vec())),
            BOUND_EXCLUDED => Ok(Bound::Excluded(self.read_bytes()?.to_vec())),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown WAL range bound tag {tag}"),
            }),
        }
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.payload.len()
    }

    const fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}
