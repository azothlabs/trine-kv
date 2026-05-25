use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    ops::Bound,
    path::{Path, PathBuf},
};

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
    types::{KeyRange, Sequence},
    write_batch::BatchOperation,
};

pub const WAL_MAGIC: u32 = 0x5452_574c;
pub const WAL_FORMAT_VERSION: u16 = 1;
pub const WAL_FILE_NAME: &str = "trine.wal";

const HEADER_LEN: usize = 18;
const OP_INSERT: u8 = 1;
const OP_REMOVE: u8 = 2;
const OP_REMOVE_RANGE: u8 = 3;
const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;
const MIN_WAL_OPERATION_BYTES: usize = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecordHeader {
    pub commit_sequence: Sequence,
    pub operation_count: u32,
    pub payload_len: u32,
    pub header_checksum: u32,
    pub payload_checksum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalBatch {
    pub sequence: Sequence,
    pub operations: Vec<BatchOperation>,
}

#[derive(Debug)]
pub struct WalWriter {
    file: File,
}

impl WalWriter {
    pub fn open_append(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(Error::from)?;

        Ok(Self { file })
    }

    pub fn append_batch(
        &mut self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        let payload = encode_payload(sequence, operations)?;
        let payload_checksum = checksum(&payload);
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| Error::invalid_options("WAL payload exceeds u32::MAX bytes"))?;
        let header_checksum = header_checksum(payload_len, payload_checksum);

        self.file.write_all(&WAL_MAGIC.to_le_bytes())?;
        self.file.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
        self.file.write_all(&payload_len.to_le_bytes())?;
        self.file.write_all(&header_checksum.to_le_bytes())?;
        self.file.write_all(&payload_checksum.to_le_bytes())?;
        self.file.write_all(&payload)?;

        self.persist(durability)?;

        Ok(())
    }

    pub fn persist(&mut self, durability: DurabilityMode) -> Result<()> {
        match durability {
            DurabilityMode::Buffered => {}
            DurabilityMode::Flush => self.file.flush()?,
            DurabilityMode::SyncData => self.file.sync_data()?,
            DurabilityMode::SyncAll => self.file.sync_all()?,
        }
        Ok(())
    }
}

#[must_use]
pub fn wal_path(db_path: &Path) -> PathBuf {
    db_path.join(WAL_FILE_NAME)
}

pub fn read_batches(path: &Path) -> Result<Vec<WalBatch>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    decode_frames(&bytes)
}

pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(Error::from)
}

fn encode_payload(sequence: Sequence, operations: &[BatchOperation]) -> Result<Vec<u8>> {
    let op_count = u32::try_from(operations.len())
        .map_err(|_| Error::invalid_options("WAL operation count exceeds u32::MAX"))?;
    let mut bytes = Vec::new();

    put_u64(&mut bytes, sequence.get());
    put_u32(&mut bytes, op_count);
    for operation in operations {
        match operation {
            BatchOperation::Insert {
                keyspace,
                key,
                value,
            } => {
                put_u8(&mut bytes, OP_INSERT);
                put_bytes(&mut bytes, keyspace.as_bytes())?;
                put_bytes(&mut bytes, key)?;
                put_bytes(&mut bytes, value)?;
            }
            BatchOperation::Remove { keyspace, key } => {
                put_u8(&mut bytes, OP_REMOVE);
                put_bytes(&mut bytes, keyspace.as_bytes())?;
                put_bytes(&mut bytes, key)?;
            }
            BatchOperation::RemoveRange { keyspace, range } => {
                put_u8(&mut bytes, OP_REMOVE_RANGE);
                put_bytes(&mut bytes, keyspace.as_bytes())?;
                put_bound(&mut bytes, &range.start)?;
                put_bound(&mut bytes, &range.end)?;
            }
        }
    }

    Ok(bytes)
}

fn decode_frames(bytes: &[u8]) -> Result<Vec<WalBatch>> {
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
        let payload_start = offset + HEADER_LEN;
        let payload_end = payload_start + payload_len;
        if payload_end > bytes.len() {
            break;
        }

        let payload = &bytes[payload_start..payload_end];
        if checksum(payload) != payload_checksum {
            return Err(Error::Corruption {
                message: "WAL payload checksum mismatch".to_owned(),
            });
        }

        batches.push(decode_payload(payload)?);
        offset = payload_end;
    }

    Ok(batches)
}

fn decode_payload(payload: &[u8]) -> Result<WalBatch> {
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
        let keyspace =
            String::from_utf8(cursor.read_bytes()?.to_vec()).map_err(|_| Error::InvalidFormat {
                message: "WAL keyspace name is not valid UTF-8".to_owned(),
            })?;

        let operation = match tag {
            OP_INSERT => {
                let key = cursor.read_bytes()?.to_vec();
                let value = cursor.read_bytes()?.to_vec();
                BatchOperation::Insert {
                    keyspace,
                    key,
                    value,
                }
            }
            OP_REMOVE => {
                let key = cursor.read_bytes()?.to_vec();
                BatchOperation::Remove { keyspace, key }
            }
            OP_REMOVE_RANGE => {
                let start = cursor.read_bound()?;
                let end = cursor.read_bound()?;
                BatchOperation::RemoveRange {
                    keyspace,
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

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("WAL byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

fn put_bound(bytes: &mut Vec<u8>, bound: &Bound<Vec<u8>>) -> Result<()> {
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

fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| invalid_wal("short u16"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_wal("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn header_checksum(payload_len: u32, payload_checksum: u32) -> u32 {
    let mut bytes = Vec::with_capacity(14);
    bytes.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    checksum(&bytes)
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

fn invalid_wal(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid WAL: {message}"),
    }
}

struct Cursor<'payload> {
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
        let value = self
            .payload
            .get(self.offset..self.offset + 8)
            .ok_or_else(|| invalid_wal("short u64"))?;
        self.offset += 8;
        Ok(u64::from_le_bytes([
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
        ]))
    }

    fn read_bytes(&mut self) -> Result<&'payload [u8]> {
        let len = self.read_u32()? as usize;
        let value = self
            .payload
            .get(self.offset..self.offset + len)
            .ok_or_else(|| invalid_wal("short bytes"))?;
        self.offset += len;
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

#[cfg(test)]
mod tests {
    use super::decode_payload;

    #[test]
    fn wal_decode_rejects_operation_count_before_large_allocation() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1_u64.to_le_bytes());
        payload.extend_from_slice(&u32::MAX.to_le_bytes());

        let error = decode_payload(&payload).expect_err("oversized operation count fails");
        assert!(
            error
                .to_string()
                .contains("operation count exceeds payload bytes"),
            "unexpected error: {error}"
        );
    }
}
