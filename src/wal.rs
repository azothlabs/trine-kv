use std::{
    ops::Bound,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
    storage::{
        BlockingStorageAppendBackend, BlockingStorageAppendObject,
        BlockingStorageObjectReadBackend, BlockingStorageWalRewriteBackend, NativeFileAppendObject,
        NativeFileBackend, StorageCapability, StorageObjectId, StorageObjectKind,
        StorageReadBackend,
    },
    types::{KeyRange, Sequence},
    write_batch::BatchOperation,
};

pub const WAL_MAGIC: u32 = 0x5452_574c;
pub const WAL_FORMAT_VERSION: u16 = 1;
pub const WAL_FILE_NAME: &str = "trine.wal";
pub const WAL_REWRITE_TMP_FILE_NAME: &str = "trine.wal.tmp";

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
    append: NativeFileAppendObject,
}

#[derive(Debug)]
pub(crate) struct WalFrontDoor {
    lane: Mutex<WalWriter>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalFrontDoorAccept {
    sequence: Sequence,
}

impl WalWriter {
    pub fn open_append(path: &Path) -> Result<Self> {
        let backend = NativeFileBackend::new();
        Self::open_append_with_backend(&backend, path)
    }

    pub(crate) fn open_append_with_backend(
        backend: &NativeFileBackend,
        path: &Path,
    ) -> Result<Self> {
        Ok(Self {
            append: open_wal_append_object_with_backend(backend, path)?,
        })
    }

    pub fn append_batch(
        &mut self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        let frame = encode_batch_frame(sequence, operations)?;
        self.append.append_blocking(&frame, durability)
    }

    pub fn persist(&mut self, durability: DurabilityMode) -> Result<()> {
        self.append.persist_blocking(durability)
    }

    pub fn reopen_append(&mut self, path: &Path) -> Result<()> {
        let backend = NativeFileBackend::new();
        self.reopen_append_with_backend(&backend, path)
    }

    pub(crate) fn reopen_append_with_backend(
        &mut self,
        backend: &NativeFileBackend,
        path: &Path,
    ) -> Result<()> {
        self.append = open_wal_append_object_with_backend(backend, path)?;
        Ok(())
    }
}

impl WalFrontDoor {
    pub(crate) fn open_single_lane_with_backend(
        backend: &NativeFileBackend,
        path: &Path,
    ) -> Result<Self> {
        Ok(Self {
            lane: Mutex::new(WalWriter::open_append_with_backend(backend, path)?),
        })
    }

    pub(crate) fn accept_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<WalFrontDoorAccept> {
        self.lane
            .lock()
            .map_err(|_| wal_front_door_lock_poisoned())?
            .append_batch(sequence, operations, durability)?;
        Ok(WalFrontDoorAccept { sequence })
    }

    pub(crate) fn persist(&self, durability: DurabilityMode) -> Result<()> {
        self.lane
            .lock()
            .map_err(|_| wal_front_door_lock_poisoned())?
            .persist(durability)
    }

    pub(crate) fn rewrite_after_replay_floor(
        &self,
        backend: &NativeFileBackend,
        path: &Path,
        replay_floor: Sequence,
    ) -> Result<()> {
        let mut lane = self
            .lane
            .lock()
            .map_err(|_| wal_front_door_lock_poisoned())?;
        lane.persist(DurabilityMode::SyncAll)?;
        rewrite_batches_after_with_backend(backend, path, replay_floor)?;
        lane.reopen_append_with_backend(backend, path)
    }
}

impl WalFrontDoorAccept {
    #[must_use]
    pub(crate) const fn sequence(self) -> Sequence {
        self.sequence
    }
}

#[must_use]
pub fn wal_path(db_path: &Path) -> PathBuf {
    db_path.join(WAL_FILE_NAME)
}

pub fn read_batches(path: &Path) -> Result<Vec<WalBatch>> {
    read_batches_after(path, Sequence::ZERO)
}

pub fn read_batches_after(path: &Path, replay_floor: Sequence) -> Result<Vec<WalBatch>> {
    let backend = NativeFileBackend::new();
    read_batches_after_with_backend(&backend, path, replay_floor)
}

pub(crate) fn read_batches_after_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
    replay_floor: Sequence,
) -> Result<Vec<WalBatch>> {
    let Some(bytes) = read_wal_object_with_backend(backend, path)? else {
        return Ok(Vec::new());
    };
    decode_frames_after(bytes.as_ref(), replay_floor)
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

pub fn rewrite_batches_after(path: &Path, replay_floor: Sequence) -> Result<()> {
    let backend = NativeFileBackend::new();
    rewrite_batches_after_with_backend(&backend, path, replay_floor)
}

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

fn wal_rewrite_tmp_path(path: &Path) -> PathBuf {
    path.with_file_name(WAL_REWRITE_TMP_FILE_NAME)
}

fn open_wal_append_object_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<NativeFileAppendObject> {
    backend.capabilities().require(StorageCapability::Append)?;
    backend.open_append_blocking(wal_storage_object(path))
}

fn read_wal_object_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<Option<Arc<[u8]>>> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectRead)?;
    backend.read_object_bytes_blocking(wal_storage_object(path))
}

fn rewrite_wal_object_with_backend(
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

fn wal_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::Wal, path)
}

fn wal_front_door_lock_poisoned() -> Error {
    Error::Corruption {
        message: "WAL front door lock poisoned".to_owned(),
    }
}

fn validate_wal_stream_order(batches: &[WalBatch]) -> Result<()> {
    let mut last_seen = Sequence::ZERO;
    for batch in batches {
        if batch.sequence <= last_seen {
            return Err(invalid_wal("WAL stream sequence did not increase"));
        }
        last_seen = batch.sequence;
    }
    Ok(())
}

fn encode_batches_after(batches: &[WalBatch], replay_floor: Sequence) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for batch in batches.iter().filter(|batch| batch.sequence > replay_floor) {
        bytes.extend_from_slice(&encode_batch_frame(batch.sequence, &batch.operations)?);
    }
    Ok(bytes)
}

fn encode_batch_frame(sequence: Sequence, operations: &[BatchOperation]) -> Result<Vec<u8>> {
    let payload = encode_payload(sequence, operations)?;
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

fn encode_payload(sequence: Sequence, operations: &[BatchOperation]) -> Result<Vec<u8>> {
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

fn decode_frames_after(bytes: &[u8], replay_floor: Sequence) -> Result<Vec<WalBatch>> {
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

        if payload_sequence(payload)? > replay_floor {
            batches.push(decode_payload(payload)?);
        }
        offset = payload_end;
    }

    Ok(batches)
}

fn payload_sequence(payload: &[u8]) -> Result<Sequence> {
    Ok(Sequence::new(read_u64_at(payload, 0)?))
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

fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid_wal("short u64"))?;
    Ok(u64::from_le_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
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
        let value = read_u64_at(self.payload, self.offset)?;
        self.offset += 8;
        Ok(value)
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
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        options::DurabilityMode, storage::NativeFileBackend, types::Sequence,
        write_batch::BatchOperation,
    };

    use super::{
        WAL_FILE_NAME, WAL_FORMAT_VERSION, WAL_MAGIC, WalFrontDoor, checksum, decode_frames_after,
        decode_payload, merge_batch_streams_by_sequence, read_batches_after_with_backend,
    };

    #[test]
    fn wal_front_door_accepts_whole_commit_record() {
        let dir = temp_dir("front-door-accept");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let path = dir.join(WAL_FILE_NAME);
        let backend = NativeFileBackend::new();
        let front_door =
            WalFrontDoor::open_single_lane_with_backend(&backend, &path).expect("front door opens");
        let operations = vec![BatchOperation::Put {
            bucket: "default".to_owned(),
            key: b"a".to_vec(),
            value: b"a1".to_vec(),
        }];

        let accepted = front_door
            .accept_commit(Sequence::new(7), &operations, DurabilityMode::Flush)
            .expect("front door accepts commit");

        assert_eq!(accepted.sequence(), Sequence::new(7));
        let batches =
            read_batches_after_with_backend(&backend, &path, Sequence::ZERO).expect("WAL reads");
        assert_eq!(
            batches,
            vec![super::WalBatch {
                sequence: Sequence::new(7),
                operations,
            }]
        );
        cleanup_dir(&dir);
    }

    #[test]
    fn wal_front_door_rewrite_reopens_append_lane() {
        let dir = temp_dir("front-door-rewrite");
        fs::create_dir_all(&dir).expect("create WAL test dir");
        let path = dir.join(WAL_FILE_NAME);
        let backend = NativeFileBackend::new();
        let front_door =
            WalFrontDoor::open_single_lane_with_backend(&backend, &path).expect("front door opens");

        front_door
            .accept_commit(Sequence::new(1), &[put("a", "old")], DurabilityMode::Flush)
            .expect("first commit accepts");
        front_door
            .accept_commit(Sequence::new(2), &[put("b", "kept")], DurabilityMode::Flush)
            .expect("second commit accepts");
        front_door
            .rewrite_after_replay_floor(&backend, &path, Sequence::new(1))
            .expect("front door rewrites WAL");
        front_door
            .accept_commit(Sequence::new(3), &[put("c", "new")], DurabilityMode::Flush)
            .expect("append lane still accepts after rewrite");

        let sequences = read_batches_after_with_backend(&backend, &path, Sequence::ZERO)
            .expect("WAL reads")
            .into_iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![Sequence::new(2), Sequence::new(3)]);
        cleanup_dir(&dir);
    }

    #[test]
    fn wal_stream_merge_orders_batches_across_sources() {
        let first = vec![batch(Sequence::new(1)), batch(Sequence::new(4))];
        let second = vec![batch(Sequence::new(2)), batch(Sequence::new(3))];

        let sequences = merge_batch_streams_by_sequence([first, second])
            .expect("streams merge")
            .into_iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>();

        assert_eq!(
            sequences,
            vec![
                Sequence::new(1),
                Sequence::new(2),
                Sequence::new(3),
                Sequence::new(4)
            ]
        );
    }

    #[test]
    fn wal_stream_merge_rejects_duplicate_sequence() {
        let error = merge_batch_streams_by_sequence([
            vec![batch(Sequence::new(1)), batch(Sequence::new(3))],
            vec![batch(Sequence::new(2)), batch(Sequence::new(3))],
        ])
        .expect_err("duplicate sequence fails");

        assert!(
            error
                .to_string()
                .contains("duplicate WAL sequence across streams"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn wal_stream_merge_rejects_non_increasing_source() {
        let error = merge_batch_streams_by_sequence([vec![
            batch(Sequence::new(2)),
            batch(Sequence::new(1)),
        ]])
        .expect_err("non-increasing source fails");

        assert!(
            error
                .to_string()
                .contains("WAL stream sequence did not increase"),
            "unexpected error: {error}"
        );
    }

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

    #[test]
    fn wal_decode_after_floor_skips_old_operation_payloads() {
        let mut old_payload = Vec::new();
        old_payload.extend_from_slice(&1_u64.to_le_bytes());
        old_payload.extend_from_slice(&u32::MAX.to_le_bytes());

        let new_payload = super::encode_payload(
            Sequence::new(2),
            &[BatchOperation::Put {
                bucket: "default".to_owned(),
                key: b"a".to_vec(),
                value: b"a1".to_vec(),
            }],
        )
        .expect("new payload encodes");

        let mut bytes = frame_for_payload(&old_payload);
        bytes.extend_from_slice(&frame_for_payload(&new_payload));

        let batches =
            decode_frames_after(&bytes, Sequence::new(1)).expect("old payload is skipped");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].sequence, Sequence::new(2));

        let error = decode_frames_after(&bytes, Sequence::ZERO)
            .expect_err("old payload is decoded without a replay floor");
        assert!(
            error
                .to_string()
                .contains("operation count exceeds payload bytes"),
            "unexpected error: {error}"
        );
    }

    fn frame_for_payload(payload: &[u8]) -> Vec<u8> {
        let payload_len = u32::try_from(payload.len()).expect("test payload fits u32");
        let payload_checksum = checksum(payload);
        let header_checksum = super::header_checksum(payload_len, payload_checksum);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&header_checksum.to_le_bytes());
        bytes.extend_from_slice(&payload_checksum.to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn put(key: &str, value: &str) -> BatchOperation {
        BatchOperation::Put {
            bucket: "default".to_owned(),
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn batch(sequence: Sequence) -> super::WalBatch {
        super::WalBatch {
            sequence,
            operations: Vec::new(),
        }
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "trine-kv-wal-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn cleanup_dir(dir: &std::path::Path) {
        match fs::remove_dir_all(dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("failed to cleanup {}: {error}", dir.display()),
        }
    }
}
