use crate::types::Sequence;

pub const WAL_MAGIC: u32 = 0x5452_574c;
pub const WAL_FORMAT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecordHeader {
    pub commit_sequence: Sequence,
    pub operation_count: u32,
    pub payload_len: u32,
    pub header_checksum: u32,
    pub payload_checksum: u32,
}
