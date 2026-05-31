use crate::{
    codec::{self, CodecId},
    error::{Error, Result},
    storage::StorageReadBuffer,
};

const BLOCK_HEADER_LEN: usize = 13;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockHandle {
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct BlockManager;

pub(crate) trait BlockReadSource {
    fn read_exact_at(&self, offset: usize, bytes: &mut [u8]) -> Result<()>;

    /// Reads `len` bytes at `offset` into an owned, `Arc`-backed completion that
    /// is decoupled from the borrowed-buffer path. Block decode reads through
    /// this seam so the read completion no longer borrows the decode call frame,
    /// which is the precondition for driving decode through the runtime's
    /// owned-read boundary in a later phase. The default fills a heap buffer via
    /// the borrowed path; native-file sources override it to use the storage
    /// object's owned blocking read.
    fn read_exact_at_owned(&self, offset: usize, len: usize) -> Result<StorageReadBuffer> {
        let mut bytes = vec![0_u8; len];
        self.read_exact_at(offset, &mut bytes)?;
        Ok(StorageReadBuffer::from_vec(offset, bytes))
    }
}

impl BlockManager {
    pub(crate) fn append_checked(
        bytes: &mut Vec<u8>,
        codec: CodecId,
        block_payload: &[u8],
    ) -> Result<BlockHandle> {
        let section_start = bytes.len();
        let block = Self::encode_checked(codec, block_payload)?;
        bytes.extend_from_slice(&block);

        Ok(BlockHandle {
            offset: usize_to_u64(section_start, "block offset")?,
            len: usize_to_u64(bytes.len() - section_start, "block length")?,
        })
    }

    pub(crate) fn encode_checked(codec: CodecId, block_payload: &[u8]) -> Result<Vec<u8>> {
        let encoded = codec::encode_block(codec, block_payload)?;
        let mut bytes = Vec::with_capacity(BLOCK_HEADER_LEN + encoded.len());
        bytes.push(codec.tag());
        put_u32(
            &mut bytes,
            usize_to_u32(block_payload.len(), "block payload length")?,
        );
        put_u32(
            &mut bytes,
            usize_to_u32(encoded.len(), "encoded block length")?,
        );
        put_u32(&mut bytes, checksum(&encoded));
        bytes.extend_from_slice(&encoded);
        Ok(bytes)
    }

    #[cfg(test)]
    pub(crate) fn read_checked(payload: &[u8], block: BlockHandle) -> Result<(CodecId, Vec<u8>)> {
        let (start, end) = block_bounds(block)?;
        let block_bytes = payload
            .get(start..end)
            .ok_or_else(|| invalid_table("block outside table payload"))?;
        Self::decode_checked(block_bytes)
    }

    #[cfg(test)]
    pub(crate) fn read_checked_at_payload_offset(
        payload: &[u8],
        offset: usize,
    ) -> Result<(BlockHandle, CodecId, Vec<u8>)> {
        let header_end = offset
            .checked_add(BLOCK_HEADER_LEN)
            .ok_or_else(|| invalid_table("block offset overflow"))?;
        let header = payload
            .get(offset..header_end)
            .ok_or_else(|| invalid_table("short block header"))?;
        let len = checked_block_len(header)?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| invalid_table("block offset overflow"))?;
        let block = BlockHandle {
            offset: usize_to_u64(offset, "block offset")?,
            len: usize_to_u64(len, "block length")?,
        };
        let block_bytes = payload
            .get(offset..end)
            .ok_or_else(|| invalid_table("block outside table payload"))?;
        let (codec, decoded) = Self::decode_checked(block_bytes)?;
        Ok((block, codec, decoded))
    }

    pub(crate) fn read_checked_from_source(
        payload_len: usize,
        payload_base_offset: usize,
        block: BlockHandle,
        source: &impl BlockReadSource,
    ) -> Result<(CodecId, Vec<u8>)> {
        let (start, end) = block_bounds(block)?;
        if end > payload_len {
            return Err(invalid_table("block outside table payload"));
        }

        let source_offset = payload_base_offset
            .checked_add(start)
            .ok_or_else(|| invalid_table("block file offset overflow"))?;
        let block_bytes = source.read_exact_at_owned(source_offset, end - start)?;
        Self::decode_checked(block_bytes.as_slice())
    }

    pub(crate) fn read_checked_at_source_offset(
        payload_len: usize,
        payload_base_offset: usize,
        offset: usize,
        source: &impl BlockReadSource,
    ) -> Result<(BlockHandle, CodecId, Vec<u8>)> {
        if offset >= payload_len {
            return Err(invalid_table("block outside table payload"));
        }
        let source_offset = payload_base_offset
            .checked_add(offset)
            .ok_or_else(|| invalid_table("block file offset overflow"))?;
        let header = source.read_exact_at_owned(source_offset, BLOCK_HEADER_LEN)?;
        let len = checked_block_len(header.as_slice())?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| invalid_table("block offset overflow"))?;
        if end > payload_len {
            return Err(invalid_table("block outside table payload"));
        }
        let block_bytes = source.read_exact_at_owned(source_offset, len)?;
        let (codec, decoded) = Self::decode_checked(block_bytes.as_slice())?;
        Ok((
            BlockHandle {
                offset: usize_to_u64(offset, "block offset")?,
                len: usize_to_u64(len, "block length")?,
            },
            codec,
            decoded,
        ))
    }

    pub(crate) fn decode_checked(block_bytes: &[u8]) -> Result<(CodecId, Vec<u8>)> {
        if block_bytes.len() < BLOCK_HEADER_LEN {
            return Err(invalid_table("short block header"));
        }

        let codec = CodecId::from_tag(block_bytes[0])?;
        let uncompressed_len = read_u32_at(block_bytes, 1)? as usize;
        let encoded_len = read_u32_at(block_bytes, 5)? as usize;
        let expected_checksum = read_u32_at(block_bytes, 9)?;
        if block_bytes.len() != BLOCK_HEADER_LEN + encoded_len {
            return Err(Error::Corruption {
                message: "block length mismatch".to_owned(),
            });
        }

        let encoded = &block_bytes[BLOCK_HEADER_LEN..];
        if checksum(encoded) != expected_checksum {
            return Err(Error::Corruption {
                message: "block checksum mismatch".to_owned(),
            });
        }

        Ok((
            codec,
            codec::decode_block(codec, encoded, uncompressed_len)?,
        ))
    }
}

pub(crate) fn block_bounds(handle: BlockHandle) -> Result<(usize, usize)> {
    bounds(handle.offset, handle.len)
}

pub(crate) fn bounds(offset: u64, len: u64) -> Result<(usize, usize)> {
    let start = usize::try_from(offset).map_err(|_| invalid_table("offset exceeds usize"))?;
    let len = usize::try_from(len).map_err(|_| invalid_table("length exceeds usize"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| invalid_table("offset plus length overflows usize"))?;
    Ok((start, end))
}

pub(crate) fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

fn checked_block_len(header: &[u8]) -> Result<usize> {
    let encoded_len = read_u32_at(header, 5)? as usize;
    BLOCK_HEADER_LEN
        .checked_add(encoded_len)
        .ok_or_else(|| invalid_table("block length overflow"))
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_table("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn usize_to_u32(value: usize, field: &'static str) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u32::MAX")))
}

fn usize_to_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u64::MAX")))
}

fn invalid_table(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid table: {message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Slice-backed source that exercises the default (borrowed-fallback) owned
    /// read seam: it only implements the borrowed read, so `read_exact_at_owned`
    /// runs the trait default that copies into an owned buffer.
    struct SliceSource<'a> {
        bytes: &'a [u8],
    }

    impl BlockReadSource for SliceSource<'_> {
        fn read_exact_at(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
            let end = offset
                .checked_add(bytes.len())
                .ok_or_else(|| invalid_table("read offset overflow"))?;
            let slice = self
                .bytes
                .get(offset..end)
                .ok_or_else(|| invalid_table("read past end"))?;
            bytes.copy_from_slice(slice);
            Ok(())
        }
    }

    #[test]
    fn owned_default_fallback_matches_borrowed_read() {
        let payload = b"trine block decode owned seam".to_vec();
        let source = SliceSource { bytes: &payload };

        let owned = source.read_exact_at_owned(4, 5).expect("owned read");
        assert_eq!(owned.offset(), 4);
        assert_eq!(owned.as_slice(), &payload[4..9]);
    }

    #[test]
    fn read_checked_from_source_decodes_through_owned_seam() {
        let mut payload = Vec::new();
        let block_payload = b"checked block body";
        let handle = BlockManager::append_checked(&mut payload, CodecId::None, block_payload)
            .expect("append block");
        let source = SliceSource { bytes: &payload };

        let (codec, decoded) =
            BlockManager::read_checked_from_source(payload.len(), 0, handle, &source)
                .expect("read checked block");
        assert_eq!(codec, CodecId::None);
        assert_eq!(decoded, block_payload);
    }

    #[test]
    fn read_checked_at_source_offset_decodes_through_owned_seam() {
        let mut payload = Vec::new();
        let block_payload = b"offset addressed block";
        let handle = BlockManager::append_checked(&mut payload, CodecId::None, block_payload)
            .expect("append block");
        let source = SliceSource { bytes: &payload };

        let offset = usize::try_from(handle.offset).expect("offset fits usize");
        let (read_handle, codec, decoded) =
            BlockManager::read_checked_at_source_offset(payload.len(), 0, offset, &source)
                .expect("read checked block at offset");
        assert_eq!(read_handle, handle);
        assert_eq!(codec, CodecId::None);
        assert_eq!(decoded, block_payload);
    }
}
