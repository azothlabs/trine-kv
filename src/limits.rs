use crate::{Error, error::Result};

pub(crate) const MAX_DECODED_BLOCK_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_WHOLE_TABLE_DECODE_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const MAX_WHOLE_BLOB_DECODE_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const MAX_BLOB_RECORD_BODY_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_BLOB_PROPERTIES_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_WAL_FRAME_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn ensure_invalid_format_len(len: usize, max: usize, field: &'static str) -> Result<()> {
    if len <= max {
        return Ok(());
    }

    Err(Error::InvalidFormat {
        message: format!("{field} {len} exceeds maximum {max}"),
    })
}

pub(crate) fn ensure_corruption_len(len: usize, max: usize, field: &'static str) -> Result<()> {
    if len <= max {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!("{field} {len} exceeds maximum {max}"),
    })
}

pub(crate) fn checked_add_invalid_format(
    left: usize,
    right: usize,
    field: &'static str,
) -> Result<usize> {
    left.checked_add(right).ok_or_else(|| Error::InvalidFormat {
        message: format!("{field} overflows"),
    })
}
