use crate::{Error, error::Result};

pub(crate) const MAX_DECODED_BLOCK_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_WHOLE_TABLE_DECODE_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const MAX_WHOLE_BLOB_DECODE_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const MAX_BLOB_RECORD_BODY_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_BLOB_PROPERTIES_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_WAL_FRAME_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
// Blob properties retain both boundary internal keys. Reserving fixed metadata
// room and dividing the remainder by two guarantees every accepted key can be
// represented in that properties block.
pub(crate) const MAX_USER_KEY_BYTES: usize = (MAX_BLOB_PROPERTIES_BYTES - 128) / 2;
// LZ4's block worst case is input + input/255 + 16. Reserve the maximum key and
// fixed record metadata too, so independently accepted maximum key/value fields
// remain encodable when they occur in the same put.
const MAX_BLOB_VALUE_BUDGET: usize = MAX_BLOB_RECORD_BODY_BYTES - MAX_USER_KEY_BYTES - 64 - 16;
pub(crate) const MAX_USER_VALUE_BYTES: usize =
    (MAX_BLOB_VALUE_BUDGET / 256) * 255 + (MAX_BLOB_VALUE_BUDGET % 256) * 255 / 256;

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
