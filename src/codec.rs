use crate::{Error, error::Result, limits};

// Normal data blocks target 16 KiB and large values use blob storage. This
// keeps corrupt headers from turning a tiny compressed payload into an
// unbounded decoder allocation while leaving wide headroom for large records and
// metadata blocks.
pub(crate) const MAX_DECODED_BLOCK_BYTES: usize = limits::MAX_DECODED_BLOCK_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodecId {
    None,
    FastLz4Block,
}

impl CodecId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::FastLz4Block => "fast-lz4-block",
        }
    }

    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::None => 0,
            Self::FastLz4Block => 1,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(Self::None),
            1 => Ok(Self::FastLz4Block),
            tag => Err(Error::UnsupportedFormat {
                message: format!("unknown table codec {tag}"),
            }),
        }
    }
}

pub trait BlockCodec: Send + Sync {
    fn encode(&self, input: &[u8]) -> Result<Vec<u8>>;

    fn decode(&self, input: &[u8], uncompressed_len: usize) -> Result<Vec<u8>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoneCodec;

impl BlockCodec for NoneCodec {
    fn encode(&self, input: &[u8]) -> Result<Vec<u8>> {
        Ok(input.to_vec())
    }

    fn decode(&self, input: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
        ensure_decoded_block_len(uncompressed_len)?;
        if input.len() == uncompressed_len {
            Ok(input.to_vec())
        } else {
            Err(Error::InvalidFormat {
                message: "uncompressed block length mismatch".to_owned(),
            })
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FastLz4BlockCodec;

impl BlockCodec for FastLz4BlockCodec {
    fn encode(&self, input: &[u8]) -> Result<Vec<u8>> {
        Ok(lz4_flex::block::compress(input))
    }

    fn decode(&self, input: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
        ensure_decoded_block_len(uncompressed_len)?;
        let decoded = lz4_flex::block::decompress(input, uncompressed_len).map_err(|error| {
            Error::InvalidFormat {
                message: format!("invalid lz4 block: {error}"),
            }
        })?;
        if decoded.len() == uncompressed_len {
            Ok(decoded)
        } else {
            Err(Error::InvalidFormat {
                message: "lz4 block length mismatch".to_owned(),
            })
        }
    }
}

pub(crate) fn encode_block(codec: CodecId, input: &[u8]) -> Result<Vec<u8>> {
    match codec {
        CodecId::None => NoneCodec.encode(input),
        CodecId::FastLz4Block => FastLz4BlockCodec.encode(input),
    }
}

pub(crate) fn decode_block(
    codec: CodecId,
    input: &[u8],
    uncompressed_len: usize,
) -> Result<Vec<u8>> {
    match codec {
        CodecId::None => NoneCodec.decode(input, uncompressed_len),
        CodecId::FastLz4Block => FastLz4BlockCodec.decode(input, uncompressed_len),
    }
}

pub(crate) fn ensure_decoded_block_len(uncompressed_len: usize) -> Result<()> {
    limits::ensure_invalid_format_len(
        uncompressed_len,
        MAX_DECODED_BLOCK_BYTES,
        "decoded block length",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lz4_decode_rejects_oversized_output_before_allocation() {
        let error = decode_block(CodecId::FastLz4Block, &[0], u32::MAX as usize)
            .expect_err("oversized decoded block should fail before lz4 allocation");

        assert!(
            matches!(error, Error::InvalidFormat { .. }),
            "unexpected error kind: {error}"
        );
        assert!(
            error.to_string().contains("decoded block length"),
            "unexpected error: {error}"
        );
    }
}
