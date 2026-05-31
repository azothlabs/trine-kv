use crate::{Error, error::Result};

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
    fn id(&self) -> CodecId;

    fn encode(&self, input: &[u8]) -> Result<Vec<u8>>;

    fn decode(&self, input: &[u8], uncompressed_len: usize) -> Result<Vec<u8>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoneCodec;

impl BlockCodec for NoneCodec {
    fn id(&self) -> CodecId {
        CodecId::None
    }

    fn encode(&self, input: &[u8]) -> Result<Vec<u8>> {
        Ok(input.to_vec())
    }

    fn decode(&self, input: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
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
    fn id(&self) -> CodecId {
        CodecId::FastLz4Block
    }

    fn encode(&self, input: &[u8]) -> Result<Vec<u8>> {
        Ok(lz4_flex::block::compress(input))
    }

    fn decode(&self, input: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
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
