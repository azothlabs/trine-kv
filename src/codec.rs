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
