#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueRef {
    Inline(Vec<u8>),
    Blob {
        file_id: u64,
        offset: u64,
        len: u64,
        checksum: u32,
    },
}

impl ValueRef {
    #[must_use]
    pub const fn len(&self) -> u64 {
        match self {
            Self::Inline(bytes) => bytes.len() as u64,
            Self::Blob { len, .. } => *len,
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline(bytes) => Some(bytes),
            Self::Blob { .. } => None,
        }
    }
}
