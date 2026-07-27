use super::{CONTENT_ID_SHA256_TAG, Digest, Error, Result, Sha256, fmt, write_hex};

/// Cryptographic algorithm carried by a [`ContentId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ContentHashAlgorithm {
    /// SHA-256 over the complete original byte sequence.
    Sha256,
}

impl ContentHashAlgorithm {
    pub(in crate::content) const fn tag(self) -> u8 {
        match self {
            Self::Sha256 => CONTENT_ID_SHA256_TAG,
        }
    }

    pub(in crate::content) fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            CONTENT_ID_SHA256_TAG => Ok(Self::Sha256),
            _ => Err(Error::UnsupportedFormat {
                message: format!("unsupported content hash algorithm tag {tag}"),
            }),
        }
    }
}

/// Algorithm-tagged identity of one immutable original byte sequence.
///
/// Equality means byte identity under the carried cryptographic algorithm. It
/// does not identify a file name, owner, directory entry, or physical object.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentId {
    pub(in crate::content) algorithm: ContentHashAlgorithm,
    pub(in crate::content) digest: [u8; 32],
}

impl ContentId {
    /// Decodes the portable 33-byte content identity.
    ///
    /// Byte zero selects the digest algorithm and the remaining 32 bytes carry
    /// its digest. Unknown algorithm tags fail closed so stored catalog values
    /// are never reinterpreted under a different hash scheme.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the algorithm tag is not
    /// supported by this build.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        let algorithm = ContentHashAlgorithm::from_tag(bytes[0])?;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&bytes[1..]);
        Ok(Self { algorithm, digest })
    }

    /// Returns the portable algorithm-tagged 33-byte content identity.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 33] {
        let mut bytes = [0_u8; 33];
        bytes[0] = self.algorithm.tag();
        bytes[1..].copy_from_slice(&self.digest);
        bytes
    }

    /// Computes the identity of an in-memory byte slice.
    ///
    /// Incremental uploads compute the same value without retaining all bytes;
    /// this convenience constructor is intended for expected-digest checks and
    /// small inputs.
    #[must_use]
    pub fn for_bytes(bytes: &[u8]) -> Self {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        Self {
            algorithm: ContentHashAlgorithm::Sha256,
            digest,
        }
    }

    /// Returns the hash algorithm carried by this identity.
    #[must_use]
    pub const fn algorithm(self) -> ContentHashAlgorithm {
        self.algorithm
    }

    /// Returns the 32-byte digest without its algorithm tag.
    #[must_use]
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }

    pub(crate) fn from_sha256(digest: [u8; 32]) -> Self {
        Self {
            algorithm: ContentHashAlgorithm::Sha256,
            digest,
        }
    }

    pub(in crate::content) fn encode_into(self, bytes: &mut Vec<u8>) {
        bytes.push(self.algorithm.tag());
        bytes.extend_from_slice(&self.digest);
    }
}

impl fmt::Debug for ContentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ContentId({self})")
    }
}

impl fmt::Display for ContentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("sha256:")?;
        write_hex(formatter, &self.digest)
    }
}
