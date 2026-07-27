use super::{Error, Result, UPLOAD_TOKEN_VERSION, fmt, write_hex};

/// Opaque bearer authority returned only after an upload is sealed.
///
/// The 32 random bytes are secret. Logging implementations deliberately redact
/// them; callers that persist or transmit a token should protect it like any
/// other short-lived capability.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UploadToken(pub(in crate::content) [u8; 32]);

impl UploadToken {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("upload token entropy: {error}")))?;
        Ok(Self(bytes))
    }

    pub(crate) const fn secret(self) -> [u8; 32] {
        self.0
    }

    /// Decodes the versioned 33-byte bearer representation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the first byte names an
    /// unknown token format version.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        if bytes[0] != UPLOAD_TOKEN_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!("unsupported upload token version {}", bytes[0]),
            });
        }
        let mut secret = [0_u8; 32];
        secret.copy_from_slice(&bytes[1..]);
        Ok(Self(secret))
    }

    /// Returns the versioned 33-byte bearer representation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 33] {
        let mut bytes = [0_u8; 33];
        bytes[0] = UPLOAD_TOKEN_VERSION;
        bytes[1..].copy_from_slice(&self.0);
        bytes
    }
}

impl fmt::Debug for UploadToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UploadToken([REDACTED])")
    }
}

/// Opaque database-layer change identity used for idempotent consumption.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentChangeId(pub(in crate::content) [u8; 16]);

impl ContentChangeId {
    /// Reconstructs a change identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentChangeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentChangeId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}
