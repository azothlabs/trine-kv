use super::{Error, Result, fmt, write_hex};

/// Globally random identity of one upload attempt.
///
/// Upload identity is temporary transfer state. It is deliberately distinct
/// from [`crate::ContentId`], which is known only after the complete byte
/// sequence has been hashed or supplied and verified.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UploadId(pub(in crate::content) [u8; 16]);

impl UploadId {
    /// Creates a new random upload identity for caller-controlled idempotency.
    ///
    /// Generate and persist this identity before starting a transfer when an
    /// uncertain response must be retried through
    /// [`Db::begin_content_upload_with_id`](crate::Db::begin_content_upload_with_id).
    /// The identity is not secret and does not itself authorize content access.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when the operating system cannot provide
    /// cryptographic randomness. No durable state is created by this method.
    pub fn new() -> Result<Self> {
        Self::generate()
    }

    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("upload identity entropy: {error}")))?;
        Ok(Self(bytes))
    }

    pub(crate) const fn bytes(self) -> [u8; 16] {
        self.0
    }

    /// Reconstructs an upload identity from its portable 16-byte form.
    ///
    /// Upload identities are not secrets. Persist this value when an upload
    /// must be resumed after the current process or database handle is gone.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable 16-byte representation.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for UploadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "UploadId({self})")
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}
