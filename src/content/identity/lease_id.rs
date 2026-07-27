use super::{CONTENT_LEASE_ID_VERSION, Error, Result, fmt, write_hex};

/// Opaque authenticated owner scope supplied by the database layer.
///
/// This identity can represent a project, tenant, or another authorization
/// boundary. Trine KV only requires exact equality when a token is consumed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerScopeId(pub(in crate::content) [u8; 16]);

impl OwnerScopeId {
    /// Reconstructs an identity from its portable bytes.
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

impl fmt::Debug for OwnerScopeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerScopeId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Opaque higher-layer owner of one short-lived content read lease.
///
/// Trine KV persists and compares this identity but assigns it no Principal,
/// tenant, or Policy meaning. The caller must authorize before opening or
/// renewing a leased handle.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentLeaseOwnerId(pub(in crate::content) [u8; 16]);

impl ContentLeaseOwnerId {
    /// Reconstructs an opaque lease owner from portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the opaque owner bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentLeaseOwnerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentLeaseOwnerId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Generated identity of one durable short-lived content read lease.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentLeaseId(pub(in crate::content) [u8; 16]);

impl ContentLeaseId {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("content lease entropy: {error}")))?;
        bytes[0] = CONTENT_LEASE_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned lease identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the first byte carries an
    /// unknown identity format.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_LEASE_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!("unsupported content lease identity version {}", bytes[0]),
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the versioned portable identity bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentLeaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentLeaseId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ContentLeaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}
