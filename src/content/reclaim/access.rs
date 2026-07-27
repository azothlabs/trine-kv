use super::{CONTENT_ACCESS_BARRIER_ID_VERSION, Error, Result, StorageDomainId, fmt, write_hex};

/// Versioned identity of one irreversible leased-only content-access barrier.
///
/// The identity makes an interrupted barrier publication discoverable and
/// idempotent. It is scoped by the [`StorageDomainId`] passed to
/// [`Db::enforce_content_leased_only`](crate::Db::enforce_content_leased_only),
/// rather than being a database-wide authorization identity.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentAccessBarrierId([u8; 16]);

impl ContentAccessBarrierId {
    /// Generates a new identity from operating-system entropy.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when secure entropy is unavailable.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            Error::runtime_busy(format!("content access-barrier entropy: {error}"))
        })?;
        bytes[0] = CONTENT_ACCESS_BARRIER_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned portable identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when byte zero names an unknown
    /// identity format.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_ACCESS_BARRIER_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!(
                    "unsupported content access-barrier identity version {}",
                    bytes[0]
                ),
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

impl fmt::Debug for ContentAccessBarrierId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentAccessBarrierId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ContentAccessBarrierId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Persisted access policy for one physical storage domain.
///
/// `CompatibleUnleased` is the default for domains without a barrier.
/// `LeasedOnly` means every new ordinary
/// [`Db::open_content`](crate::Db::open_content) call fails with
/// [`Error::ContentLeaseRequired`]; callers must use
/// [`Db::open_content_leased`](crate::Db::open_content_leased). The transition
/// is irreversible because reverting it could race physical reclamation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContentAccessMode {
    /// Compatible mode in which ordinary unleased opens remain available.
    CompatibleUnleased,
    /// New content opens require durable read leases.
    LeasedOnly {
        /// Durable identity of the barrier that established this mode.
        barrier_id: ContentAccessBarrierId,
    },
}

/// Durable coordinate returned after leased-only access is fully recorded.
///
/// The backend barrier becomes visible before the protected commit coordinate
/// is published. Therefore this value proves that new unleased opens are
/// fenced and gives later lifecycle work a local ordering point. It does not
/// prove that handles opened before the barrier have drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentAccessBarrier {
    storage_domain_id: StorageDomainId,
    barrier_id: ContentAccessBarrierId,
    enforced_at: crate::ReadVersion,
}

impl ContentAccessBarrier {
    pub(crate) const fn new(
        storage_domain_id: StorageDomainId,
        barrier_id: ContentAccessBarrierId,
        enforced_at: crate::ReadVersion,
    ) -> Self {
        Self {
            storage_domain_id,
            barrier_id,
            enforced_at,
        }
    }

    /// Returns the physical lifecycle domain fenced by this barrier.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns the durable barrier identity.
    #[must_use]
    pub const fn barrier_id(self) -> ContentAccessBarrierId {
        self.barrier_id
    }

    /// Returns the local commit sequence that recorded the barrier coordinate.
    ///
    /// This sequence is useful for ordering work in the same database instance;
    /// it is not a portable identity and does not prove reader drain.
    #[must_use]
    pub const fn enforced_at(self) -> crate::ReadVersion {
        self.enforced_at
    }
}
