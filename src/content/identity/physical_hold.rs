use super::{
    CONTENT_PHYSICAL_HOLD_ID_VERSION, Duration, Error, Result, duration_millis, fmt, write_hex,
};

/// Classifies why physical content bytes must remain available.
///
/// The class is operational metadata, not authorization. Every class enters
/// the same reclaim fence; callers must not infer weaker protection from one
/// variant than another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ContentPhysicalHoldKind {
    /// A storage migration still reads or copies a representation.
    Migration,
    /// A backup workflow has not durably completed its copy.
    Backup,
    /// A repair workflow needs the current bytes or replicas.
    Repair,
    /// A storage provider operation still references provider-side objects.
    Provider,
    /// An explicit operator or compliance hold remains in force.
    Administrative,
    /// Durable asynchronous processing still depends on the source bytes.
    Processing,
    /// A verified device-local offline cache entry must remain readable.
    Offline,
}

impl ContentPhysicalHoldKind {
    pub(in crate::content) const fn tag(self) -> u8 {
        match self {
            Self::Migration => 1,
            Self::Backup => 2,
            Self::Repair => 3,
            Self::Provider => 4,
            Self::Administrative => 5,
            Self::Processing => 6,
            Self::Offline => 7,
        }
    }

    pub(in crate::content) fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            1 => Ok(Self::Migration),
            2 => Ok(Self::Backup),
            3 => Ok(Self::Repair),
            4 => Ok(Self::Provider),
            5 => Ok(Self::Administrative),
            6 => Ok(Self::Processing),
            7 => Ok(Self::Offline),
            _ => Err(Error::UnsupportedFormat {
                message: format!("unsupported content physical-hold kind {tag}"),
            }),
        }
    }
}

impl fmt::Display for ContentPhysicalHoldKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Migration => "migration",
            Self::Backup => "backup",
            Self::Repair => "repair",
            Self::Provider => "provider",
            Self::Administrative => "administrative",
            Self::Processing => "processing",
            Self::Offline => "offline",
        })
    }
}

/// Generated identity of one durable physical content hold.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentPhysicalHoldId(pub(in crate::content) [u8; 16]);

impl ContentPhysicalHoldId {
    /// Generates a new versioned hold identity from operating-system entropy.
    ///
    /// Callers should durably retain this identity before acquiring an
    /// until-released hold. Passing the same identity to acquisition after a
    /// lost response makes the operation idempotent and recoverable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when secure entropy is unavailable.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            Error::runtime_busy(format!("content physical-hold entropy: {error}"))
        })?;
        bytes[0] = CONTENT_PHYSICAL_HOLD_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned portable hold identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when byte zero names an unknown
    /// identity format.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_PHYSICAL_HOLD_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!(
                    "unsupported content physical-hold identity version {}",
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

impl fmt::Debug for ContentPhysicalHoldId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentPhysicalHoldId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ContentPhysicalHoldId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Opaque higher-layer owner of a physical content hold.
///
/// Trine KV compares this identity during resume, renewal, and release but does
/// not interpret it as a Principal, tenant, workflow, or Policy identity.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentPhysicalHoldOwnerId(pub(in crate::content) [u8; 16]);

impl ContentPhysicalHoldOwnerId {
    /// Reconstructs an opaque owner from portable bytes.
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

impl fmt::Debug for ContentPhysicalHoldOwnerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentPhysicalHoldOwnerId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::content) enum ContentPhysicalHoldLifetime {
    Expiring(Duration),
    UntilReleased,
}

/// Options for acquiring one durable physical content hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentPhysicalHoldOptions {
    pub(in crate::content) kind: ContentPhysicalHoldKind,
    pub(in crate::content) owner_id: ContentPhysicalHoldOwnerId,
    pub(in crate::content) lifetime: ContentPhysicalHoldLifetime,
}

impl ContentPhysicalHoldOptions {
    /// Creates an expiring hold for an already-authorized workflow.
    ///
    /// `ttl` is rounded down to whole milliseconds and must retain at least one
    /// millisecond. An expired hold is inert and cannot be renewed.
    #[must_use]
    pub const fn expiring(
        kind: ContentPhysicalHoldKind,
        owner_id: ContentPhysicalHoldOwnerId,
        ttl: Duration,
    ) -> Self {
        Self {
            kind,
            owner_id,
            lifetime: ContentPhysicalHoldLifetime::Expiring(ttl),
        }
    }

    /// Creates a hold that remains active until an explicit durable release.
    ///
    /// This form is appropriate only when the owner has a recovery path that
    /// can resume and release the hold after a process crash.
    #[must_use]
    pub const fn until_released(
        kind: ContentPhysicalHoldKind,
        owner_id: ContentPhysicalHoldOwnerId,
    ) -> Self {
        Self {
            kind,
            owner_id,
            lifetime: ContentPhysicalHoldLifetime::UntilReleased,
        }
    }

    /// Returns the operational hold class.
    #[must_use]
    pub const fn kind(self) -> ContentPhysicalHoldKind {
        self.kind
    }

    /// Returns the opaque higher-layer owner.
    #[must_use]
    pub const fn owner_id(self) -> ContentPhysicalHoldOwnerId {
        self.owner_id
    }

    /// Returns the requested expiring lifetime, or `None` for explicit release.
    #[must_use]
    pub const fn ttl(self) -> Option<Duration> {
        match self.lifetime {
            ContentPhysicalHoldLifetime::Expiring(ttl) => Some(ttl),
            ContentPhysicalHoldLifetime::UntilReleased => None,
        }
    }

    pub(crate) fn expires_at_unix_ms(self, now_unix_ms: u64) -> Result<u64> {
        let ContentPhysicalHoldLifetime::Expiring(ttl) = self.lifetime else {
            return Ok(0);
        };
        let ttl_ms = duration_millis(ttl, "content physical-hold lifetime")?;
        now_unix_ms
            .checked_add(ttl_ms)
            .ok_or_else(|| Error::invalid_options("content physical-hold expiry overflow"))
    }
}
