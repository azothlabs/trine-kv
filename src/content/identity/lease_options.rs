use super::{ContentLeaseOwnerId, Duration, Error, Result};

/// Options for opening a sealed `ContentObject` under a short-lived read lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentLeaseOptions {
    pub(in crate::content) owner_id: ContentLeaseOwnerId,
    pub(in crate::content) ttl: Duration,
}

impl ContentLeaseOptions {
    /// Creates lease options for an already-authorized opaque owner.
    ///
    /// `ttl` is rounded down to whole milliseconds and must be at least one
    /// millisecond. The deadline is computed during leased-open acquisition,
    /// immediately before Trine KV stages the durable record.
    #[must_use]
    pub const fn new(owner_id: ContentLeaseOwnerId, ttl: Duration) -> Self {
        Self { owner_id, ttl }
    }

    /// Returns the opaque higher-layer owner identity.
    #[must_use]
    pub const fn owner_id(self) -> ContentLeaseOwnerId {
        self.owner_id
    }

    /// Returns the requested lease lifetime.
    #[must_use]
    pub const fn ttl(self) -> Duration {
        self.ttl
    }

    pub(crate) fn ttl_ms(self) -> Result<u64> {
        let millis = u64::try_from(self.ttl.as_millis()).map_err(|_| {
            Error::invalid_options("content lease lifetime milliseconds exceed u64::MAX")
        })?;
        if millis == 0 {
            return Err(Error::invalid_options(
                "content lease lifetime must be at least one millisecond",
            ));
        }
        Ok(millis)
    }
}
