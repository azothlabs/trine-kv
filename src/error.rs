use std::{error, fmt, io};

use crate::options::DurabilityMode;

/// Convenient result alias used by Trine KV APIs.
pub type Result<T> = std::result::Result<T, Error>;

/// Error returned by database, storage, recovery, and transaction operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Underlying I/O error from the selected storage backend.
    Io(io::Error),
    /// Durable data failed an integrity or consistency check.
    Corruption {
        /// Human-readable corruption detail.
        message: String,
    },
    /// Bytes could not be decoded as a valid Trine storage record.
    InvalidFormat {
        /// Human-readable decode failure detail.
        message: String,
    },
    /// Bytes use a storage format that this crate version does not support.
    UnsupportedFormat {
        /// Human-readable unsupported-format detail.
        message: String,
    },
    /// The requested codec is not available in this build.
    CodecUnavailable {
        /// Codec name or identifier that was requested.
        codec: String,
    },
    /// An optimistic transaction conflicted with a committed write.
    Conflict {
        /// Human-readable conflict detail.
        message: String,
    },
    /// The database was opened read-only and a write was requested.
    ReadOnly,
    /// The database handle is closed.
    Closed,
    /// The configured runtime cannot accept the requested work now.
    RuntimeBusy {
        /// Human-readable runtime capacity detail.
        message: String,
    },
    /// A named bucket required by durable metadata was not found.
    BucketMissing {
        /// Missing bucket name.
        name: String,
    },
    /// Options were invalid or inconsistent.
    InvalidOptions {
        /// Human-readable options failure detail.
        message: String,
    },
    /// A Trine feature is unavailable in the current runtime or build.
    Unsupported {
        /// Feature name that is unavailable.
        feature: &'static str,
    },
    /// The selected storage backend does not provide a required capability.
    UnsupportedBackend {
        /// Backend capability that is unavailable.
        feature: &'static str,
    },
    /// The selected storage backend cannot provide the requested durability.
    UnsupportedDurability {
        /// Durability mode requested by the caller.
        requested: DurabilityMode,
    },
}

impl Error {
    /// Creates an unsupported-feature error.
    #[must_use]
    pub const fn unsupported(feature: &'static str) -> Self {
        Self::Unsupported { feature }
    }

    /// Creates an unsupported-backend error.
    #[must_use]
    pub const fn unsupported_backend(feature: &'static str) -> Self {
        Self::UnsupportedBackend { feature }
    }

    /// Creates an unsupported-durability error.
    #[must_use]
    pub const fn unsupported_durability(requested: DurabilityMode) -> Self {
        Self::UnsupportedDurability { requested }
    }

    /// Creates an invalid-options error.
    #[must_use]
    pub fn invalid_options(message: impl Into<String>) -> Self {
        Self::InvalidOptions {
            message: message.into(),
        }
    }

    /// Creates a runtime-busy error.
    #[must_use]
    pub fn runtime_busy(message: impl Into<String>) -> Self {
        Self::RuntimeBusy {
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "io error: {error}"),
            Self::Corruption { message } => write!(formatter, "corruption: {message}"),
            Self::InvalidFormat { message } => write!(formatter, "invalid format: {message}"),
            Self::UnsupportedFormat { message } => {
                write!(formatter, "unsupported format: {message}")
            }
            Self::CodecUnavailable { codec } => write!(formatter, "codec unavailable: {codec}"),
            Self::Conflict { message } => write!(formatter, "transaction conflict: {message}"),
            Self::ReadOnly => formatter.write_str("database is read-only"),
            Self::Closed => formatter.write_str("database is closed"),
            Self::RuntimeBusy { message } => write!(formatter, "runtime busy: {message}"),
            Self::BucketMissing { name } => write!(formatter, "bucket is missing: {name}"),
            Self::InvalidOptions { message } => write!(formatter, "invalid options: {message}"),
            Self::Unsupported { feature } => write!(formatter, "unsupported feature: {feature}"),
            Self::UnsupportedBackend { feature } => {
                write!(formatter, "unsupported storage backend feature: {feature}")
            }
            Self::UnsupportedDurability { requested } => {
                write!(
                    formatter,
                    "unsupported durability mode: {}",
                    requested.as_str()
                )
            }
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
