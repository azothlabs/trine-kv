use std::{error, fmt, io};

use crate::options::DurabilityMode;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    Io(io::Error),
    Corruption { message: String },
    InvalidFormat { message: String },
    UnsupportedFormat { message: String },
    CodecUnavailable { codec: String },
    Conflict { message: String },
    ReadOnly,
    Closed,
    RuntimeBusy { message: String },
    BucketMissing { name: String },
    InvalidOptions { message: String },
    Unsupported { feature: &'static str },
    UnsupportedBackend { feature: &'static str },
    UnsupportedDurability { requested: DurabilityMode },
}

impl Error {
    #[must_use]
    pub const fn unsupported(feature: &'static str) -> Self {
        Self::Unsupported { feature }
    }

    #[must_use]
    pub const fn unsupported_backend(feature: &'static str) -> Self {
        Self::UnsupportedBackend { feature }
    }

    #[must_use]
    pub const fn unsupported_durability(requested: DurabilityMode) -> Self {
        Self::UnsupportedDurability { requested }
    }

    #[must_use]
    pub fn invalid_options(message: impl Into<String>) -> Self {
        Self::InvalidOptions {
            message: message.into(),
        }
    }

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
