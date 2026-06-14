use std::ops::Range;

use crate::{
    blob::{self, ValueRef},
    error::{Error, Result},
    internal_key::InternalKey,
    stats::BlobReadMetrics,
    storage::StorageReadBackend,
    types::Value,
};
use bytes::Bytes;

/// Value returned by `BucketReader::get`.
///
/// A `PointValue` may own its bytes or keep a shared reference to bytes inside
/// a decoded table block. Use `as_bytes` when you only need to inspect the
/// value, or `into_vec` when you need owned bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointValue {
    inner: PointValueInner,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PointValueInner {
    Owned(Value),
    Shared { bytes: Bytes, range: Range<usize> },
}

impl PointValue {
    /// Returns the value bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match &self.inner {
            PointValueInner::Owned(bytes) => bytes,
            PointValueInner::Shared { bytes, range } => &bytes[range.start..range.end],
        }
    }

    /// Returns owned value bytes.
    ///
    /// This is allocation-free when the value is already owned, and copies when
    /// the value is backed by shared table-block bytes.
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        match self.inner {
            PointValueInner::Owned(bytes) => bytes,
            PointValueInner::Shared { bytes, range } => bytes[range].to_vec(),
        }
    }

    /// Returns an owned `Value`.
    ///
    /// This is equivalent to `into_vec`.
    #[must_use]
    pub fn into_value(self) -> Value {
        self.into_vec()
    }

    #[must_use]
    pub(crate) fn from_owned(bytes: Value) -> Self {
        Self {
            inner: PointValueInner::Owned(bytes),
        }
    }

    pub(crate) fn from_shared(bytes: Bytes, range: Range<usize>) -> Result<Self> {
        if range.start > range.end || range.end > bytes.len() {
            return Err(Error::Corruption {
                message: "point value range outside data block".to_owned(),
            });
        }
        Ok(Self {
            inner: PointValueInner::Shared { bytes, range },
        })
    }
}

impl AsRef<[u8]> for PointValue {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PointValueSource {
    Value(PointValue),
    Blob(ValueRef),
}

impl PointValueSource {
    pub(crate) fn from_value_ref(value: ValueRef) -> Self {
        match value {
            ValueRef::Inline(bytes) => Self::Value(PointValue::from_owned(bytes)),
            ValueRef::BlobIndex(_) | ValueRef::Blob { .. } => Self::Blob(value),
        }
    }

    pub(crate) fn from_shared(bytes: Bytes, range: Range<usize>) -> Result<Self> {
        PointValue::from_shared(bytes, range).map(Self::Value)
    }

    pub(crate) fn into_point_value(
        self,
        internal_key: &InternalKey,
        db_path: Option<&std::path::Path>,
        blob_reads: Option<&BlobReadMetrics>,
    ) -> Result<PointValue> {
        match self {
            Self::Value(value) => Ok(value),
            Self::Blob(value) => {
                let db_path = db_path.ok_or_else(|| Error::Corruption {
                    message: "in-memory database cannot read blob value references".to_owned(),
                })?;
                let bytes = blob::read_value_for_internal_key(db_path, &value, Some(internal_key))?;
                if let Some(blob_reads) = blob_reads {
                    blob_reads.record(bytes.len() as u64);
                }
                Ok(PointValue::from_owned(bytes))
            }
        }
    }

    pub(crate) async fn into_point_value_with_backend_async<B>(
        self,
        backend: &B,
        internal_key: &InternalKey,
        db_path: Option<&std::path::Path>,
        blob_reads: Option<&BlobReadMetrics>,
    ) -> Result<PointValue>
    where
        B: StorageReadBackend,
    {
        match self {
            Self::Value(value) => Ok(value),
            Self::Blob(value) => {
                let db_path = db_path.ok_or_else(|| Error::Corruption {
                    message: "in-memory database cannot read blob value references".to_owned(),
                })?;
                let bytes = blob::read_value_for_internal_key_with_backend_async(
                    backend,
                    db_path,
                    &value,
                    Some(internal_key),
                )
                .await?;
                if let Some(blob_reads) = blob_reads {
                    blob_reads.record(bytes.len() as u64);
                }
                Ok(PointValue::from_owned(bytes))
            }
        }
    }
}
