use std::ops::Bound;

/// Stable numeric cursor for a committed database state.
///
/// `ReadVersion` is the public identifier callers use when they want to open a
/// [`Snapshot`](crate::Snapshot) at a specific historical state. Values are
/// database-scoped: a read version is meaningful only for the database lineage
/// that produced it, even if another database happens to contain the same
/// numeric value.
///
/// Use [`Db::latest_read_version`](crate::Db::latest_read_version) to capture
/// the newest visible state, and [`Db::snapshot_at`](crate::Db::snapshot_at) to
/// validate and pin a version before reading. Versions older than
/// [`Db::oldest_retained_read_version`](crate::Db::oldest_retained_read_version)
/// are expired and must not be read by falling back to latest.
///
/// # Examples
///
/// ```rust
/// use trine_kv::{Db, DbOptions, ReadVersion};
///
/// # fn main() -> trine_kv::Result<()> {
/// let db = Db::open_sync(DbOptions::memory())?;
/// assert_eq!(db.latest_read_version(), ReadVersion::ZERO);
///
/// db.put_sync(b"k", b"v1")?;
/// let version = db.latest_read_version();
/// let keep_version = db.snapshot_at(version)?;
///
/// db.put_sync(b"k", b"v2")?;
/// let snapshot = db.snapshot_at(version)?;
/// assert_eq!(db.get_at_sync(&snapshot, b"k")?, Some(b"v1".to_vec()));
/// drop(keep_version);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadVersion(u64);

impl ReadVersion {
    /// Read version for the empty database state before the first successful
    /// state-changing write.
    pub const ZERO: Self = Self(0);

    /// Creates a read version from its stable numeric cursor value.
    ///
    /// The value is suitable for application-owned cursors that were
    /// previously obtained from this same database lineage. Creating a
    /// `ReadVersion` from a number does not prove the version is still
    /// retained; call [`Db::snapshot_at`](crate::Db::snapshot_at) to validate
    /// it before reading.
    #[must_use]
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the stable numeric cursor value.
    ///
    /// Applications may persist this value and later rebuild it with
    /// [`ReadVersion::from_u64`]. The number is a public cursor, not a promise
    /// about Trine's internal commit allocation machinery.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub(crate) const fn from_sequence(sequence: Sequence) -> Self {
        Self(sequence.get())
    }

    pub(crate) const fn to_sequence(self) -> Sequence {
        Sequence::new(self.0)
    }
}

/// Monotonic commit sequence used for MVCC visibility.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sequence(u64);

impl Sequence {
    /// Sequence value used before the first committed write.
    pub const ZERO: Self = Self(0);

    /// Creates a sequence from its raw numeric value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw numeric sequence value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next sequence, or `None` if the value would overflow.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Value bytes stored for a key.
pub type Value = Vec<u8>;

/// Owned key/value row returned by eager iterators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    /// User key bytes.
    pub key: Vec<u8>,
    /// Value bytes visible for the key.
    pub value: Value,
}

impl KeyValue {
    /// Creates an owned key/value row.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// User-key range used by range scans and range deletes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    /// Inclusive, exclusive, or unbounded start key.
    pub start: Bound<Vec<u8>>,
    /// Inclusive, exclusive, or unbounded end key.
    pub end: Bound<Vec<u8>>,
}

impl KeyRange {
    /// Returns an unbounded range over all user keys.
    #[must_use]
    pub const fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// Creates a half-open range `[start, end)`.
    #[must_use]
    pub fn half_open(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self {
            start: Bound::Included(start.into()),
            end: Bound::Excluded(end.into()),
        }
    }
}

impl Default for KeyRange {
    fn default() -> Self {
        Self::all()
    }
}

/// Information returned after a write becomes committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    sequence: Sequence,
}

impl CommitInfo {
    /// Creates commit information for `sequence`.
    #[must_use]
    pub const fn new(sequence: Sequence) -> Self {
        Self { sequence }
    }

    /// Returns the commit sequence assigned to the write.
    #[must_use]
    pub const fn sequence(self) -> Sequence {
        self.sequence
    }

    /// Returns the read version made visible by this write.
    ///
    /// For a state-changing atomic write, every operation in the write becomes
    /// visible at this read version. An accepted empty write batch does not
    /// create a new database state and returns the latest read version that was
    /// already visible.
    ///
    /// [`CommitInfo::sequence`] remains available for lower-level callers that
    /// already depend on engine sequence terminology. New user-facing code
    /// should prefer `read_version`.
    #[must_use]
    pub const fn read_version(self) -> ReadVersion {
        ReadVersion::from_sequence(self.sequence)
    }
}
