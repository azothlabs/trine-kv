use std::ops::Bound;

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
}
