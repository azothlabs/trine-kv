use std::ops::Bound;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sequence(u64);

impl Sequence {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

pub type Value = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Value,
}

impl KeyValue {
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
}

impl KeyRange {
    #[must_use]
    pub const fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    sequence: Sequence,
}

impl CommitInfo {
    #[must_use]
    pub const fn new(sequence: Sequence) -> Self {
        Self { sequence }
    }

    #[must_use]
    pub const fn sequence(self) -> Sequence {
        self.sequence
    }
}
