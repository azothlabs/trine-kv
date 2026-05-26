use crate::{
    db::Db,
    error::Result,
    iterator::{Direction, Iter},
    options::{BucketOptions, WriteOptions},
    snapshot::Snapshot,
    types::{CommitInfo, KeyRange, Value},
    write_batch::WriteBatch,
};

/// Name of an optional bucket opened through `Db::open_bucket`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BucketName(String);

impl BucketName {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for BucketName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for BucketName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Handle for an optional named bucket.
///
/// A bucket has its own options, memtables, `SSTables`, filters, and compaction
/// state. Most applications can use the direct `Db` helpers instead, which
/// target the built-in default bucket.
#[derive(Debug, Clone)]
pub struct Bucket {
    db: Db,
    name: BucketName,
    options: BucketOptions,
}

impl Bucket {
    pub(crate) const fn new(db: Db, name: BucketName, options: BucketOptions) -> Self {
        Self { db, name, options }
    }

    /// Returns the bucket name used in WAL and manifest metadata.
    #[must_use]
    pub fn name(&self) -> &BucketName {
        &self.name
    }

    /// Returns the fixed options this bucket was opened with.
    #[must_use]
    pub fn options(&self) -> &BucketOptions {
        &self.options
    }

    /// Reads the newest committed value for `key` from this bucket.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.db
            .get_at_sequence(self.name.as_str(), key, self.db.last_committed_sequence())
    }

    /// Reads `key` at the sequence pinned by `snapshot`.
    pub fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.db.get_at_with_pin_state(
            self.name.as_str(),
            key,
            snapshot.read_sequence(),
            snapshot.is_pinned(),
        )
    }

    /// Writes one key/value pair to this bucket using default write options.
    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        self.put_with_options(key, value, WriteOptions::default())
            .map(|_| ())
    }

    /// Writes one key/value pair and returns the commit information.
    pub fn put_with_options(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = WriteBatch::new();
        batch.put(self.name.as_str(), key, value);
        self.db.write(batch, options)
    }

    /// Adds a point delete for one key using default write options.
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.delete_with_options(key, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a point delete and returns the commit information.
    pub fn delete_with_options(
        &self,
        key: impl Into<Vec<u8>>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = WriteBatch::new();
        batch.delete(self.name.as_str(), key);
        self.db.write(batch, options)
    }

    /// Adds a range delete using default write options.
    pub fn delete_range(&self, range: KeyRange) -> Result<()> {
        self.delete_range_with_options(range, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a range delete and returns the commit information.
    pub fn delete_range_with_options(
        &self,
        range: KeyRange,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = WriteBatch::new();
        batch.delete_range(self.name.as_str(), range);
        self.db.write(batch, options)
    }

    /// Returns a forward iterator over visible rows in `range`.
    pub fn range(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, self.db.last_committed_sequence(), Direction::Forward)
    }

    /// Returns a forward iterator over `range` at `snapshot`.
    pub fn range_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, snapshot.read_sequence(), Direction::Forward)
    }

    /// Returns a reverse iterator over visible rows in `range`.
    pub fn range_reverse(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, self.db.last_committed_sequence(), Direction::Reverse)
    }

    /// Returns a reverse iterator over `range` at `snapshot`.
    pub fn range_reverse_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, snapshot.read_sequence(), Direction::Reverse)
    }

    /// Returns a forward iterator over rows whose keys begin with `prefix`.
    pub fn prefix(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            &prefix,
            self.db.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward prefix iterator at `snapshot`.
    pub fn prefix_at(&self, snapshot: &Snapshot, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(&prefix, snapshot.read_sequence(), Direction::Forward)
    }

    /// Returns a reverse iterator over rows whose keys begin with `prefix`.
    pub fn prefix_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            &prefix,
            self.db.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse prefix iterator at `snapshot`.
    pub fn prefix_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(&prefix, snapshot.read_sequence(), Direction::Reverse)
    }

    #[must_use]
    /// Builds an empty iterator with the requested direction.
    pub fn empty_iter(direction: Direction) -> Iter {
        Iter::empty(direction)
    }

    fn range_at_sequence(
        &self,
        range: &KeyRange,
        read_sequence: crate::types::Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.db
            .range_at_sequence(self.name.as_str(), range, read_sequence, direction)
    }

    fn prefix_at_sequence(
        &self,
        prefix: &[u8],
        read_sequence: crate::types::Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.db
            .prefix_at_sequence(self.name.as_str(), prefix, read_sequence, direction)
    }
}
