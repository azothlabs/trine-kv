use std::{marker::PhantomData, sync::Arc};

use crate::{
    db::Db,
    error::Result,
    iterator::{Direction, Iter, LazyIter},
    lsm::{LsmPointReadSnapshot, LsmTree},
    options::{BucketOptions, WriteOptions},
    point_value::PointValue,
    snapshot::Snapshot,
    types::{CommitInfo, KeyRange, Sequence, Value},
    write_batch::WriteBatch,
};

pub(crate) const DEFAULT_BUCKET_NAME: &str = "default";

/// Name of an optional bucket returned through `Db::bucket`.
///
/// `Db` validates bucket names when creating them. The reserved default bucket
/// is reached through direct `Db` helpers or `Db::default_bucket`.
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
    state: Arc<LsmTree>,
}

/// Point-read handle bound to one bucket and one snapshot.
///
/// Create one with `Bucket::reader` when a workload performs many point reads
/// under the same snapshot. The reader keeps a stable set of memtable and table
/// sources, so repeated `get` calls avoid reacquiring the bucket's version
/// lock.
#[derive(Debug)]
pub struct BucketReader<'snapshot> {
    db: Db,
    state: Arc<LsmTree>,
    read_snapshot: LsmPointReadSnapshot,
    read_sequence: Sequence,
    _read_pin: Option<Snapshot>,
    _snapshot: PhantomData<&'snapshot Snapshot>,
}

impl Bucket {
    pub(crate) fn new(
        db: Db,
        name: BucketName,
        options: BucketOptions,
        state: Arc<LsmTree>,
    ) -> Self {
        Self {
            db,
            name,
            options,
            state,
        }
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
        self.db.get_at_state_with_pin_state(
            &self.state,
            key,
            self.db.last_committed_sequence(),
            false,
        )
    }

    /// Reads `key` at the sequence pinned by `snapshot`.
    pub fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.db.get_at_state_with_pin_state(
            &self.state,
            key,
            snapshot.read_sequence(),
            snapshot.is_pinned(),
        )
    }

    /// Creates a point-read handle for repeated reads under `snapshot`.
    ///
    /// Use `BucketReader::get` when the caller can inspect bytes through
    /// `PointValue::as_bytes` and does not need an owned `Vec<u8>`.
    pub fn reader<'snapshot>(
        &self,
        snapshot: &'snapshot Snapshot,
    ) -> Result<BucketReader<'snapshot>> {
        self.db.reader_for_state(&self.state, snapshot)
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
        if self.name.as_str() == DEFAULT_BUCKET_NAME {
            batch.put(key, value);
        } else {
            batch.put_bucket(self.name.as_str(), key, value)?;
        }
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
        if self.name.as_str() == DEFAULT_BUCKET_NAME {
            batch.delete(key);
        } else {
            batch.delete_bucket(self.name.as_str(), key)?;
        }
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
        if self.name.as_str() == DEFAULT_BUCKET_NAME {
            batch.delete_range(range);
        } else {
            batch.delete_range_bucket(self.name.as_str(), range)?;
        }
        self.db.write(batch, options)
    }

    /// Returns a forward iterator over visible rows in `range`.
    pub fn range(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, self.db.last_committed_sequence(), Direction::Forward)
    }

    /// Returns a forward iterator whose blob values are read on demand.
    pub fn range_lazy(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(range, self.db.last_committed_sequence(), Direction::Forward)
    }

    /// Returns a forward iterator over `range` at `snapshot`.
    pub fn range_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, snapshot.read_sequence(), Direction::Forward)
    }

    /// Returns a forward value-lazy iterator at `snapshot`.
    pub fn range_lazy_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(range, snapshot.read_sequence(), Direction::Forward)
    }

    /// Returns a reverse iterator over visible rows in `range`.
    pub fn range_reverse(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, self.db.last_committed_sequence(), Direction::Reverse)
    }

    /// Returns a reverse iterator whose blob values are read on demand.
    pub fn range_lazy_reverse(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(range, self.db.last_committed_sequence(), Direction::Reverse)
    }

    /// Returns a reverse iterator over `range` at `snapshot`.
    pub fn range_reverse_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, snapshot.read_sequence(), Direction::Reverse)
    }

    /// Returns a reverse value-lazy iterator at `snapshot`.
    pub fn range_lazy_reverse_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(range, snapshot.read_sequence(), Direction::Reverse)
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

    /// Returns a forward prefix iterator whose blob values are read on demand.
    pub fn prefix_lazy(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
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

    /// Returns a forward value-lazy prefix iterator at `snapshot`.
    pub fn prefix_lazy_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(&prefix, snapshot.read_sequence(), Direction::Forward)
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

    /// Returns a reverse prefix iterator whose blob values are read on demand.
    pub fn prefix_lazy_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
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

    /// Returns a reverse value-lazy prefix iterator at `snapshot`.
    pub fn prefix_lazy_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(&prefix, snapshot.read_sequence(), Direction::Reverse)
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

    fn range_lazy_at_sequence(
        &self,
        range: &KeyRange,
        read_sequence: crate::types::Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        self.db
            .range_lazy_at_sequence(self.name.as_str(), range, read_sequence, direction)
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

    fn prefix_lazy_at_sequence(
        &self,
        prefix: &[u8],
        read_sequence: crate::types::Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        self.db
            .prefix_lazy_at_sequence(self.name.as_str(), prefix, read_sequence, direction)
    }
}

impl BucketReader<'_> {
    pub(crate) fn new(
        db: Db,
        state: Arc<LsmTree>,
        read_snapshot: LsmPointReadSnapshot,
        read_sequence: Sequence,
        read_pin: Option<Snapshot>,
    ) -> Self {
        Self {
            db,
            state,
            read_snapshot,
            read_sequence,
            _read_pin: read_pin,
            _snapshot: PhantomData,
        }
    }

    /// Reads `key` using the sources pinned when this reader was created.
    ///
    /// This returns a `PointValue` so inline table values can be inspected
    /// without first copying them into an owned `Vec<u8>`.
    pub fn get(&self, key: &[u8]) -> Result<Option<PointValue>> {
        self.db.get_value_at_state_snapshot_with_pin_state(
            &self.state,
            &self.read_snapshot,
            key,
            self.read_sequence,
            true,
        )
    }

    /// Reads `key` and returns an owned value.
    ///
    /// Use this when the caller needs the same owned-value shape as `Db::get`
    /// or `Bucket::get`.
    pub fn get_owned(&self, key: &[u8]) -> Result<Option<Value>> {
        self.get(key)?
            .map(|value| Ok(value.into_value()))
            .transpose()
    }
}
