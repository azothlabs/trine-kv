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
    /// Creates a bucket name wrapper.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Returns the bucket name as a string slice.
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

/// Handle for one bucket.
///
/// A bucket has its own options, memtables, `SSTables`, filters, and compaction
/// state. Most applications can use the direct `Db` helpers instead, which
/// target the built-in default bucket.
///
/// Named buckets are created through [`Db::bucket_sync`], [`Db::bucket`],
/// [`Db::bucket_with_options_sync`], or [`Db::bucket_with_options`]. A bucket's
/// options are fixed when it is created; reopening it with different options is
/// rejected.
///
/// # Examples
///
/// ```rust
/// use trine_kv::Db;
///
/// # fn main() -> trine_kv::Result<()> {
/// let db = Db::open_sync(trine_kv::DbOptions::memory())?;
/// let users = db.bucket_sync("users")?;
///
/// users.put_sync(b"1", b"Ada")?;
/// assert_eq!(users.get_sync(b"1")?, Some(b"Ada".to_vec()));
/// # Ok(())
/// # }
/// ```
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
    ///
    /// This is the bucket-scoped form of [`Db::get_sync`]. It searches only
    /// this bucket's memtables and table files and returns owned value bytes.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes inside this bucket.
    pub fn get_sync(&self, key: &[u8]) -> Result<Option<Value>> {
        self.db.get_at_state_with_pin_state(
            &self.state,
            key,
            self.db.last_committed_sequence(),
            false,
        )
    }

    /// Reads many newest committed values from this bucket.
    ///
    /// The returned vector has exactly one entry for each input key, in input
    /// order. `Ok(None)` at a position means that key has no value visible at
    /// the read sequence captured for this batch, either because it was never
    /// written or because its newest visible record is a delete. Duplicate
    /// input keys produce duplicate result entries; this method does not
    /// reorder or deduplicate keys.
    ///
    /// A batch captures one committed read sequence and one set of point-read
    /// sources before reading the first key. That gives all keys a consistent
    /// view and avoids rebuilding the bucket read state for each key. Large
    /// blob-backed values are read before the method returns.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes inside this bucket. The slice may be empty; an
    ///   empty input returns an empty vector.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`](crate::Error::Closed) if the parent database
    /// is closed, plus storage or format errors encountered while reading
    /// tables or blob files. Any such error fails the whole batch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions};
    ///
    /// # fn main() -> trine_kv::Result<()> {
    /// let db = Db::open_sync(DbOptions::memory())?;
    /// let users = db.bucket_sync("users")?;
    /// users.put_sync(b"ada", b"Ada")?;
    /// users.put_sync(b"grace", b"Grace")?;
    ///
    /// let keys = [b"ada".as_slice(), b"missing".as_slice(), b"grace".as_slice()];
    /// let values = users.get_many_sync(&keys)?;
    /// assert_eq!(values, vec![Some(b"Ada".to_vec()), None, Some(b"Grace".to_vec())]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_many_sync<K>(&self, keys: &[K]) -> Result<Vec<Option<Value>>>
    where
        K: AsRef<[u8]>,
    {
        let reader = self.db.reader_for_state_keys_at_sequence(
            &self.state,
            keys,
            self.db.last_committed_sequence(),
            false,
        )?;
        reader.get_many_owned_sync(keys)
    }

    /// Reads `key` at the sequence pinned by `snapshot`.
    pub fn get_at_sync(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.db.get_at_state_with_pin_state(
            &self.state,
            key,
            snapshot.read_sequence(),
            snapshot.is_pinned(),
        )
    }

    /// Creates a point-read handle for repeated reads under `snapshot`.
    ///
    /// The reader captures the read sequence and a stable set of sources once,
    /// then reuses them for many point reads. That reduces repeated lock
    /// traffic for workloads that read many keys under one snapshot.
    ///
    /// Use [`BucketReader::get_sync`] or [`BucketReader::get`] when the caller
    /// can inspect bytes through [`PointValue::as_bytes`] and does not need an
    /// owned `Vec<u8>`.
    ///
    /// # Parameters
    ///
    /// - `snapshot`: snapshot that defines visibility for all reads made
    ///   through the returned reader.
    pub fn reader<'snapshot>(
        &self,
        snapshot: &'snapshot Snapshot,
    ) -> Result<BucketReader<'snapshot>> {
        self.db.reader_for_state(&self.state, snapshot)
    }

    /// Writes one key/value pair to this bucket using default write options.
    ///
    /// The write is committed through the parent database as a one-operation
    /// batch. Named bucket writes use the bucket name in WAL and manifest
    /// metadata; default bucket writes use the built-in default bucket name.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes in this bucket.
    /// - `value`: value bytes to store.
    pub fn put_sync(&self, key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        self.put_with_options_sync(key, value, WriteOptions::default())
            .map(|_| ())
    }

    /// Writes one key/value pair and returns the commit information.
    pub fn put_with_options_sync(
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
        self.db.write_sync(batch, options)
    }

    /// Adds a point delete for one key using default write options.
    pub fn delete_sync(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.delete_with_options_sync(key, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a point delete and returns the commit information.
    pub fn delete_with_options_sync(
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
        self.db.write_sync(batch, options)
    }

    /// Adds a range delete using default write options.
    pub fn delete_range_sync(&self, range: KeyRange) -> Result<()> {
        self.delete_range_with_options_sync(range, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a range delete and returns the commit information.
    pub fn delete_range_with_options_sync(
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
        self.db.write_sync(batch, options)
    }

    /// Returns a forward iterator over visible rows in `range`.
    ///
    /// This is the bucket-scoped form of [`Db::range_sync`]. Rows are returned
    /// in ascending byte order and filtered to the newest value visible at the
    /// sequence captured when the iterator is created.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range inside this bucket.
    pub fn range_sync(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, self.db.last_committed_sequence(), Direction::Forward)
    }

    /// Returns a forward iterator whose blob values are read on demand.
    pub fn range_lazy_sync(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(range, self.db.last_committed_sequence(), Direction::Forward)
    }

    /// Returns a forward iterator over `range` at `snapshot`.
    pub fn range_at_sync(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, snapshot.read_sequence(), Direction::Forward)
    }

    /// Returns a forward value-lazy iterator at `snapshot`.
    pub fn range_lazy_at_sync(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(range, snapshot.read_sequence(), Direction::Forward)
    }

    /// Returns a reverse iterator over visible rows in `range`.
    pub fn range_reverse_sync(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, self.db.last_committed_sequence(), Direction::Reverse)
    }

    /// Returns a reverse iterator whose blob values are read on demand.
    pub fn range_lazy_reverse_sync(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(range, self.db.last_committed_sequence(), Direction::Reverse)
    }

    /// Returns a reverse iterator over `range` at `snapshot`.
    pub fn range_reverse_at_sync(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(range, snapshot.read_sequence(), Direction::Reverse)
    }

    /// Returns a reverse value-lazy iterator at `snapshot`.
    pub fn range_lazy_reverse_at_sync(
        &self,
        snapshot: &Snapshot,
        range: &KeyRange,
    ) -> Result<LazyIter> {
        self.range_lazy_at_sequence(range, snapshot.read_sequence(), Direction::Reverse)
    }

    /// Returns a forward iterator over rows whose keys begin with `prefix`.
    pub fn prefix_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            &prefix,
            self.db.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward prefix iterator whose blob values are read on demand.
    pub fn prefix_lazy_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            &prefix,
            self.db.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward prefix iterator at `snapshot`.
    pub fn prefix_at_sync(&self, snapshot: &Snapshot, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(&prefix, snapshot.read_sequence(), Direction::Forward)
    }

    /// Returns a forward value-lazy prefix iterator at `snapshot`.
    pub fn prefix_lazy_at_sync(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(&prefix, snapshot.read_sequence(), Direction::Forward)
    }

    /// Returns a reverse iterator over rows whose keys begin with `prefix`.
    pub fn prefix_reverse_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            &prefix,
            self.db.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse prefix iterator whose blob values are read on demand.
    pub fn prefix_lazy_reverse_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            &prefix,
            self.db.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse prefix iterator at `snapshot`.
    pub fn prefix_reverse_at_sync(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(&prefix, snapshot.read_sequence(), Direction::Reverse)
    }

    /// Returns a reverse value-lazy prefix iterator at `snapshot`.
    pub fn prefix_lazy_reverse_at_sync(
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

/// Primary async bucket API. Synchronous callers can use the explicit
/// `*_sync` adapters above.
#[allow(clippy::unused_async)]
impl Bucket {
    /// Reads the newest committed value for `key` from this bucket.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.db
            .get_at_state_with_pin_state_async(
                &self.state,
                key,
                self.db.last_committed_sequence(),
                false,
            )
            .await
    }

    /// Reads many newest committed values from this bucket.
    ///
    /// This is the async form of [`Bucket::get_many_sync`]. It preserves input
    /// order, returns `None` for missing or deleted keys, and fails the whole
    /// batch on storage or format errors. The batch captures one committed read
    /// sequence and one set of point-read sources before reading the first key,
    /// so all returned values share one consistent view.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes inside this bucket. Empty input returns an
    ///   empty vector.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`](crate::Error::Closed) if the parent database
    /// is closed, plus storage or format errors encountered while reading
    /// tables or blob files. Any such error fails the whole batch.
    ///
    pub async fn get_many<K>(&self, keys: &[K]) -> Result<Vec<Option<Value>>>
    where
        K: AsRef<[u8]>,
    {
        let reader = self.db.reader_for_state_keys_at_sequence(
            &self.state,
            keys,
            self.db.last_committed_sequence(),
            false,
        )?;
        reader.get_many_owned(keys).await
    }

    /// Reads `key` at the sequence pinned by `snapshot`.
    pub async fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.db
            .get_at_state_with_pin_state_async(
                &self.state,
                key,
                snapshot.read_sequence(),
                snapshot.is_pinned(),
            )
            .await
    }

    /// Writes one key/value pair to this bucket using default write options.
    pub async fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        self.put_with_options(key, value, WriteOptions::default())
            .await
            .map(|_| ())
    }

    /// Writes one key/value pair and returns the commit information.
    pub async fn put_with_options(
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
        self.db.write(batch, options).await
    }

    /// Adds a point delete for one key using default write options.
    pub async fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.delete_with_options(key, WriteOptions::default())
            .await
            .map(|_| ())
    }

    /// Adds a point delete and returns the commit information.
    pub async fn delete_with_options(
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
        self.db.write(batch, options).await
    }

    /// Adds a range delete using default write options.
    pub async fn delete_range(&self, range: KeyRange) -> Result<()> {
        self.delete_range_with_options(range, WriteOptions::default())
            .await
            .map(|_| ())
    }

    /// Adds a range delete and returns the commit information.
    pub async fn delete_range_with_options(
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
        self.db.write(batch, options).await
    }

    /// Returns a forward iterator over visible rows in `range`.
    pub async fn range(&self, range: &KeyRange) -> Result<Iter> {
        self.db
            .range_at_sequence_async(
                self.name.as_str(),
                range,
                self.db.last_committed_sequence(),
                Direction::Forward,
            )
            .await
    }

    /// Returns a forward iterator whose blob values are read on demand.
    pub async fn range_lazy(&self, range: &KeyRange) -> Result<LazyIter> {
        self.db
            .range_lazy_at_sequence_async(
                self.name.as_str(),
                range,
                self.db.last_committed_sequence(),
                Direction::Forward,
            )
            .await
    }

    /// Returns a forward iterator over `range` at `snapshot`.
    pub async fn range_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.db
            .range_at_sequence_async(
                self.name.as_str(),
                range,
                snapshot.read_sequence(),
                Direction::Forward,
            )
            .await
    }

    /// Returns a forward value-lazy iterator at `snapshot`.
    pub async fn range_lazy_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.db
            .range_lazy_at_sequence_async(
                self.name.as_str(),
                range,
                snapshot.read_sequence(),
                Direction::Forward,
            )
            .await
    }

    /// Returns a reverse iterator over visible rows in `range`.
    pub async fn range_reverse(&self, range: &KeyRange) -> Result<Iter> {
        self.db
            .range_at_sequence_async(
                self.name.as_str(),
                range,
                self.db.last_committed_sequence(),
                Direction::Reverse,
            )
            .await
    }

    /// Returns a reverse iterator whose blob values are read on demand.
    pub async fn range_lazy_reverse(&self, range: &KeyRange) -> Result<LazyIter> {
        self.db
            .range_lazy_at_sequence_async(
                self.name.as_str(),
                range,
                self.db.last_committed_sequence(),
                Direction::Reverse,
            )
            .await
    }

    /// Returns a reverse iterator over `range` at `snapshot`.
    pub async fn range_reverse_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.db
            .range_at_sequence_async(
                self.name.as_str(),
                range,
                snapshot.read_sequence(),
                Direction::Reverse,
            )
            .await
    }

    /// Returns a reverse value-lazy iterator at `snapshot`.
    pub async fn range_lazy_reverse_at(
        &self,
        snapshot: &Snapshot,
        range: &KeyRange,
    ) -> Result<LazyIter> {
        self.db
            .range_lazy_at_sequence_async(
                self.name.as_str(),
                range,
                snapshot.read_sequence(),
                Direction::Reverse,
            )
            .await
    }

    /// Returns a forward iterator over rows whose keys begin with `prefix`.
    pub async fn prefix(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.db
            .prefix_at_sequence_async(
                self.name.as_str(),
                &prefix,
                self.db.last_committed_sequence(),
                Direction::Forward,
            )
            .await
    }

    /// Returns a forward prefix iterator whose blob values are read on demand.
    pub async fn prefix_lazy(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.db
            .prefix_lazy_at_sequence_async(
                self.name.as_str(),
                &prefix,
                self.db.last_committed_sequence(),
                Direction::Forward,
            )
            .await
    }

    /// Returns a forward prefix iterator at `snapshot`.
    pub async fn prefix_at(&self, snapshot: &Snapshot, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.db
            .prefix_at_sequence_async(
                self.name.as_str(),
                &prefix,
                snapshot.read_sequence(),
                Direction::Forward,
            )
            .await
    }

    /// Returns a forward value-lazy prefix iterator at `snapshot`.
    pub async fn prefix_lazy_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.db
            .prefix_lazy_at_sequence_async(
                self.name.as_str(),
                &prefix,
                snapshot.read_sequence(),
                Direction::Forward,
            )
            .await
    }

    /// Returns a reverse iterator over rows whose keys begin with `prefix`.
    pub async fn prefix_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.db
            .prefix_at_sequence_async(
                self.name.as_str(),
                &prefix,
                self.db.last_committed_sequence(),
                Direction::Reverse,
            )
            .await
    }

    /// Returns a reverse prefix iterator whose blob values are read on demand.
    pub async fn prefix_lazy_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.db
            .prefix_lazy_at_sequence_async(
                self.name.as_str(),
                &prefix,
                self.db.last_committed_sequence(),
                Direction::Reverse,
            )
            .await
    }

    /// Returns a reverse prefix iterator at `snapshot`.
    pub async fn prefix_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<Iter> {
        let prefix = prefix.into();
        self.db
            .prefix_at_sequence_async(
                self.name.as_str(),
                &prefix,
                snapshot.read_sequence(),
                Direction::Reverse,
            )
            .await
    }

    /// Returns a reverse value-lazy prefix iterator at `snapshot`.
    pub async fn prefix_lazy_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.db
            .prefix_lazy_at_sequence_async(
                self.name.as_str(),
                &prefix,
                snapshot.read_sequence(),
                Direction::Reverse,
            )
            .await
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
    pub fn get_sync(&self, key: &[u8]) -> Result<Option<PointValue>> {
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
    /// Use this when the caller needs the same owned-value shape as
    /// `Db::get_sync` or `Bucket::get_sync`.
    pub fn get_owned_sync(&self, key: &[u8]) -> Result<Option<Value>> {
        self.get_sync(key)?
            .map(|value| Ok(value.into_value()))
            .transpose()
    }

    /// Reads many keys using the sources pinned when this reader was created.
    ///
    /// The returned vector has exactly one entry for each input key, in input
    /// order. `Ok(None)` at a position means that key has no value visible at
    /// this reader's snapshot sequence. Duplicate input keys produce duplicate
    /// result entries.
    ///
    /// This method returns [`PointValue`] so inline table values can be
    /// inspected without first copying them into owned `Vec<u8>` values. Use
    /// [`BucketReader::get_many_owned_sync`] when the caller needs owned value
    /// bytes. The reader's snapshot sequence is fixed when
    /// [`Bucket::reader`] creates it, so later commits do not affect this
    /// method's results.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes inside this reader's bucket. Empty input
    ///   returns an empty vector.
    ///
    /// # Errors
    ///
    /// Returns storage or format errors encountered while reading tables or
    /// blob files. Any such error fails the whole batch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions};
    ///
    /// # fn main() -> trine_kv::Result<()> {
    /// let db = Db::open_sync(DbOptions::memory())?;
    /// let bucket = db.default_bucket_sync()?;
    /// bucket.put_sync(b"a", b"one")?;
    /// let snapshot = db.snapshot();
    /// let reader = bucket.reader(&snapshot)?;
    /// bucket.put_sync(b"a", b"new")?;
    ///
    /// let keys = [b"a".as_slice()];
    /// let values = reader.get_many_sync(&keys)?;
    /// assert_eq!(values[0].as_ref().map(|value| value.as_bytes()), Some(b"one".as_slice()));
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_many_sync<K>(&self, keys: &[K]) -> Result<Vec<Option<PointValue>>>
    where
        K: AsRef<[u8]>,
    {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get_sync(key.as_ref())?);
        }
        Ok(values)
    }

    /// Reads many keys and returns owned values.
    ///
    /// This is the owned-value form of [`BucketReader::get_many_sync`]. Values
    /// already owned by the read path are moved directly; values backed by
    /// shared table-block bytes are copied into `Vec<u8>`. The returned vector
    /// preserves input order and uses `None` for keys that are not visible at
    /// the reader's snapshot sequence.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes inside this reader's bucket. Empty input
    ///   returns an empty vector.
    ///
    /// # Errors
    ///
    /// Returns storage or format errors encountered while reading tables or
    /// blob files. Any such error fails the whole batch.
    pub fn get_many_owned_sync<K>(&self, keys: &[K]) -> Result<Vec<Option<Value>>>
    where
        K: AsRef<[u8]>,
    {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get_sync(key.as_ref())?.map(PointValue::into_value));
        }
        Ok(values)
    }

    /// Reads `key` using the sources pinned when this reader was created.
    ///
    /// This returns a `PointValue` so inline table values can be inspected
    /// without first copying them into an owned `Vec<u8>`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<PointValue>> {
        self.db
            .get_value_at_state_snapshot_with_pin_state_async(
                &self.state,
                &self.read_snapshot,
                key,
                self.read_sequence,
                true,
            )
            .await
    }

    /// Reads `key` and returns an owned value.
    ///
    /// Use this when the caller needs the same owned-value shape as
    /// `Db::get_sync` or `Bucket::get_sync`.
    pub async fn get_owned(&self, key: &[u8]) -> Result<Option<Value>> {
        self.get(key)
            .await?
            .map(|value| Ok(value.into_value()))
            .transpose()
    }

    /// Reads many keys using the sources pinned when this reader was created.
    ///
    /// This is the async form of [`BucketReader::get_many_sync`]. It preserves
    /// input order and returns `None` for keys that are not visible at the
    /// reader's snapshot sequence.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes inside this reader's bucket. Empty input
    ///   returns an empty vector.
    ///
    /// # Errors
    ///
    /// Returns storage or format errors encountered while reading tables or
    /// blob files. Any such error fails the whole batch.
    pub async fn get_many<K>(&self, keys: &[K]) -> Result<Vec<Option<PointValue>>>
    where
        K: AsRef<[u8]>,
    {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(key.as_ref()).await?);
        }
        Ok(values)
    }

    /// Reads many keys and returns owned values.
    ///
    /// This is the async owned-value form of
    /// [`BucketReader::get_many_owned_sync`]. It preserves input order and
    /// returns `None` for keys that are not visible at the reader's snapshot
    /// sequence.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes inside this reader's bucket. Empty input
    ///   returns an empty vector.
    ///
    /// # Errors
    ///
    /// Returns storage or format errors encountered while reading tables or
    /// blob files. Any such error fails the whole batch.
    pub async fn get_many_owned<K>(&self, keys: &[K]) -> Result<Vec<Option<Value>>>
    where
        K: AsRef<[u8]>,
    {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(key.as_ref()).await?.map(PointValue::into_value));
        }
        Ok(values)
    }
}
