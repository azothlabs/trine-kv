use super::{
    CommitInfo, DEFAULT_BUCKET_NAME, Db, Direction, Iter, KeyRange, LazyIter, Result, Snapshot,
    Value, WriteOptions,
};

#[allow(clippy::unused_async)]
impl Db {
    /// Reads the newest committed value for `key` from the default bucket.
    ///
    /// This is the async form of [`Db::get_sync`]. It returns owned value bytes,
    /// or `Ok(None)` when no value is visible at the latest read version.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes in the built-in default bucket.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_sequence_async(DEFAULT_BUCKET_NAME, key, self.last_committed_sequence())
            .await
    }

    /// Reads many newest committed values from the default bucket.
    ///
    /// This is the async form of [`Db::get_many_sync`]. It preserves input
    /// order, returns `None` for missing or deleted keys, and fails the whole
    /// batch on storage or format errors. The batch captures one committed read
    /// sequence and one set of point-read sources before reading the first key,
    /// so all returned values share one consistent view of the default bucket.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes in the built-in default bucket. Empty input
    ///   returns an empty vector.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Closed`] if the handle is closed, plus storage or
    /// format errors encountered while reading tables or blob files. Any such
    /// error fails the whole batch.
    pub async fn get_many<K>(&self, keys: &[K]) -> Result<Vec<Option<Value>>>
    where
        K: AsRef<[u8]>,
    {
        self.default_bucket().await?.get_many(keys).await
    }

    /// Reads `key` from the default bucket at the sequence pinned by `snapshot`.
    pub async fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_with_pin_state_async(
            DEFAULT_BUCKET_NAME,
            key,
            self.snapshot_sequence(snapshot)?,
            snapshot.is_pinned(),
        )
        .await
    }

    /// Writes one key/value pair to the default bucket using default write options.
    ///
    /// This is the async form of [`Db::put_sync`]. The write is appended to the
    /// WAL for persistent databases, added to the memtable, and made visible to
    /// later reads once the commit sequence is published.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes.
    /// - `value`: value bytes to store.
    pub async fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        self.put_with_options(key, value, WriteOptions::default())
            .await
            .map(|_| ())
    }

    /// Writes one key/value pair to the default bucket and returns commit information.
    ///
    /// This is the async explicit-options form of [`Db::put`]. Use it when one
    /// write needs different durability than the database default.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes.
    /// - `value`: value bytes to store.
    /// - `options`: per-write durability options.
    pub async fn put_with_options(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.put(key, value);
        self.write(batch, options).await
    }

    /// Adds a point delete for one default-bucket key using default write options.
    pub async fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.delete_with_options(key, WriteOptions::default())
            .await
            .map(|_| ())
    }

    /// Adds a point delete for one default-bucket key and returns commit information.
    pub async fn delete_with_options(
        &self,
        key: impl Into<Vec<u8>>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete(key);
        self.write(batch, options).await
    }

    /// Adds a range delete to the default bucket using default write options.
    pub async fn delete_range(&self, range: KeyRange) -> Result<()> {
        self.delete_range_with_options(range, WriteOptions::default())
            .await
            .map(|_| ())
    }

    /// Adds a range delete to the default bucket and returns commit information.
    pub async fn delete_range_with_options(
        &self,
        range: KeyRange,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete_range(range);
        self.write(batch, options).await
    }

    /// Returns a forward iterator over default-bucket rows in `range`.
    ///
    /// This is the async form of [`Db::range_sync`]. The returned iterator is
    /// positioned over the latest read version captured when this method runs.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range to scan.
    pub async fn range(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward default-bucket iterator whose blob values are read on demand.
    pub async fn range_lazy(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward default-bucket iterator over `range` at `snapshot`.
    pub async fn range_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.snapshot_sequence(snapshot)?,
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward value-lazy default-bucket iterator at `snapshot`.
    pub async fn range_lazy_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.snapshot_sequence(snapshot)?,
            Direction::Forward,
        )
        .await
    }

    /// Returns a reverse iterator over default-bucket rows in `range`.
    pub async fn range_reverse(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse default-bucket iterator whose blob values are read on demand.
    pub async fn range_lazy_reverse(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse default-bucket iterator over `range` at `snapshot`.
    pub async fn range_reverse_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.snapshot_sequence(snapshot)?,
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse value-lazy default-bucket iterator at `snapshot`.
    pub async fn range_lazy_reverse_at(
        &self,
        snapshot: &Snapshot,
        range: &KeyRange,
    ) -> Result<LazyIter> {
        self.range_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.snapshot_sequence(snapshot)?,
            Direction::Reverse,
        )
        .await
    }

    /// Returns a forward iterator over default-bucket rows whose keys begin with `prefix`.
    pub async fn prefix(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward default-bucket prefix iterator whose blob values are read on demand.
    pub async fn prefix_lazy(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward default-bucket prefix iterator at `snapshot`.
    pub async fn prefix_at(&self, snapshot: &Snapshot, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.snapshot_sequence(snapshot)?,
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward value-lazy default-bucket prefix iterator at `snapshot`.
    pub async fn prefix_lazy_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.snapshot_sequence(snapshot)?,
            Direction::Forward,
        )
        .await
    }

    /// Returns a reverse iterator over default-bucket rows whose keys begin with `prefix`.
    pub async fn prefix_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse default-bucket prefix iterator whose blob values are read on demand.
    pub async fn prefix_lazy_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse default-bucket prefix iterator at `snapshot`.
    pub async fn prefix_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.snapshot_sequence(snapshot)?,
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse value-lazy default-bucket prefix iterator at `snapshot`.
    pub async fn prefix_lazy_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.snapshot_sequence(snapshot)?,
            Direction::Reverse,
        )
        .await
    }
}
