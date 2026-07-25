use super::open_helpers::{
    buckets_from_manifest_async, ensure_default_bucket_loaded, lock_poisoned,
    object_store_committed_wal_batches, require_internal_checkpoint_name, validate_checkpoint_name,
};
use super::{
    Arc, Bucket, BucketName, BucketOptions, CommitInfo, DEFAULT_BUCKET_NAME, Db, Direction,
    DurabilityMode, Error, HostStorageBackend, IntoOpenOptions, Iter, KeyRange, LazyIter,
    MaintenanceBudget, MaintenanceOutcome, ManifestStore, ObjectLeaseState, ObjectWriterLease,
    ReadVersion, Result, Snapshot, StorageMode, Value, WriteOptions, commit, manifest, recovery,
};
use crate::{
    bucket::{require_internal_bucket, validate_user_named_bucket},
    object_store::canonical_object_key,
    substrate::object_store_wal_batches_after_replay_floor,
};

/// Primary async database API. Synchronous callers can use the explicit
/// `*_sync` adapters above.
#[allow(clippy::unused_async)]
impl Db {
    /// Opens a database asynchronously.
    ///
    /// This has the same input conversion and recovery behavior as
    /// [`Db::open_sync`], but persistent storage work is driven through the
    /// configured runtime. On browser WASM targets this is the required entry
    /// point for browser persistence.
    ///
    /// # Parameters
    ///
    /// - `options`: either [`crate::DbOptions`] or a path-like value converted
    ///   through [`crate::DbOptions::new`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions};
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     db.put(b"k", b"v").await?;
    ///     assert_eq!(db.get(b"k").await?, Some(b"v".to_vec()));
    ///     Ok(())
    /// }
    /// ```
    pub async fn open(options: impl IntoOpenOptions) -> Result<Self> {
        let options = options.into_open_options();
        match &options.storage_mode {
            StorageMode::InMemory => Self::memory_sync(options),
            StorageMode::Persistent { .. } => {
                Self::open_persistent_with_options_async(options).await
            }
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Self::open_browser_persistent_with_options_async(options).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => Self::open_wasi_persistent_with_options_async(options).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => Err(Error::unsupported_backend(
                "object-store databases must be opened with an object-store client",
            )),
        }
    }

    /// Refreshes a read-only object-store handle to the newest shared state.
    ///
    /// Object-store databases support a compute/storage split: one writer owns
    /// the fencing lease while any number of read-only handles can serve reads
    /// from the shared object store. A read-only handle is a point-in-time view
    /// until this method is called. Refresh re-reads the manifest, rebuilds the
    /// bucket state from the tables it names, then replays the current remote
    /// WAL segment when the writer has acknowledged commits newer than the
    /// manifest replay floor. The returned [`ReadVersion`] is the newest version
    /// visible to reads after the refresh completes.
    ///
    /// `refresh_object_store` never takes the writer lease and never publishes
    /// metadata. It requires the object-store client to make a successful
    /// conditional lease/head write visible to later `get`/`head` reads of that
    /// same key; object listing is used only for cleanup paths, not as the
    /// source of committed WAL state.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] if the handle is not a read-only
    /// object-store database, [`Error::Closed`] if the handle is closed, or a
    /// storage/corruption error if the manifest, table objects, or referenced
    /// WAL segment cannot be read.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use trine_kv::{Db, DbOptions, InMemoryObjectStore};
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let client = Arc::new(InMemoryObjectStore::new());
    ///     let writer = Db::open_object_store(client.clone(), DbOptions::object_store()).await?;
    ///     let reader = Db::open_object_store(client, DbOptions::object_store().read_only()).await?;
    ///
    ///     writer.put(b"k", b"v").await?;
    ///     assert_eq!(reader.get(b"k").await?, None);
    ///
    ///     let refreshed = reader.refresh_object_store().await?;
    ///     assert_eq!(reader.get(b"k").await?, Some(b"v".to_vec()));
    ///     assert_eq!(refreshed, reader.latest_read_version());
    ///     Ok(())
    /// }
    /// ```
    pub async fn refresh_object_store(&self) -> Result<ReadVersion> {
        self.ensure_open()?;
        if !self.inner.options.storage_mode.is_object_store_persistent()
            || !self.inner.options.read_only
        {
            return Err(Error::invalid_options(
                "refresh_object_store requires a read-only object-store database",
            ));
        }

        let _refresh = self.inner.object_manifest_async_lock.lock().await;
        let backend = self.object_storage()?;
        let storage_client = backend.client();
        let wal_backend = self.object_wal_storage()?;
        let wal_client = wal_backend.client();
        let db_path = self.object_store_db_path().to_path_buf();
        let manifest_key = canonical_object_key(&manifest::manifest_path(&db_path))?;
        let lease_key = canonical_object_key(&db_path.join(recovery::PROCESS_LOCK_FILE_NAME))?;

        let remote_wal_state = ObjectWriterLease::read_current(Arc::clone(&wal_client), lease_key)
            .await?
            .unwrap_or_else(ObjectLeaseState::empty);
        let manifest =
            ManifestStore::open_object_store_async(storage_client, manifest_key, 0).await?;
        let mut buckets =
            buckets_from_manifest_async(&backend, &db_path, manifest.state(), true).await?;
        ensure_default_bucket_loaded(&mut buckets, &self.inner.options)?;

        let replay_floor = manifest.state().wal_replay_floor();
        let wal_batches = object_store_wal_batches_after_replay_floor(
            Arc::clone(&wal_client),
            &db_path,
            &remote_wal_state,
            replay_floor,
        )
        .await?;
        let batches = object_store_committed_wal_batches(
            wal_batches,
            replay_floor,
            remote_wal_state.committed_sequence,
        )?;
        let last_committed =
            commit::replay_wal_batches_into_buckets(&buckets, batches, replay_floor)?;

        {
            let mut installed = self
                .inner
                .buckets
                .write()
                .map_err(|_| lock_poisoned("bucket registry"))?;
            *installed = buckets;
        }
        self.inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "object-store database is missing manifest store".to_owned(),
            })?
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .install_object_manifest(manifest.clone_object_manifest()?)?;
        self.inner
            .commit_tracker
            .reset_visible_boundary(last_committed)?;

        Ok(ReadVersion::from_sequence(last_committed))
    }

    /// Returns a handle for the built-in default bucket.
    ///
    /// Direct `Db` methods such as [`Db::put`] and [`Db::get`] use this bucket
    /// internally. Use the handle when code wants to pass a bucket-bound API to
    /// another component or create a [`crate::BucketReader`].
    pub async fn default_bucket(&self) -> Result<Bucket> {
        self.default_bucket_sync()
    }

    /// Returns an existing named bucket or creates it with default options.
    ///
    /// Bucket creation is durable for persistent databases: Trine publishes the
    /// bucket metadata before returning the handle. If the bucket already uses
    /// non-default options, this returns [`Error::InvalidOptions`]; call
    /// [`Db::bucket_with_options`] with the options used at creation instead.
    ///
    /// # Parameters
    ///
    /// - `name`: non-empty bucket name of at most 1024 UTF-8 bytes. The default
    ///   name and Trine's internal namespace are reserved; access the default
    ///   through [`Db::default_bucket`].
    pub async fn bucket(&self, name: impl Into<BucketName>) -> Result<Bucket> {
        self.bucket_with_options(name, BucketOptions::default())
            .await
    }

    /// Returns an existing named bucket or creates it with explicit options.
    ///
    /// Bucket options are fixed at creation. Calling this for an existing
    /// bucket with different options returns [`Error::InvalidOptions`] instead
    /// of silently changing the bucket's storage behavior.
    ///
    /// # Parameters
    ///
    /// - `name`: non-empty bucket name of at most 1024 UTF-8 bytes, outside the
    ///   default and internal reserved namespaces.
    /// - `options`: compression, filter, prefix, blob, and block settings used
    ///   if the bucket is created.
    pub async fn bucket_with_options(
        &self,
        name: impl Into<BucketName>,
        options: BucketOptions,
    ) -> Result<Bucket> {
        let name = name.into();
        validate_user_named_bucket(name.as_str())?;
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return self
                .bucket_with_options_object_store_async(name, options)
                .await;
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            return self.bucket_with_options_browser_async(name, options).await;
        }

        self.bucket_with_options_sync(name, options)
    }

    pub(crate) async fn internal_bucket(&self, name: impl Into<BucketName>) -> Result<Bucket> {
        let name = name.into();
        require_internal_bucket(name.as_str())?;
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return self
                .bucket_with_options_object_store_async(name, BucketOptions::default())
                .await;
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            return self
                .bucket_with_options_browser_async(name, BucketOptions::default())
                .await;
        }

        self.internal_bucket_sync(name)
    }

    /// Creates a named checkpoint at the newest visible read version.
    ///
    /// This is the async-first form of [`Db::create_checkpoint_sync`]. For
    /// object-storage and browser-backed databases, the checkpoint metadata is
    /// published through the backend's async manifest path.
    ///
    /// # Parameters
    ///
    /// - `name`: non-empty checkpoint name of at most 1024 UTF-8 bytes, outside
    ///   Trine's internal namespace and unique within this database until
    ///   deleted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`], [`Error::ReadOnly`],
    /// [`Error::InvalidOptions`] for an invalid or reserved name, or
    /// [`Error::CheckpointAlreadyExists`] if `name` already exists.
    pub async fn create_checkpoint(&self, name: &str) -> Result<ReadVersion> {
        self.ensure_open()?;
        validate_checkpoint_name(name)?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        if self.inner.options.storage_mode.is_object_store_persistent() {
            let sequence = self.last_committed_sequence();
            self.publish_object_manifest_create_checkpoint(name.to_owned(), sequence)
                .await?;
            return Ok(ReadVersion::from_sequence(sequence));
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            return self.create_checkpoint_browser_async(name).await;
        }

        self.create_checkpoint_sync(name)
    }

    /// Deletes a named checkpoint.
    ///
    /// This is the async-first form of [`Db::delete_checkpoint_sync`].
    ///
    /// # Parameters
    ///
    /// - `name`: checkpoint name to delete.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`], [`Error::ReadOnly`],
    /// [`Error::InvalidOptions`] for an invalid or reserved name, or
    /// [`Error::CheckpointNotFound`] if `name` does not exist.
    pub async fn delete_checkpoint(&self, name: &str) -> Result<()> {
        self.ensure_open()?;
        validate_checkpoint_name(name)?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        if self.inner.options.storage_mode.is_object_store_persistent() {
            self.publish_object_manifest_delete_checkpoint(name.to_owned())
                .await?;
            return Ok(());
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            return self.delete_checkpoint_browser_async(name).await;
        }

        self.delete_checkpoint_sync(name)
    }

    pub(crate) async fn delete_internal_checkpoint(&self, name: &str) -> Result<()> {
        self.ensure_open()?;
        require_internal_checkpoint_name(name)?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        if self.inner.options.storage_mode.is_object_store_persistent() {
            self.publish_object_manifest_delete_checkpoint(name.to_owned())
                .await?;
            return Ok(());
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            return self.delete_checkpoint_browser_async(name).await;
        }

        self.delete_internal_checkpoint_sync(name)
    }

    /// Returns the read version pinned by a named checkpoint.
    ///
    /// This is the async counterpart of [`Db::checkpoint_read_version_sync`].
    /// The lookup itself does not pin a snapshot; call [`Db::snapshot_at`] with
    /// the returned value before reading.
    ///
    /// # Parameters
    ///
    /// - `name`: checkpoint name to look up.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`], [`Error::InvalidOptions`] for an invalid or
    /// reserved name, or [`Error::CheckpointNotFound`] if `name` does not
    /// exist.
    pub async fn checkpoint_read_version(&self, name: &str) -> Result<ReadVersion> {
        self.checkpoint_read_version_sync(name)
    }

    pub(crate) fn internal_checkpoint_read_version(&self, name: &str) -> Result<ReadVersion> {
        self.internal_checkpoint_read_version_sync(name)
    }

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
    /// Returns [`Error::Closed`] if the handle is closed, plus storage or
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
            snapshot.read_sequence(),
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
            snapshot.read_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward value-lazy default-bucket iterator at `snapshot`.
    pub async fn range_lazy_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
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
            snapshot.read_sequence(),
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
            snapshot.read_sequence(),
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
            snapshot.read_sequence(),
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
            snapshot.read_sequence(),
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
            snapshot.read_sequence(),
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
            snapshot.read_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Persists pending WAL bytes according to `mode`.
    ///
    /// This is the async form of [`Db::persist_sync`]. Native persistent
    /// databases run blocking persistence work through the configured runtime;
    /// browser persistent databases use the browser storage path on supported
    /// targets. Browser persistent persistence is tied to the active write
    /// front door and is not a storage-quota reservation; on browser builds,
    /// use `trine_kv::browser::browser_storage_estimate` and
    /// `trine_kv::browser::request_browser_persistent_storage` before opening
    /// a browser database when eviction risk matters.
    ///
    /// # Parameters
    ///
    /// - `mode`: durability level to request for pending WAL bytes.
    pub async fn persist(&self, mode: DurabilityMode) -> Result<()> {
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return self.inner.substrate.persist_wal_async(mode).await;
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if matches!(
            self.inner.options.storage_mode,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. }
            }
        ) {
            return self.persist_browser_async(mode).await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            return self.persist_native_async(mode).await;
        }

        self.persist_sync(mode)
    }

    /// Flushes committed memtable data to persistent table files.
    ///
    /// This is the async form of [`Db::flush_sync`]. It can be used by async
    /// applications to force committed memtable data into table files without
    /// blocking the caller's executor thread on native persistent storage.
    ///
    /// On browser persistent databases, once this future is first polled the
    /// flush runs in a browser-local task. Dropping the future after that point
    /// does not cancel the flush; the task finishes or reports its result to
    /// the waiting future.
    pub async fn flush(&self) -> Result<()> {
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return self.flush_object_store_async().await;
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            let db = self.clone();
            return Self::run_owned_browser_task(
                "browser persistent flush task was cancelled",
                async move { db.flush_browser_async().await },
            )
            .await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            return self.flush_native_async().await;
        }

        self.flush_sync()
    }

    /// Compacts table files that overlap `range`.
    ///
    /// On browser persistent databases, once this future is first polled the
    /// compaction runs in a browser-local task. Dropping the future after that
    /// point does not cancel the compaction, because partially abandoned table
    /// publish work would make startup recovery more expensive.
    pub async fn compact_range(&self, range: KeyRange) -> Result<()> {
        if self.inner.options.storage_mode.is_object_store_persistent() {
            self.ensure_open()?;
            if self.inner.options.read_only {
                return Err(Error::ReadOnly);
            }
            let outcome = self
                .run_compaction_once_object_store_async(
                    self.object_store_db_path(),
                    &range,
                    false,
                    MaintenanceBudget::unbounded(),
                )
                .await?;
            if outcome.busy {
                return Err(Error::runtime_busy(
                    "object-store compaction is already active",
                ));
            }
            return Ok(());
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            let db = self.clone();
            return Self::run_owned_browser_task(
                "browser persistent compaction task was cancelled",
                async move { db.compact_range_browser_async(range).await },
            )
            .await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            self.take_background_maintenance_error()?;
            self.ensure_open()?;
            if self.inner.options.read_only {
                return Err(Error::ReadOnly);
            }
            let Some(path) = self.persistent_path() else {
                return Ok(());
            };
            let db_path = path.to_path_buf();
            self.run_compaction_barrier_native_async(&db_path, &range, false)
                .await?;
            return Ok(());
        }

        self.compact_range_sync(range)
    }

    /// Compacts table files that overlap `range` within `budget`.
    ///
    /// On browser persistent databases, once this future is first polled the
    /// compaction runs in a browser-local task. Dropping the future after that
    /// point does not cancel the underlying storage work.
    pub async fn compact_range_with_budget(
        &self,
        range: KeyRange,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        if self.inner.options.storage_mode.is_object_store_persistent() {
            self.ensure_open()?;
            if self.inner.options.read_only {
                return Err(Error::ReadOnly);
            }
            return self
                .run_compaction_once_object_store_async(
                    self.object_store_db_path(),
                    &range,
                    false,
                    budget,
                )
                .await;
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            let db = self.clone();
            return Self::run_owned_browser_task(
                "browser persistent compaction task was cancelled",
                async move {
                    db.compact_range_with_budget_browser_async(range, budget)
                        .await
                },
            )
            .await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            self.take_background_maintenance_error()?;
            self.ensure_open()?;
            if self.inner.options.read_only {
                return Err(Error::ReadOnly);
            }
            let Some(path) = self.persistent_path() else {
                return Ok(MaintenanceOutcome::default());
            };
            let db_path = path.to_path_buf();
            return self
                .run_compaction_once_with_budget_host_async(&db_path, &range, false, budget)
                .await;
        }

        self.compact_range_with_budget_sync(range, budget)
    }

    /// Runs cooperative flush and compaction work within `budget`.
    ///
    /// The budget carries separate limits for flush inputs and compaction
    /// inputs. A call may consume work from both limits: it first flushes
    /// immutable memtables, then runs one compaction pass if the flush step did
    /// not report busy or budget exhaustion.
    ///
    /// On browser persistent databases, once this future is first polled the
    /// maintenance pass runs in a browser-local task. Dropping the future after
    /// that point does not cancel the pass.
    pub async fn run_maintenance_with_budget(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        if self.inner.options.storage_mode.is_object_store_persistent() {
            self.ensure_open()?;
            if self.inner.options.read_only {
                return Err(Error::ReadOnly);
            }
            let mut outcome = MaintenanceOutcome::default();
            let mut flush_permits_compaction = true;
            if self.has_immutable_memtables()? {
                let flush = self.flush_object_store_with_budget_async(budget).await?;
                flush_permits_compaction = flush.permits_follow_up_compaction();
                outcome.add_assign(flush);
            }
            if flush_permits_compaction {
                let compaction = self
                    .run_compaction_once_object_store_async(
                        self.object_store_db_path(),
                        &KeyRange::all(),
                        false,
                        budget,
                    )
                    .await?;
                outcome.add_assign(compaction);
            }
            if !outcome.busy && !outcome.budget_exhausted {
                self.cleanup_object_store_orphans_async().await?;
            }
            return Ok(outcome);
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            let db = self.clone();
            return Self::run_owned_browser_task(
                "browser persistent maintenance task was cancelled",
                async move { db.run_maintenance_with_budget_browser_async(budget).await },
            )
            .await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            self.take_background_maintenance_error()?;
            self.ensure_open()?;
            if self.inner.options.read_only {
                return Err(Error::ReadOnly);
            }
            let Some(path) = self.persistent_path() else {
                return Ok(MaintenanceOutcome::default());
            };
            let db_path = path.to_path_buf();
            let mut outcome = MaintenanceOutcome::default();
            let mut should_compact = self.l0_pressure_exceeded()?;

            if self.has_immutable_memtables()? {
                let (flush_should_compact, flush_outcome) = self
                    .run_flush_once_with_budget_host_async(&db_path, false, budget)
                    .await?;
                should_compact |= flush_should_compact;
                outcome.add_assign(flush_outcome);
            }

            if should_compact {
                let compaction_outcome = self
                    .run_compaction_once_with_budget_host_async(
                        &db_path,
                        &KeyRange::all(),
                        true,
                        budget,
                    )
                    .await?;
                outcome.add_assign(compaction_outcome);
            }

            if outcome.made_progress() {
                self.cleanup_pending_obsolete_table_files_native_async(&db_path)
                    .await?;
                self.cleanup_pending_obsolete_blob_files_native_async(&db_path)
                    .await?;
            }
            self.take_background_maintenance_error()?;
            return Ok(outcome);
        }

        self.run_maintenance_with_budget_sync(budget)
    }

    /// Closes this handle asynchronously and stops background workers.
    pub async fn close(&self) -> Result<()> {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            return self.close_native_async().await;
        }

        self.close_sync();
        Ok(())
    }
}
