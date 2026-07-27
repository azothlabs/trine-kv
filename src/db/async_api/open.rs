use super::{
    Arc, DatabaseStorageRef, Db, Error, HostStorageBackend, IntoOpenOptions, ManifestStore,
    ObjectLeaseState, ObjectWriterLease, ReadVersion, Result, StorageMode,
    buckets_from_manifest_async, canonical_object_key, commit, ensure_default_bucket_loaded,
    lock_poisoned, manifest, object_store_committed_wal_batches,
    object_store_wal_batches_after_replay_floor, recovery,
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
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] for invalid paths or option
    /// combinations, [`Error::UnsupportedBackend`] or
    /// [`Error::UnsupportedDurability`] when the selected target cannot honor
    /// the requested backend or durability, and [`Error::LeaseUnavailable`]
    /// when another writable handle owns the persistent namespace. Storage and
    /// recovery failures are returned as typed I/O or corruption errors.
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if !self.inner.options.storage_mode.is_object_store_persistent()
            || !self.inner.options.read_only
        {
            return Err(Error::invalid_options(
                "refresh_object_store requires a read-only object-store database",
            ));
        }

        let _refresh = self.inner.object_manifest_async_lock.lock().await;
        let DatabaseStorageRef::ObjectStore(resources) = self.inner.storage.resources() else {
            return Err(Error::invalid_options(
                "refresh_object_store requires an object-store database",
            ));
        };
        let backend = resources.objects.clone();
        let storage_client = backend.client();
        let wal_backend = resources.wal_objects.clone();
        let wal_client = wal_backend.client();
        let db_path = resources.prefix.to_path_buf();
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
}
