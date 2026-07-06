use super::{
    Arc, Bucket, BucketName, BucketOptions, CommitInfo, DEFAULT_BUCKET_NAME, Db, Direction, Error,
    HostStorageBackend, Iter, KeyRange, LazyIter, LsmTree, Result, Snapshot, StorageMode, Value,
    WriteOptions, lock_poisoned, validate_bucket_options,
};

impl Db {
    /// Returns a handle for the built-in default bucket.
    ///
    /// Direct helpers such as `Db::put_sync` and `Db::get_sync` use this bucket without
    /// requiring callers to open it explicitly.
    pub fn default_bucket_sync(&self) -> Result<Bucket> {
        let state = self.bucket_state(DEFAULT_BUCKET_NAME)?;
        let options = state.options.clone();
        Ok(Bucket::new(
            self.clone(),
            BucketName::new(DEFAULT_BUCKET_NAME),
            options,
            state,
        ))
    }

    /// Returns an existing named bucket or creates it with default
    /// `BucketOptions`.
    ///
    /// The built-in default bucket is reserved for direct `Db` helpers and
    /// `Db::default_bucket_sync`; using `"default"` as a named bucket returns an
    /// error.
    pub fn bucket_sync(&self, name: impl Into<BucketName>) -> Result<Bucket> {
        self.bucket_with_options_sync(name, BucketOptions::default())
    }

    /// Returns an existing named bucket or creates it with explicit options.
    ///
    /// Bucket options are fixed after creation. Calling this for an existing
    /// named bucket with different options returns an error. The built-in
    /// default bucket is configured through `DbOptions::default_bucket_options`.
    pub fn bucket_with_options_sync(
        &self,
        name: impl Into<BucketName>,
        options: BucketOptions,
    ) -> Result<Bucket> {
        self.ensure_open()?;

        let name = name.into();
        if name.as_str().is_empty() {
            return Err(Error::invalid_options("bucket name cannot be empty"));
        }
        if name.as_str() == DEFAULT_BUCKET_NAME {
            return Err(Error::invalid_options(
                "default bucket is accessed through Db helpers",
            ));
        }

        validate_bucket_options(&options)?;

        if let Some(existing_state) = self.bucket_state_if_exists(name.as_str())? {
            let existing_options = existing_state.options.clone();
            if existing_options != options {
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            return Ok(Bucket::new(
                self.clone(),
                name,
                existing_options,
                existing_state,
            ));
        }

        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent bucket creation requires async API",
            ));
        }

        self.persist_bucket_creation(name.as_str(), &options)?;

        let durable_bucket_creation = self.inner.manifest.is_some();
        let installed_bucket = (|| -> Result<_> {
            let mut buckets = self
                .inner
                .buckets
                .write()
                .map_err(|_| lock_poisoned("bucket registry"))?;

            if let Some(state) = buckets.get(name.as_str()) {
                if state.options != options {
                    return Err(Error::invalid_options(
                        "existing bucket options do not match requested options",
                    ));
                }
                Ok((state.options.clone(), Arc::clone(state)))
            } else {
                let bucket_options = options.clone();
                let state = Arc::new(LsmTree::new(options, Vec::new())?);
                buckets.insert(name.as_str().to_owned(), Arc::clone(&state));
                Ok((bucket_options, state))
            }
        })();
        let (bucket_options, state) = if durable_bucket_creation {
            installed_bucket.map_err(|error| {
                self.close_after_durable_publish_error("bucket creation", &error)
            })?
        } else {
            installed_bucket?
        };

        Ok(Bucket::new(self.clone(), name, bucket_options, state))
    }

    /// Drops a named bucket, removing it from the database and reclaiming its
    /// storage. Existing [`Bucket`] handles or snapshots that still reference the
    /// bucket's tables keep working until dropped (file deletion is deferred while
    /// any reference remains), but no new handle can open it.
    ///
    /// Supported on in-memory and native filesystem databases. Object-store and
    /// other host backends return [`Error::UnsupportedBackend`] (their remote file
    /// reclamation is not driven from here).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the handle is closed, [`Error::ReadOnly`] if
    /// read-only, [`Error::InvalidOptions`] for the default bucket or a bucket
    /// that does not exist, or [`Error::UnsupportedBackend`] on an unsupported
    /// backend.
    pub fn drop_bucket_sync(&self, name: impl Into<BucketName>) -> Result<()> {
        self.ensure_open()?;
        let name = name.into();
        if name.as_str() == DEFAULT_BUCKET_NAME {
            return Err(Error::invalid_options(
                "the default bucket cannot be dropped",
            ));
        }
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if matches!(self.inner.options.storage_mode, StorageMode::InMemory) {
            let removed = self
                .inner
                .buckets
                .write()
                .map_err(|_| lock_poisoned("bucket registry"))?
                .remove(name.as_str());
            return if removed.is_none() {
                Err(Error::invalid_options(
                    "cannot drop a bucket that does not exist",
                ))
            } else {
                Ok(())
            };
        }
        // A native filesystem path: the local-disk `Persistent` backend, and the
        // WASI backend (which is the same native file machinery over a WASI
        // preopened path). Both reclaim files the same way; object-store has its
        // own async path ([`Db::drop_bucket`]) and other host backends are not
        // supported here.
        let native_path = match &self.inner.options.storage_mode {
            StorageMode::Persistent { path }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { path },
            } => path.clone(),
            _ => {
                return Err(Error::unsupported_backend(
                    "dropping a bucket is not supported on this backend",
                ));
            }
        };
        // Flush first so the bucket has no unflushed WAL records that recovery
        // would replay into a now-missing bucket (this also advances the WAL
        // replay floor past them).
        self.flush_sync()?;
        let removed = self
            .inner
            .buckets
            .write()
            .map_err(|_| lock_poisoned("bucket registry"))?
            .remove(name.as_str());
        let Some(tree) = removed else {
            return Err(Error::invalid_options(
                "cannot drop a bucket that does not exist",
            ));
        };
        let tables = tree.tables_snapshot()?;
        let blob_ids: Vec<u64> = tables
            .iter()
            .flat_map(|table| table.blob_file_ids())
            .collect();
        let sequence = self.last_committed_sequence();
        if let Some(manifest) = &self.inner.manifest {
            manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?
                .drop_bucket(name.as_str(), blob_ids, sequence)?;
        }
        // Retire the bucket's table files (deferred while readers hold a
        // reference) and its now-orphaned blob files.
        self.retire_obsolete_table_files(&native_path, tables)?;
        self.cleanup_pending_obsolete_blob_files(&native_path)?;
        Ok(())
    }

    /// Drops a named bucket, reclaiming its storage — the async form that also
    /// supports object-store databases. In-memory and native databases delegate
    /// to [`Db::drop_bucket_sync`]; an object-store database removes the bucket
    /// from the manifest (a CAS publish) so its table and blob objects become
    /// orphans, then reclaims them with snapshot-safe orphan GC.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the handle is closed, [`Error::ReadOnly`] if
    /// read-only, [`Error::InvalidOptions`] for the default bucket or a missing
    /// bucket, or a storage/`ObjectClient` error from the manifest CAS.
    pub async fn drop_bucket(&self, name: impl Into<BucketName>) -> Result<()> {
        self.ensure_open()?;
        let name = name.into();
        if name.as_str() == DEFAULT_BUCKET_NAME {
            return Err(Error::invalid_options(
                "the default bucket cannot be dropped",
            ));
        }
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_object_store_persistent() {
            // Object store: remove the bucket from the manifest via CAS; its table
            // and blob objects are now unreferenced and reclaimed by orphan GC.
            self.publish_object_manifest_drop_bucket(name.as_str().to_owned())
                .await?;
            self.inner
                .buckets
                .write()
                .map_err(|_| lock_poisoned("bucket registry"))?
                .remove(name.as_str());
            // Reclaim the now-orphaned objects (best effort: the bucket is already
            // logically dropped; periodic orphan GC also collects them).
            let _ = self.cleanup_object_store_orphans_async().await;
            return Ok(());
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            // Browser: publish the bucket removal to the IndexedDB-backed manifest
            // (marking its blobs for deletion), then retire its table and blob
            // files through the browser async cleanup.
            let tree = self
                .inner
                .buckets
                .read()
                .map_err(|_| lock_poisoned("bucket registry"))?
                .get(name.as_str())
                .cloned();
            let Some(tree) = tree else {
                return Err(Error::invalid_options(
                    "cannot drop a bucket that does not exist",
                ));
            };
            let tables = tree.tables_snapshot()?;
            let blob_ids: Vec<u64> = tables
                .iter()
                .flat_map(|table| table.blob_file_ids())
                .collect();
            let sequence = self.last_committed_sequence();
            let _publish = self.inner.browser_manifest_async_lock.lock().await;
            let manifest = self
                .inner
                .manifest
                .as_ref()
                .ok_or_else(|| Error::Corruption {
                    message: "browser persistent database is missing manifest store".to_owned(),
                })?;
            let prepared = {
                manifest
                    .lock()
                    .map_err(|_| lock_poisoned("manifest store"))?
                    .prepare_drop_bucket_publish(name.as_str(), blob_ids, sequence)?
            };
            if let Some(prepared) = prepared {
                prepared.publish_async().await?;
                self.install_prepared_manifest_after_durable_publish(
                    "bucket drop",
                    manifest,
                    prepared,
                )?;
            }
            self.inner
                .buckets
                .write()
                .map_err(|_| lock_poisoned("bucket registry"))?
                .remove(name.as_str());
            let db_path = std::path::Path::new("");
            self.retire_obsolete_table_files_browser_async(db_path, tables)
                .await?;
            self.cleanup_pending_obsolete_blob_files_browser_async(db_path)
                .await?;
            return Ok(());
        }
        // In-memory / native / WASI: the synchronous path is correct from here.
        self.drop_bucket_sync(name)
    }

    pub(in crate::db) async fn bucket_with_options_object_store_async(
        &self,
        name: BucketName,
        options: BucketOptions,
    ) -> Result<Bucket> {
        self.ensure_open()?;
        if name.as_str().is_empty() {
            return Err(Error::invalid_options("bucket name cannot be empty"));
        }
        if name.as_str() == DEFAULT_BUCKET_NAME {
            return Err(Error::invalid_options(
                "default bucket is accessed through Db helpers",
            ));
        }
        validate_bucket_options(&options)?;

        if let Some(existing_state) = self.bucket_state_if_exists(name.as_str())? {
            let existing_options = existing_state.options.clone();
            if existing_options != options {
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            return Ok(Bucket::new(
                self.clone(),
                name,
                existing_options,
                existing_state,
            ));
        }
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        self.publish_object_manifest_create_bucket(name.as_str().to_owned(), options.clone())
            .await?;

        let (bucket_options, state) = (|| -> Result<_> {
            let mut buckets = self
                .inner
                .buckets
                .write()
                .map_err(|_| lock_poisoned("bucket registry"))?;
            if let Some(state) = buckets.get(name.as_str()) {
                if state.options != options {
                    return Err(Error::invalid_options(
                        "existing bucket options do not match requested options",
                    ));
                }
                Ok((state.options.clone(), Arc::clone(state)))
            } else {
                let bucket_options = options.clone();
                let state = Arc::new(LsmTree::new(options, Vec::new())?);
                buckets.insert(name.as_str().to_owned(), Arc::clone(&state));
                Ok((bucket_options, state))
            }
        })()
        .map_err(|error| self.close_after_durable_publish_error("bucket creation", &error))?;
        Ok(Bucket::new(self.clone(), name, bucket_options, state))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn bucket_with_options_browser_async(
        &self,
        name: BucketName,
        options: BucketOptions,
    ) -> Result<Bucket> {
        self.ensure_open()?;

        if name.as_str().is_empty() {
            return Err(Error::invalid_options("bucket name cannot be empty"));
        }
        if name.as_str() == DEFAULT_BUCKET_NAME {
            return Err(Error::invalid_options(
                "default bucket is accessed through Db helpers",
            ));
        }

        validate_bucket_options(&options)?;

        if let Some(existing_state) = self.bucket_state_if_exists(name.as_str())? {
            let existing_options = existing_state.options.clone();
            if existing_options != options {
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            return Ok(Bucket::new(
                self.clone(),
                name,
                existing_options,
                existing_state,
            ));
        }

        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        let _manifest_publish = self.inner.browser_manifest_async_lock.lock().await;
        if let Some(existing_state) = self.bucket_state_if_exists(name.as_str())? {
            let existing_options = existing_state.options.clone();
            if existing_options != options {
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            return Ok(Bucket::new(
                self.clone(),
                name,
                existing_options,
                existing_state,
            ));
        }

        let manifest = self
            .inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "browser persistent database is missing manifest store".to_owned(),
            })?;
        let prepared_publish = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            manifest.prepare_create_bucket_publish(name.as_str().to_owned(), options.clone())?
        };
        if let Some(prepared_publish) = prepared_publish {
            prepared_publish.publish_async().await?;
            self.install_prepared_manifest_after_durable_publish(
                "bucket creation",
                manifest,
                prepared_publish,
            )?;
        }

        let (bucket_options, state) = (|| -> Result<_> {
            let mut buckets = self
                .inner
                .buckets
                .write()
                .map_err(|_| lock_poisoned("bucket registry"))?;

            if let Some(state) = buckets.get(name.as_str()) {
                if state.options != options {
                    return Err(Error::invalid_options(
                        "existing bucket options do not match requested options",
                    ));
                }
                Ok((state.options.clone(), Arc::clone(state)))
            } else {
                let bucket_options = options.clone();
                let state = Arc::new(LsmTree::new(options, Vec::new())?);
                buckets.insert(name.as_str().to_owned(), Arc::clone(&state));
                Ok((bucket_options, state))
            }
        })()
        .map_err(|error| self.close_after_durable_publish_error("bucket creation", &error))?;

        Ok(Bucket::new(self.clone(), name, bucket_options, state))
    }

    /// Reads the newest committed value for `key` from the default bucket.
    ///
    /// The `key` parameter is compared as raw bytes. The returned value is an
    /// owned `Vec<u8>` so callers can keep it after the database handle or
    /// iterator state changes. `Ok(None)` means no visible value exists at the
    /// latest read version, either because the key was never written or because
    /// the newest visible record is a delete.
    ///
    /// This method searches the active memtable, immutable memtables, and table
    /// files in newest-to-oldest MVCC order. Large values stored in blob files
    /// are read before the method returns.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes in the built-in default bucket.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the handle is closed, plus storage or
    /// format errors encountered while reading tables or blob files.
    pub fn get_sync(&self, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_sequence(DEFAULT_BUCKET_NAME, key, self.last_committed_sequence())
    }

    /// Reads many newest committed values from the default bucket.
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
    /// view and avoids rebuilding the default bucket read state for each key.
    /// Large blob-backed values are read before the method returns.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes in the built-in default bucket. The slice may
    ///   be empty; an empty input returns an empty vector.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the handle is closed, plus storage or
    /// format errors encountered while reading tables or blob files. Any such
    /// error fails the whole batch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions};
    ///
    /// # fn main() -> trine_kv::Result<()> {
    /// let db = Db::open_sync(DbOptions::memory())?;
    /// db.put_sync(b"a", b"one")?;
    /// db.put_sync(b"b", b"two")?;
    ///
    /// let keys = [b"a".as_slice(), b"missing".as_slice(), b"b".as_slice()];
    /// let values = db.get_many_sync(&keys)?;
    /// assert_eq!(values, vec![Some(b"one".to_vec()), None, Some(b"two".to_vec())]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_many_sync<K>(&self, keys: &[K]) -> Result<Vec<Option<Value>>>
    where
        K: AsRef<[u8]>,
    {
        self.default_bucket_sync()?.get_many_sync(keys)
    }

    /// Reads `key` from the default bucket at the read version pinned by
    /// `snapshot`.
    ///
    /// This is the repeatable-read form of [`Db::get_sync`]. Later commits do
    /// not affect the result because visibility is capped at the snapshot's
    /// read boundary.
    ///
    /// # Parameters
    ///
    /// - `snapshot`: snapshot whose read version defines read visibility.
    /// - `key`: user key bytes in the built-in default bucket.
    pub fn get_at_sync(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_with_pin_state(
            DEFAULT_BUCKET_NAME,
            key,
            snapshot.read_sequence(),
            snapshot.is_pinned(),
        )
    }

    /// Writes one key/value pair to the default bucket using default write options.
    ///
    /// The write is assigned the next commit sequence, appended to the WAL for
    /// persistent databases, inserted into the active memtable, and then made
    /// visible to future reads. The method returns after the configured default
    /// durability has been requested.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes. Empty keys are allowed unless the bucket
    ///   options reject them.
    /// - `value`: value bytes to store. Values at or above the bucket's blob
    ///   threshold may be written to blob files.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadOnly`] for read-only handles, [`Error::Closed`] for
    /// closed handles, [`Error::InvalidOptions`] for invalid key/options, or
    /// storage errors from WAL/blob writes.
    pub fn put_sync(&self, key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        self.put_with_options_sync(key, value, WriteOptions::default())
            .map(|_| ())
    }

    /// Writes one key/value pair to the default bucket and returns commit information.
    ///
    /// This is the explicit-options form of [`Db::put_sync`]. Use it when a
    /// specific write needs a different durability level than the database
    /// default.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes.
    /// - `value`: value bytes to store.
    /// - `options`: per-write durability options.
    ///
    /// # Returns
    ///
    /// Returns [`CommitInfo`] containing the sequence assigned to this commit.
    pub fn put_with_options_sync(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.put(key, value);
        self.write_sync(batch, options)
    }

    /// Adds a point delete for one default-bucket key using default write options.
    ///
    /// A point delete creates a new committed record that hides older values for
    /// the same key at later read sequences. Existing snapshots may still see an
    /// older value if their read sequence is before the delete.
    pub fn delete_sync(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.delete_with_options_sync(key, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a point delete for one default-bucket key and returns commit
    /// information.
    pub fn delete_with_options_sync(
        &self,
        key: impl Into<Vec<u8>>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete(key);
        self.write_sync(batch, options)
    }

    /// Adds a range delete to the default bucket using default write options.
    ///
    /// A range delete hides keys in `range` for later read sequences. It is
    /// committed atomically like a point write and participates in transaction
    /// conflict checks. Existing snapshots can continue to see earlier values.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range to hide. Bounds follow `std::ops::Bound`
    ///   semantics through [`KeyRange`].
    pub fn delete_range_sync(&self, range: KeyRange) -> Result<()> {
        self.delete_range_with_options_sync(range, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a range delete to the default bucket and returns commit
    /// information.
    pub fn delete_range_with_options_sync(
        &self,
        range: KeyRange,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete_range(range);
        self.write_sync(batch, options)
    }

    /// Returns a forward iterator over default-bucket rows in `range`.
    ///
    /// The iterator yields owned [`crate::KeyValue`] rows in ascending byte
    /// order. Each row is the newest value visible at the sequence captured
    /// when the iterator is created. Point deletes and covering range deletes
    /// are skipped.
    ///
    /// The returned iterator may read table blocks and blob files as iteration
    /// advances. Use [`Db::range_lazy_sync`] when callers want keys first and
    /// large values only on demand.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range to scan.
    pub fn range_sync(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket iterator whose blob values are read on demand.
    ///
    /// This has the same visibility and ordering rules as [`Db::range_sync`],
    /// but yields [`crate::LazyKeyValue`] rows. Inline values are already
    /// available; blob-backed values are read only when
    /// [`crate::LazyValue::read_sync`] or [`crate::LazyValue::into_value_sync`]
    /// is called.
    pub fn range_lazy_sync(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket iterator over `range` at `snapshot`.
    pub fn range_at_sync(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward value-lazy default-bucket iterator at `snapshot`.
    pub fn range_lazy_at_sync(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a reverse iterator over default-bucket rows in `range`.
    pub fn range_reverse_sync(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket iterator whose blob values are read on
    /// demand.
    pub fn range_lazy_reverse_sync(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket iterator over `range` at `snapshot`.
    pub fn range_reverse_at_sync(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse value-lazy default-bucket iterator at `snapshot`.
    pub fn range_lazy_reverse_at_sync(
        &self,
        snapshot: &Snapshot,
        range: &KeyRange,
    ) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a forward iterator over default-bucket rows whose keys begin with `prefix`.
    ///
    /// Prefix scans use raw byte-prefix matching over user keys. The bucket's
    /// configured [`crate::PrefixExtractor`] may let Trine skip table or block
    /// reads, but it does not change which keys are returned.
    ///
    /// # Parameters
    ///
    /// - `prefix`: byte prefix that returned keys must start with.
    pub fn prefix_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket prefix iterator whose blob values are
    /// read on demand.
    pub fn prefix_lazy_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket prefix iterator at `snapshot`.
    pub fn prefix_at_sync(&self, snapshot: &Snapshot, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward value-lazy default-bucket prefix iterator at
    /// `snapshot`.
    pub fn prefix_lazy_at_sync(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a reverse iterator over default-bucket rows whose keys begin
    /// with `prefix`.
    pub fn prefix_reverse_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket prefix iterator whose blob values are
    /// read on demand.
    pub fn prefix_lazy_reverse_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket prefix iterator at `snapshot`.
    pub fn prefix_reverse_at_sync(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse value-lazy default-bucket prefix iterator at
    /// `snapshot`.
    pub fn prefix_lazy_reverse_at_sync(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
    }
}
