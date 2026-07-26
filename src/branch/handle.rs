use super::{
    AsyncBranchRange, AsyncMergeRows, AsyncMergeSource, Backing, BranchRange, BucketName, Db,
    DurableState, Error, HashMap, KeyRange, OverlayWrite, ReadVersion, RegistryEntry, Result,
    Snapshot, TAG_TOMBSTONE, TransactionOptions, Value, data_bucket, decode_branch_value,
    encode_present, range_contains, registry_bucket, require_branch_generation_active,
    validate_user_named_bucket,
};

/// A copy-on-write branch forked from a parent database at a fixed
/// [`ReadVersion`]. Reads see the parent's state as of the fork with the
/// branch's own writes layered on top; the parent is unaffected.
pub struct Branch<'db> {
    db: &'db Db,
    fork: Snapshot,
    backing: Backing,
}

impl<'db> Branch<'db> {
    pub(super) fn ephemeral(db: &'db Db, fork: Snapshot) -> Self {
        Self {
            db,
            fork,
            backing: Backing::Ephemeral(HashMap::new()),
        }
    }

    pub(super) fn durable(db: &'db Db, fork: Snapshot, state: DurableState) -> Self {
        Self {
            db,
            fork,
            backing: Backing::Durable(state),
        }
    }

    /// The parent version this branch forked from. Reads that fall through to the
    /// parent see its state as of exactly this version.
    #[must_use]
    pub const fn fork_version(&self) -> ReadVersion {
        self.fork.read_version()
    }

    /// Whether this branch's writes are persisted (durable named branch) or live
    /// only in memory (ephemeral clone).
    #[must_use]
    pub const fn is_durable(&self) -> bool {
        matches!(self.backing, Backing::Durable(_))
    }

    /// Reads a key on the branch: the branch's own write if it has one, otherwise
    /// the parent's value as of the fork version.
    ///
    /// # Errors
    ///
    /// Returns an error if a bucket cannot be opened or a read fails.
    pub fn get_sync(&self, bucket: impl Into<BucketName>, key: &[u8]) -> Result<Option<Value>> {
        self.db.block_on_sync_api(self.get(bucket, key))
    }

    /// Asynchronously reads a key from this branch.
    ///
    /// Durable branches read their own persisted layers first and then the root
    /// snapshot captured by the fork. Ephemeral branches read their in-memory
    /// overlay first. `Ok(None)` means the nearest visible layer contains a
    /// tombstone or no layer contains the key.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable branch generation is no longer active,
    /// a bucket cannot be opened, or storage cannot complete the read.
    pub async fn get(&self, bucket: impl Into<BucketName>, key: &[u8]) -> Result<Option<Value>> {
        let bucket = bucket.into();
        validate_user_named_bucket(bucket.as_str())?;
        match &self.backing {
            Backing::Ephemeral(overlay) => match overlay.get(&(bucket.clone(), key.to_vec())) {
                Some(OverlayWrite::Put(value)) => return Ok(Some(value.clone())),
                Some(OverlayWrite::Delete) => return Ok(None),
                None => {}
            },
            Backing::Durable(state) => {
                require_branch_generation_active(
                    self.db,
                    &state.chain[0].name,
                    state.leaf_generation,
                )
                .await?;
                for layer in &state.chain {
                    if !layer.written.contains(bucket.as_str()) {
                        continue;
                    }
                    let data = self
                        .db
                        .internal_bucket(data_bucket(&layer.name, bucket.as_str()))
                        .await?;
                    let raw = match &layer.at {
                        None => data.get(key).await?,
                        Some(at) => data.get_at(at, key).await?,
                    };
                    if let Some(raw) = raw {
                        return decode_branch_value(&raw);
                    }
                }
            }
        }
        self.db.bucket(bucket).await?.get_at(&self.fork, key).await
    }

    /// Writes a key on the branch. The write is visible to this branch's reads and
    /// never touches the parent. For a durable branch the write is persisted.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting a durable write fails (ephemeral never
    /// fails).
    pub fn put_sync(
        &mut self,
        bucket: impl Into<BucketName>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) -> Result<()> {
        let db = self.db;
        db.block_on_sync_api(self.put(bucket, key, value))
    }

    /// Deletes a key on the branch (hiding any parent value, via a tombstone for a
    /// durable branch). The parent is unaffected.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting a durable tombstone fails.
    pub fn delete_sync(
        &mut self,
        bucket: impl Into<BucketName>,
        key: impl Into<Vec<u8>>,
    ) -> Result<()> {
        let db = self.db;
        db.block_on_sync_api(self.delete(bucket, key))
    }

    /// Asynchronously writes a value into this branch without changing its
    /// parent.
    ///
    /// Durable branches atomically update the branch data bucket and its
    /// written-bucket registry. Ephemeral branches only update the in-memory
    /// overlay.
    ///
    /// # Errors
    ///
    /// Returns an error when the branch generation was replaced, the database
    /// is read-only or closed, or the selected backend cannot persist the write.
    pub async fn put(
        &mut self,
        bucket: impl Into<BucketName>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) -> Result<()> {
        let bucket = bucket.into();
        validate_user_named_bucket(bucket.as_str())?;
        self.write(bucket, key.into(), OverlayWrite::Put(value.into()))
            .await
    }

    /// Asynchronously writes a branch-local tombstone for `key`.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Branch::put`].
    pub async fn delete(
        &mut self,
        bucket: impl Into<BucketName>,
        key: impl Into<Vec<u8>>,
    ) -> Result<()> {
        let bucket = bucket.into();
        validate_user_named_bucket(bucket.as_str())?;
        self.write(bucket, key.into(), OverlayWrite::Delete).await
    }

    async fn write(&mut self, bucket: BucketName, key: Vec<u8>, write: OverlayWrite) -> Result<()> {
        let db = self.db;
        match &mut self.backing {
            Backing::Ephemeral(overlay) => {
                overlay.insert((bucket, key), write);
                Ok(())
            }
            Backing::Durable(state) => {
                let leaf = &mut state.chain[0];
                let registry_name = registry_bucket();
                let data_name = data_bucket(&leaf.name, bucket.as_str());
                db.internal_bucket(registry_name.as_str()).await?;
                db.internal_bucket(data_name.as_str()).await?;
                let encoded = match write {
                    OverlayWrite::Put(value) => encode_present(&value),
                    OverlayWrite::Delete => vec![TAG_TOMBSTONE],
                };
                let mut transaction = db.transaction(TransactionOptions::default());
                let raw = transaction
                    .get_internal_bucket(&registry_name, leaf.name.as_bytes())
                    .await?
                    .ok_or_else(|| Error::invalid_options("branch no longer exists"))?;
                let mut current = RegistryEntry::decode(&raw)?;
                current.require_identity(
                    state.leaf_generation,
                    state.leaf_fork,
                    state.leaf_parent.as_deref(),
                )?;
                current.written_buckets.insert(bucket.as_str().to_owned());
                transaction.put_internal_bucket(&data_name, key, encoded)?;
                transaction.put_internal_bucket(
                    &registry_name,
                    leaf.name.as_bytes().to_vec(),
                    current.encode()?,
                )?;
                transaction.commit().await?;
                leaf.written = current.written_buckets;
                Ok(())
            }
        }
    }

    /// Scans a key range on the branch, lazily merging its writes over the
    /// parent's state as of the fork (and over each ancestor branch, for a nested
    /// branch): branch puts replace and branch deletes hide the parent's rows.
    /// Returns a [`BranchRange`] iterator yielding the merged rows in key order
    /// without building a full copy — each branch level and the root are streamed
    /// from their own sorted scans and k-way merged on the fly.
    ///
    /// # Errors
    ///
    /// Returns an error if a bucket cannot be opened or a scan cannot be started;
    /// per-row scan errors surface from the iterator.
    pub fn range_sync(
        &self,
        bucket: impl Into<BucketName>,
        range: &KeyRange,
    ) -> Result<BranchRange> {
        self.db
            .block_on_sync_api(self.range(bucket, range))
            .map(BranchRange::new)
    }

    /// Asynchronously starts a forward range scan over the resolved branch
    /// view.
    ///
    /// Storage-backed sources are opened asynchronously before this method
    /// returns. The returned iterator merges branch layers lazily in key order;
    /// per-row decode errors remain iterator items.
    ///
    /// # Errors
    ///
    /// Returns an error when the branch generation is stale, a bucket cannot be
    /// opened, or a storage-backed scan cannot be started.
    pub async fn range(
        &self,
        bucket: impl Into<BucketName>,
        range: &KeyRange,
    ) -> Result<AsyncBranchRange> {
        let bucket = bucket.into();
        validate_user_named_bucket(bucket.as_str())?;
        let mut sources = Vec::new();
        match &self.backing {
            Backing::Ephemeral(overlay) => {
                let mut entries: Vec<(Vec<u8>, Option<Value>)> = overlay
                    .iter()
                    .filter(|((overlay_bucket, key), _)| {
                        overlay_bucket == &bucket && range_contains(range, key)
                    })
                    .map(|((_, key), write)| {
                        let value = match write {
                            OverlayWrite::Put(value) => Some(value.clone()),
                            OverlayWrite::Delete => None,
                        };
                        (key.clone(), value)
                    })
                    .collect();
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                sources.push(
                    AsyncMergeSource::new(AsyncMergeRows::Items(
                        entries.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
                    ))
                    .await,
                );
            }
            Backing::Durable(state) => {
                require_branch_generation_active(
                    self.db,
                    &state.chain[0].name,
                    state.leaf_generation,
                )
                .await?;
                for layer in &state.chain {
                    if !layer.written.contains(bucket.as_str()) {
                        continue;
                    }
                    let data = self
                        .db
                        .internal_bucket(data_bucket(&layer.name, bucket.as_str()))
                        .await?;
                    let rows = match &layer.at {
                        None => data.range(range).await?,
                        Some(at) => data.range_at(at, range).await?,
                    };
                    sources.push(
                        AsyncMergeSource::new(AsyncMergeRows::Database {
                            rows,
                            decode_branch_values: true,
                        })
                        .await,
                    );
                }
            }
        }
        let root = self
            .db
            .bucket(bucket.clone())
            .await?
            .range_at(&self.fork, range)
            .await?;
        sources.push(
            AsyncMergeSource::new(AsyncMergeRows::Database {
                rows: root,
                decode_branch_values: false,
            })
            .await,
        );
        Ok(AsyncBranchRange { sources })
    }
}
