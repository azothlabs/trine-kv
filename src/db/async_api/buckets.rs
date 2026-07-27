use super::{
    Bucket, BucketName, BucketOptions, DatabaseStorageRef, Db, Error, ReadVersion, Result,
    require_internal_bucket, require_internal_checkpoint_name, validate_checkpoint_name,
    validate_user_named_bucket,
};

#[allow(clippy::unused_async)]
impl Db {
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
        let name = name.into();
        validate_user_named_bucket(name.as_str())?;
        if matches!(
            self.inner.storage.resources(),
            DatabaseStorageRef::ObjectStore(_)
        ) {
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
        let name = name.into();
        require_internal_bucket(name.as_str())?;
        if matches!(
            self.inner.storage.resources(),
            DatabaseStorageRef::ObjectStore(_)
        ) {
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
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
}
