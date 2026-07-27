use super::{
    Arc, ContentReclamationMode, Db, DbInnerParts, DbOptions, DurabilityMode, DurabilitySubstrate,
    Error, FilesystemSubstrate, ManifestStore, ObjectClient, ObjectClientTrustMode,
    ObjectLeaseState, ObjectStoreBackend, ObjectStoreInnerParts, ObjectStoreSubstrate,
    ObjectWriterLease, PathBuf, Result, Runtime, buckets_from_manifest_async, canonical_object_key,
    canonical_object_prefix, ensure_default_bucket_in_manifest_async, ensure_default_bucket_loaded,
    manifest, object_store_committed_wal_batches, object_store_wal_batches_after_replay_floor,
    recovery, validate_common_options, verify_object_client_contract_for_open,
};

impl Db {
    /// Opens an object-storage database at the bucket root (async-only).
    ///
    /// Equivalent to [`Db::open_object_store_at`] with an empty key prefix. Use
    /// the prefixed form to host several databases in one bucket.
    ///
    /// # Errors
    ///
    /// See [`Db::open_object_store_at`].
    pub async fn open_object_store(
        client: Arc<dyn ObjectClient>,
        options: DbOptions,
    ) -> Result<Self> {
        Self::open_object_store_at(client, "", options).await
    }

    /// Opens an object-storage database whose write-ahead log uses a separate
    /// object client.
    ///
    /// `storage_client` stores table/blob objects and the manifest CAS.
    /// `wal_client` stores the writer lease, remote WAL head, and WAL segment
    /// bytes. A confirmed durable write is acknowledged only after the WAL tier
    /// stores the frame and publishes the WAL head. Flush later copies covered
    /// memtable data into the storage tier's table objects and advances the
    /// storage-tier manifest replay floor.
    ///
    /// Use this when object storage is the shared bulk tier but commit latency
    /// should be bounded by a lower-latency durable WAL tier. The two clients may
    /// point at different buckets, services, or adapters; they share the same
    /// key prefix string so recovery can address the corresponding objects in
    /// each tier.
    ///
    /// # Parameters
    ///
    /// - `storage_client`: object client for manifest, tables, blobs, and bulk
    ///   cleanup.
    /// - `wal_client`: object client for the writer lease and remote WAL
    ///   objects. It must provide real conditional writes and same-key
    ///   read-after-write visibility.
    /// - `options`: must be [`DbOptions::object_store`], optionally marked
    ///   read-only.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] when `options` is not object-store mode,
    /// [`Error::LeaseUnavailable`] when another live writer owns the WAL-tier
    /// lease, [`Error::Fenced`] when this writer has been superseded by a newer
    /// epoch, or storage/`ObjectClient` errors from either tier.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use trine_kv::{Db, DbOptions, InMemoryObjectStore};
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let storage = Arc::new(InMemoryObjectStore::new());
    ///     let wal = Arc::new(InMemoryObjectStore::new());
    ///     let db = Db::open_object_store_with_wal(storage, wal, DbOptions::object_store()).await?;
    ///
    ///     db.put(b"k", b"v").await?;
    ///     assert_eq!(db.get(b"k").await?, Some(b"v".to_vec()));
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_object_store_with_wal(
        storage_client: Arc<dyn ObjectClient>,
        wal_client: Arc<dyn ObjectClient>,
        options: DbOptions,
    ) -> Result<Self> {
        Self::open_object_store_with_wal_at(storage_client, wal_client, "", options).await
    }

    /// Opens an object-storage database under a key `prefix` (async-only).
    ///
    /// Provide any [`ObjectClient`] implementation (S3 and compatible, or the
    /// in-memory fake) and object-store options ([`DbOptions::object_store`]).
    /// All of the database's object keys live under `prefix`
    /// (`<prefix>/MANIFEST`, `<prefix>/LOCK`, `<prefix>/table-*.trinet`, …), so
    /// several databases can share one bucket. An empty `prefix` uses the bucket
    /// root.
    ///
    /// The byte IO, the manifest (conditional-PUT CAS), the writer lease, and
    /// the remote WAL all ride `client`. Durable writes publish WAL frames into
    /// the current remote segment before they become visible; flush later
    /// converts covered memtables into table objects, advances the manifest
    /// replay floor, and cleans old WAL objects.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] when `options` is not object-store mode,
    /// [`Error::LeaseUnavailable`] when another live writer owns the prefix, or
    /// storage/`ObjectClient` errors from reading the manifest and acquiring
    /// the writer lease.
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    #[allow(clippy::too_many_lines)]
    pub async fn open_object_store_at(
        client: Arc<dyn ObjectClient>,
        prefix: impl Into<String>,
        options: DbOptions,
    ) -> Result<Self> {
        Self::open_object_store_with_wal_at(Arc::clone(&client), client, prefix, options).await
    }

    /// Opens an object-storage database under `prefix` with an explicit WAL tier.
    ///
    /// This is the prefixed form of [`Db::open_object_store_with_wal`]. The
    /// prefix is applied independently to both clients: storage keys such as
    /// `<prefix>/MANIFEST` live in `storage_client`, while WAL-tier keys such as
    /// `<prefix>/LOCK` and `<prefix>/wal-...` live in `wal_client`.
    ///
    /// A read-only open reads the storage manifest from `storage_client` and the
    /// remote WAL head/segment from `wal_client`, then serves a point-in-time
    /// view until [`Db::refresh_object_store`] is called.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] when `options` is not object-store mode,
    /// [`Error::UnsupportedBackend`] when a reclamation capability belongs to a
    /// different storage client or prefix, [`Error::LeaseUnavailable`] when
    /// another live writer owns the WAL-tier lease, or any error returned by
    /// the storage or WAL client while opening or recovering.
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    #[allow(clippy::too_many_lines)]
    pub async fn open_object_store_with_wal_at(
        storage_client: Arc<dyn ObjectClient>,
        wal_client: Arc<dyn ObjectClient>,
        prefix: impl Into<String>,
        options: DbOptions,
    ) -> Result<Self> {
        if !options.storage_mode.is_object_store_persistent() {
            return Err(Error::invalid_options(
                "object-store open requires the object-store storage mode",
            ));
        }
        validate_common_options(&options)?;
        if matches!(
            options.durability,
            DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
        ) {
            return Err(Error::unsupported_durability(options.durability));
        }

        let prefix = canonical_object_prefix(&prefix.into())?;
        let db_path = PathBuf::from(prefix);
        crate::substrate::validate_object_lease_wal_key_capacity(&db_path)?;
        if let ContentReclamationMode::QualifiedObjectStore(qualification) =
            &options.content_reclamation
        {
            if !qualification.matches_prefix(&db_path) {
                return Err(Error::unsupported_backend(
                    "object-store reclamation qualification names a different database prefix",
                ));
            }
            if !qualification.matches_client(&storage_client) {
                return Err(Error::unsupported_backend(
                    "object-store reclamation qualification belongs to a different client instance",
                ));
            }
        }
        let backend = ObjectStoreBackend::new(Arc::clone(&storage_client));
        let wal_backend = ObjectStoreBackend::new(Arc::clone(&wal_client));
        // The default trust mode assumes adapter qualification happened outside
        // open (CI/startup/health check). VerifyOnOpen is the explicit
        // fail-closed mode for deployments that want this open call to probe.
        if !options.read_only
            && matches!(
                options.object_client_trust,
                ObjectClientTrustMode::VerifyOnOpen
            )
        {
            verify_object_client_contract_for_open(&storage_client, &db_path, "storage").await?;
            if !Arc::ptr_eq(&storage_client, &wal_client) {
                verify_object_client_contract_for_open(&wal_client, &db_path, "wal").await?;
            }
        }
        let manifest_key = canonical_object_key(&manifest::manifest_path(&db_path))?;

        // Acquire the writer lease before the manifest, so its fencing epoch is
        // known and can be stamped into every publish.
        let lease_key = canonical_object_key(&db_path.join(recovery::PROCESS_LOCK_FILE_NAME))?;
        let (substrate, remote_wal_state) = if options.read_only {
            let remote_wal_state =
                ObjectWriterLease::read_current(Arc::clone(&wal_client), lease_key)
                    .await?
                    .unwrap_or_else(ObjectLeaseState::empty);
            (
                DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None)),
                remote_wal_state,
            )
        } else {
            let lease = ObjectWriterLease::acquire(Arc::clone(&wal_client), lease_key).await?;
            let remote_wal_state = lease.lease_state();
            (
                DurabilitySubstrate::ObjectStore(ObjectStoreSubstrate::new(
                    lease,
                    db_path.clone(),
                )?),
                remote_wal_state,
            )
        };
        let writer_epoch = substrate.object_fencing_epoch().unwrap_or(0);

        let mut manifest = ManifestStore::open_object_store_async(
            Arc::clone(&storage_client),
            manifest_key,
            writer_epoch,
        )
        .await?;
        ensure_default_bucket_in_manifest_async(&mut manifest, &options).await?;
        // Stamp our fencing epoch into the manifest now (writable opens), so a
        // just-displaced prior owner is fenced before we issue our first flush —
        // not only after it. Fails with `Error::Fenced` if a newer owner exists.
        if !options.read_only {
            manifest.claim_object_epoch_async().await?;
        }

        let mut buckets =
            buckets_from_manifest_async(&backend, &db_path, manifest.state(), true).await?;
        ensure_default_bucket_loaded(&mut buckets, &options)?;
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

        let runtime = Runtime::new(options.runtime);
        let db = Self::from_inner_parts(DbInnerParts::object_store(
            options,
            buckets,
            manifest,
            ObjectStoreInnerParts {
                substrate,
                storage: backend,
                wal_storage: wal_backend,
                prefix: db_path,
            },
            runtime,
        ));
        db.replay_wal_batches(batches, replay_floor)?;
        Ok(db)
    }
}
