use super::{
    BTreeMap, Db, DbOptions, DbStats, Error, FilterStats, LevelFilterStats, LevelStats, Ordering,
    Path, ReadVersion, Result, Sequence, Snapshot, Transaction, TransactionOptions,
    add_obsolete_blob_stats, lock_poisoned, shutdown_background_workers, table, table_file_bytes,
    validate_checkpoint_name,
};

impl Db {
    /// Creates a snapshot at the latest visible read version.
    ///
    /// Reads through the returned [`Snapshot`] see the database as of the
    /// read version captured here, even if later writes commit before those
    /// reads run. The snapshot keeps old versions needed by its reads alive
    /// until all clones are dropped.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.inner
            .snapshots
            .pinned_snapshot(self.last_committed_sequence())
    }

    /// Creates a snapshot at a retained read version.
    ///
    /// The returned snapshot pins `version`, so cleanup and compaction must keep
    /// the data needed by its reads alive until all clones are dropped. The
    /// read version is database-wide: reads through the snapshot see one
    /// consistent state across the default bucket and all named buckets.
    ///
    /// # Parameters
    ///
    /// - `version`: read version previously obtained from this database
    ///   lineage, such as [`Db::latest_read_version`] or
    ///   [`crate::CommitInfo::read_version`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the database handle is closed,
    /// [`Error::ReadVersionTooNew`] if `version` is newer than the latest
    /// visible state, or [`Error::ReadVersionExpired`] if `version` is below
    /// [`Db::oldest_retained_read_version`]. The method never falls back to the
    /// latest state.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions};
    ///
    /// # fn main() -> trine_kv::Result<()> {
    /// let db = Db::open_sync(DbOptions::memory())?;
    /// db.put_sync(b"k", b"v1")?;
    /// let old = db.latest_read_version();
    ///
    /// let keep_old = db.snapshot_at(old)?;
    /// db.put_sync(b"k", b"v2")?;
    ///
    /// let snapshot = db.snapshot_at(old)?;
    /// assert_eq!(db.get_at_sync(&snapshot, b"k")?, Some(b"v1".to_vec()));
    /// drop(keep_old);
    /// # Ok(())
    /// # }
    /// ```
    pub fn snapshot_at(&self, version: ReadVersion) -> Result<Snapshot> {
        self.ensure_open()?;
        let latest = self.last_committed_sequence();
        let retained_floor = self.retained_floor_without_active_snapshots(latest);
        self.inner
            .snapshots
            .pinned_retained_snapshot(version.to_sequence(), latest, retained_floor)
    }

    /// Creates a named checkpoint at the newest visible read version.
    ///
    /// A checkpoint is durable metadata for persistent databases and an
    /// in-memory pin for in-memory databases. While it exists, Trine treats the
    /// returned read version as retained: [`Db::snapshot_at`] can later validate
    /// and pin that version even if it is older than the configured recent
    /// retention window.
    ///
    /// # Parameters
    ///
    /// - `name`: non-empty checkpoint name. Names are database-local and must
    ///   be unique until deleted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the handle is closed, [`Error::ReadOnly`] if
    /// the handle cannot mutate metadata, [`Error::InvalidOptions`] for an
    /// empty name, or [`Error::CheckpointAlreadyExists`] when `name` already
    /// exists.
    pub fn create_checkpoint_sync(&self, name: &str) -> Result<ReadVersion> {
        self.ensure_open()?;
        validate_checkpoint_name(name)?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let sequence = self.last_committed_sequence();
        self.record_checkpoint(name, sequence)?;
        Ok(ReadVersion::from_sequence(sequence))
    }

    /// Creates a named checkpoint pinning a specific past `version` rather than
    /// the latest committed one. The version must still be retained; it is pinned
    /// for the duration of recording the checkpoint so it cannot be reclaimed
    /// between the check and the write. Use this to anchor an arbitrary
    /// point-in-time (a database branch pins its fork this way).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the handle is closed, [`Error::ReadOnly`] if
    /// the handle cannot mutate metadata, [`Error::InvalidOptions`] for an empty
    /// name, [`Error::CheckpointAlreadyExists`] when `name` already exists, or a
    /// read-version error if `version` is newer than the latest committed version
    /// or older than the retained-history floor.
    pub fn create_checkpoint_at_sync(&self, name: &str, version: ReadVersion) -> Result<()> {
        self.ensure_open()?;
        validate_checkpoint_name(name)?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        // Validate `version` is readable and pin it so it cannot be reclaimed
        // between the check and recording the checkpoint.
        let _pin = self.snapshot_at(version)?;
        self.record_checkpoint(name, version.to_sequence())
    }

    /// Async-first form of [`Db::create_checkpoint_at_sync`]. Object-store
    /// backends must publish manifest metadata through this path.
    ///
    /// # Errors
    ///
    /// Same validation and persistence errors as
    /// [`Db::create_checkpoint_at_sync`].
    pub async fn create_checkpoint_at(&self, name: &str, version: ReadVersion) -> Result<()> {
        self.ensure_open()?;
        validate_checkpoint_name(name)?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let _pin = self.snapshot_at(version)?;

        if self.inner.options.storage_mode.is_object_store_persistent() {
            self.publish_object_manifest_create_checkpoint(name.to_owned(), version.to_sequence())
                .await?;
            return Ok(());
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            return self.create_checkpoint_at_sync(name, version);
        }

        self.create_checkpoint_at_sync(name, version)
    }

    /// Records a checkpoint pinning `sequence`, in the manifest (persistent) or
    /// the in-memory registry. Shared by [`Self::create_checkpoint_sync`] and
    /// [`Self::create_checkpoint_at_sync`].
    pub(in crate::db) fn record_checkpoint(&self, name: &str, sequence: Sequence) -> Result<()> {
        if let Some(manifest) = &self.inner.manifest {
            manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?
                .create_checkpoint(name.to_owned(), sequence)?;
        } else {
            let mut checkpoints = self
                .inner
                .checkpoints
                .lock()
                .map_err(|_| lock_poisoned("checkpoint registry"))?;
            if checkpoints.insert(name.to_owned(), sequence).is_some() {
                return Err(Error::CheckpointAlreadyExists {
                    name: name.to_owned(),
                });
            }
        }
        Ok(())
    }

    /// Deletes a named checkpoint.
    ///
    /// Deleting a checkpoint removes its retained-version pin. Existing
    /// snapshots opened from that checkpoint continue to pin their read version
    /// until dropped, but future [`Db::snapshot_at`] calls must rely on another
    /// checkpoint, an active snapshot, or the configured recent retention
    /// window.
    ///
    /// # Parameters
    ///
    /// - `name`: checkpoint name previously passed to
    ///   [`Db::create_checkpoint_sync`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`], [`Error::ReadOnly`],
    /// [`Error::InvalidOptions`] for an empty name, or
    /// [`Error::CheckpointNotFound`] if no checkpoint with that name exists.
    pub fn delete_checkpoint_sync(&self, name: &str) -> Result<()> {
        self.ensure_open()?;
        validate_checkpoint_name(name)?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        if let Some(manifest) = &self.inner.manifest {
            manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?
                .delete_checkpoint(name.to_owned())?;
        } else {
            let mut checkpoints = self
                .inner
                .checkpoints
                .lock()
                .map_err(|_| lock_poisoned("checkpoint registry"))?;
            if checkpoints.remove(name).is_none() {
                return Err(Error::CheckpointNotFound {
                    name: name.to_owned(),
                });
            }
        }

        Ok(())
    }

    /// Returns the read version pinned by a named checkpoint.
    ///
    /// This does not create a snapshot. Call [`Db::snapshot_at`] with the
    /// returned value before reading so Trine can validate and pin the retained
    /// version for the lifetime of the read.
    ///
    /// # Parameters
    ///
    /// - `name`: checkpoint name to look up.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`], [`Error::InvalidOptions`] for an empty name,
    /// or [`Error::CheckpointNotFound`] when the checkpoint does not exist.
    pub fn checkpoint_read_version_sync(&self, name: &str) -> Result<ReadVersion> {
        self.ensure_open()?;
        validate_checkpoint_name(name)?;
        let sequence = if let Some(manifest) = &self.inner.manifest {
            manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?
                .checkpoint_sequence(name)
        } else {
            self.inner
                .checkpoints
                .lock()
                .map_err(|_| lock_poisoned("checkpoint registry"))?
                .get(name)
                .copied()
        }
        .ok_or_else(|| Error::CheckpointNotFound {
            name: name.to_owned(),
        })?;

        Ok(ReadVersion::from_sequence(sequence))
    }

    /// Creates an optimistic transaction over the newest visible sequence.
    ///
    /// The transaction records point and range reads against this sequence and
    /// stages writes in memory. Commit succeeds only if no later committed write
    /// conflicts with the recorded read set.
    ///
    /// # Parameters
    ///
    /// - `options`: commit options used when the transaction writes are
    ///   accepted.
    #[must_use]
    pub fn transaction(&self, options: TransactionOptions) -> Transaction {
        Transaction::new(self.clone(), self.last_committed_sequence(), options)
    }

    /// Returns a point-in-time copy of live database statistics.
    #[must_use]
    pub fn stats(&self) -> DbStats {
        let mut stats = self.base_stats();
        self.add_wal_stats(&mut stats);
        self.add_storage_runtime_stats(&mut stats);
        let (blob_read_count, blob_read_bytes) = self.inner.blob_reads.snapshot();
        stats.blob_read_count = blob_read_count;
        stats.blob_read_bytes = blob_read_bytes;
        self.add_scan_and_snapshot_health_stats(&mut stats);
        self.add_compaction_level_stats(&mut stats);
        self.add_compaction_trigger_stats(&mut stats);
        self.add_compaction_skip_stats(&mut stats);
        let cache_stats = self.inner.block_cache.stats();
        stats.block_cache_hits = cache_stats.hits;
        stats.block_cache_misses = cache_stats.misses;

        let persistent_path = self.persistent_path();
        let mut level_stats = BTreeMap::<u32, LevelStats>::new();
        let mut level_filter_stats = BTreeMap::<u32, LevelFilterStats>::new();
        let mut live_blob_bytes_by_file = BTreeMap::<u64, u64>::new();

        let Ok(buckets) = self.inner.buckets.read() else {
            return stats;
        };
        stats.live_buckets = buckets.len();

        for state in buckets.values() {
            if let Ok(memtable_bytes) = state.memtable_bytes() {
                stats.memtable_bytes = stats.memtable_bytes.saturating_add(memtable_bytes);
            }
            stats.immutable_memtables = stats
                .immutable_memtables
                .saturating_add(state.immutable_memtable_count());
            let Ok(version) = state.current_version() else {
                continue;
            };
            stats
                .read_path
                .saturating_add_assign(version.read_path_stats());

            for (level_state, tables) in version.level_table_handles() {
                let level = level_state.get();
                let level_entry = level_stats.entry(level).or_insert(LevelStats {
                    level,
                    tables: 0,
                    bytes: 0,
                });
                for table in tables {
                    let properties = table.properties();
                    let table_bytes = persistent_path.map_or(0, |db_path| {
                        table_file_bytes(&self.inner.native_storage, db_path, properties.id)
                    });
                    let table_filters = table.filter_stats();
                    stats.filters.saturating_add_assign(table_filters);
                    let level_filter_entry =
                        level_filter_stats.entry(level).or_insert(LevelFilterStats {
                            level,
                            tables: 0,
                            filters: FilterStats::default(),
                            filter_resident_bytes: 0,
                        });
                    level_filter_entry.tables += 1;
                    level_filter_entry
                        .filters
                        .saturating_add_assign(table_filters);
                    level_filter_entry.filter_resident_bytes = level_filter_entry
                        .filter_resident_bytes
                        .saturating_add(table.resident_filter_bytes());
                    stats
                        .read_path
                        .saturating_add_assign(table.read_path_stats());
                    stats.total_tables += 1;
                    stats.table_bytes = stats.table_bytes.saturating_add(table_bytes);
                    if properties.level == table::TableLevel::ZERO {
                        stats.l0_tables += 1;
                    }
                    level_entry.tables += 1;
                    level_entry.bytes = level_entry.bytes.saturating_add(table_bytes);

                    for reference in &properties.blob_references {
                        live_blob_bytes_by_file
                            .entry(reference.file_id)
                            .and_modify(|bytes| {
                                *bytes = bytes.saturating_add(reference.referenced_bytes);
                            })
                            .or_insert(reference.referenced_bytes);
                    }
                }
            }
        }

        stats.level_tables = level_stats.into_values().collect();
        stats.level_filters = level_filter_stats.into_values().collect();
        stats.live_blob_files = live_blob_bytes_by_file.len();
        stats.live_blob_bytes = live_blob_bytes_by_file.values().copied().sum();
        if let Some(db_path) = persistent_path {
            add_obsolete_blob_stats(
                &self.inner.native_storage,
                db_path,
                &live_blob_bytes_by_file,
                &mut stats,
            );
        }

        stats
    }

    pub(in crate::db) fn base_stats(&self) -> DbStats {
        DbStats {
            active_snapshots: self.inner.snapshots.active_count(),
            compaction_runs: self.inner.compaction_runs.load(Ordering::Acquire),
            compaction_input_tables: self.inner.compaction_input_tables.load(Ordering::Acquire),
            compaction_output_tables: self.inner.compaction_output_tables.load(Ordering::Acquire),
            compaction_input_bytes: self.inner.compaction_input_bytes.load(Ordering::Acquire),
            compaction_output_bytes: self.inner.compaction_output_bytes.load(Ordering::Acquire),
            commit_sequences_allocated: self.inner.commit_tracker.last_reserved_sequence().get(),
            commit_visible_sequence: self.inner.commit_tracker.visible_sequence().get(),
            commit_open_slots: self.inner.commit_tracker.open_slot_count(),
            commit_skipped_slots: self.inner.commit_tracker.skipped_slot_count(),
            blob_gc_runs: self.inner.blob_gc_runs.load(Ordering::Acquire),
            blob_gc_input_bytes: self.inner.blob_gc_input_bytes.load(Ordering::Acquire),
            blob_gc_output_bytes: self.inner.blob_gc_output_bytes.load(Ordering::Acquire),
            blob_gc_discarded_bytes: self.inner.blob_gc_discarded_bytes.load(Ordering::Acquire),
            maintenance_cooperative_yields: self
                .inner
                .maintenance_cooperative_yields
                .load(Ordering::Acquire),
            maintenance_budget_exhaustions: self
                .inner
                .maintenance_budget_exhaustions
                .load(Ordering::Acquire),
            ..DbStats::default()
        }
    }

    pub(in crate::db) fn add_compaction_level_stats(&self, stats: &mut DbStats) {
        if let Ok(compaction_levels) = self.inner.compaction_level_stats.lock() {
            stats.compaction_levels = compaction_levels.values().cloned().collect();
        }
    }

    pub(in crate::db) fn add_compaction_trigger_stats(&self, stats: &mut DbStats) {
        if let Ok(compaction_triggers) = self.inner.compaction_trigger_stats.lock() {
            stats.compaction_triggers = compaction_triggers.values().cloned().collect();
        }
    }

    pub(in crate::db) fn add_scan_and_snapshot_health_stats(&self, stats: &mut DbStats) {
        let scan_waste = self.inner.scan_waste.snapshot();
        stats.scan_internal_records = scan_waste.internal_records;
        stats.scan_user_keys = scan_waste.user_keys;
        stats.scan_tombstone_hidden_keys = scan_waste.tombstone_hidden_keys;
        let visible = self.inner.commit_tracker.visible_sequence();
        // Pure oldest active snapshot (falls back to the visible sequence when no
        // snapshot is pinned), distinct from the keep-last-versions floor.
        let oldest_snapshot = self.inner.snapshots.oldest_active_or(visible).min(visible);
        stats.oldest_snapshot_seq = oldest_snapshot.get();
        stats.oldest_snapshot_lag = visible.get().saturating_sub(oldest_snapshot.get());
    }

    pub(in crate::db) fn add_compaction_skip_stats(&self, stats: &mut DbStats) {
        if let Ok(compaction_skips) = self.inner.compaction_skip_stats.lock() {
            stats.compaction_skips = compaction_skips.values().cloned().collect();
        }
    }

    pub(in crate::db) fn add_wal_stats(&self, stats: &mut DbStats) {
        if let Some(wal_stats) = self.inner.substrate.wal_stats() {
            stats.wal_shards = wal_stats.shards;
            stats.wal_open_shards = wal_stats.open_shards;
            stats.wal_queue_capacity = wal_stats.queue_capacity;
            stats.wal_records_accepted = wal_stats.records_accepted;
            stats.wal_bytes_accepted = wal_stats.bytes_accepted;
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if let Some(wal) = &self.inner.browser_wal {
            let wal_stats = wal.stats();
            stats.wal_shards = wal_stats.shards;
            stats.wal_open_shards = wal_stats.open_shards;
            stats.wal_queue_capacity = wal_stats.queue_capacity;
            stats.wal_records_accepted = wal_stats.records_accepted;
            stats.wal_bytes_accepted = wal_stats.bytes_accepted;
        }
    }

    pub(in crate::db) fn add_storage_runtime_stats(&self, stats: &mut DbStats) {
        let storage_stats = self.inner.native_storage.stats();
        stats.storage_uses_sync_adapter = storage_stats.uses_blocking_adapter;
        stats.storage_uses_platform_io_driver = storage_stats.uses_platform_io_driver;
        stats.storage_uses_platform_async_io = storage_stats.uses_platform_async_io;
        stats.storage_sync_adapter_tasks = storage_stats.blocking_adapter_tasks;
        stats.storage_sync_adapter_queue_capacity = storage_stats.blocking_adapter_queue_capacity;
        stats.storage_sync_adapter_queued_tasks = storage_stats.blocking_adapter_queued_tasks;
        stats.storage_sync_adapter_submitted_tasks = storage_stats.blocking_adapter_submitted_tasks;
        stats.storage_sync_adapter_completed_tasks = storage_stats.blocking_adapter_completed_tasks;
        stats.storage_sync_adapter_rejected_tasks = storage_stats.blocking_adapter_rejected_tasks;
        stats.storage_sync_adapter_total_runtime_micros =
            storage_stats.blocking_adapter_total_runtime_micros;
        stats.storage_platform_async_io_tasks = storage_stats.platform_async_io_tasks;
        stats.storage_platform_thread_pool_managed_async_tasks =
            storage_stats.platform_thread_pool_managed_async_tasks;
        stats.storage_platform_sync_fallback_tasks = storage_stats.platform_blocking_fallback_tasks;
        stats.storage_platform_io_operations = storage_stats.platform_io_operations;
        stats.storage_inline_tasks = storage_stats.inline_tasks;
        stats.storage_operations = storage_stats.operations;
    }

    /// Returns the options used to open this database handle.
    #[must_use]
    pub fn options(&self) -> &DbOptions {
        &self.inner.options
    }

    #[must_use]
    pub(crate) fn last_committed_sequence(&self) -> Sequence {
        self.inner.commit_tracker.visible_sequence()
    }

    /// Returns the newest read version visible to readers.
    ///
    /// Normal `get`, range, and prefix reads use this latest version unless the
    /// caller explicitly reads through a [`Snapshot`]. An application can store
    /// the returned value as a cursor and later call [`Db::snapshot_at`] to
    /// read the same database state, as long as the version is still retained.
    #[must_use]
    pub fn latest_read_version(&self) -> ReadVersion {
        ReadVersion::from_sequence(self.last_committed_sequence())
    }

    /// Returns the oldest read version Trine currently promises to answer.
    ///
    /// A read version below this floor is expired and
    /// [`Db::snapshot_at`] returns [`Error::ReadVersionExpired`] instead of
    /// reading whatever old bytes may still happen to exist. Active snapshots,
    /// checkpoints, and configured recent-history retention can move this floor
    /// backward.
    #[must_use]
    pub fn oldest_retained_read_version(&self) -> ReadVersion {
        ReadVersion::from_sequence(self.oldest_retained_sequence())
    }

    pub(in crate::db) fn oldest_retained_sequence(&self) -> Sequence {
        let latest = self.last_committed_sequence();
        self.inner
            .snapshots
            .oldest_active_or(latest)
            .min(self.retained_floor_without_active_snapshots(latest))
    }

    pub(in crate::db) fn retained_floor_without_active_snapshots(
        &self,
        latest: Sequence,
    ) -> Sequence {
        self.configured_retention_floor(latest)
            .min(self.oldest_checkpoint_sequence_or(latest))
    }

    pub(in crate::db) fn configured_retention_floor(&self, latest: Sequence) -> Sequence {
        let keep = self.inner.options.keep_last_read_versions.saturating_sub(1);
        Sequence::new(latest.get().saturating_sub(keep))
    }

    pub(in crate::db) fn oldest_checkpoint_sequence_or(&self, fallback: Sequence) -> Sequence {
        if let Some(manifest) = &self.inner.manifest {
            return manifest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .state()
                .checkpoints()
                .values()
                .copied()
                .min()
                .unwrap_or(fallback);
        }
        self.inner
            .checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .copied()
            .min()
            .unwrap_or(fallback)
    }

    /// Closes this handle synchronously and stops background workers.
    pub fn close_sync(&self) {
        self.inner.closed.store(true, Ordering::Release);
        shutdown_background_workers(
            &self.inner.maintenance,
            &self.inner.runtime_shutdown,
            &self.inner.background_workers,
        );
        // The writer lease is released only after all admitted publish
        // activities finish. Otherwise a second process could open while this
        // one is still publishing files or making a commit visible.
        let Ok(()) = self.inner.publish_barrier.close() else {
            return;
        };
        if let Some(db_path) = self.persistent_path().map(Path::to_path_buf) {
            let _ = self.cleanup_pending_obsolete_table_files(&db_path);
            let _ = self.cleanup_pending_obsolete_blob_files(&db_path);
        }
        super::super::release_browser_writer_lease(&self.inner);
        self.inner.substrate.release_writer_lease();
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
}
