use super::{
    DatabaseStorageRef, Db, DurabilityMode, Error, KeyRange, MaintenanceBudget, MaintenanceOutcome,
    Result,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use super::{HostStorageBackend, StorageMode};

#[allow(clippy::unused_async)]
impl Db {
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if self.inner.options.storage_mode.is_object_store_persistent() {
            if matches!(
                mode,
                DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
            ) {
                return Err(Error::unsupported_durability(mode));
            }
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if let DatabaseStorageRef::ObjectStore(resources) = self.inner.storage.resources() {
            if self.inner.options.read_only {
                return Err(Error::ReadOnly);
            }
            let outcome = self
                .run_compaction_once_object_store_async(
                    resources.objects,
                    resources.prefix,
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if let DatabaseStorageRef::ObjectStore(resources) = self.inner.storage.resources() {
            if self.inner.options.read_only {
                return Err(Error::ReadOnly);
            }
            return self
                .run_compaction_once_object_store_async(
                    resources.objects,
                    resources.prefix,
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if let DatabaseStorageRef::ObjectStore(resources) = self.inner.storage.resources() {
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
                        resources.objects,
                        resources.prefix,
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
