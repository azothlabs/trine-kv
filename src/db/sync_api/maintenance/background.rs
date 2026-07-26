use super::{
    Arc, AsyncPointReadIo, BACKGROUND_MAINTENANCE_PROGRESS_WAIT, BucketReader, Db, Direction,
    Duration, Iter, KeyRange, LazyIter, LsmPointReadSnapshot, LsmTree, MaintenanceBudget,
    MaintenanceRequest, Ordering, Path, PointValue, Result, ScanSelector, ScanSourceInput,
    Sequence, Snapshot,
};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use super::{background_worker_loop, lock_poisoned};

impl Db {
    pub(in crate::db) fn start_background_workers(&self) -> Result<()> {
        if !self.background_workers_enabled() {
            return Ok(());
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Err(crate::Error::unsupported_backend(
                "browser persistent background workers",
            ))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            for worker_index in 0..self.inner.options.background_worker_count {
                let inner = Arc::downgrade(&self.inner);
                let maintenance = Arc::clone(&self.inner.maintenance);
                let runtime_shutdown = self.inner.runtime_shutdown.clone();
                let worker = self.inner.runtime.spawn_background(
                    format!("trine-kv-maintenance-{worker_index}"),
                    move || background_worker_loop(&inner, &maintenance, &runtime_shutdown),
                )?;
                self.inner
                    .background_workers
                    .lock()
                    .map_err(|_| lock_poisoned("background worker registry"))?
                    .push(worker);
            }
            self.request_background_maintenance();

            Ok(())
        }
    }

    pub(in crate::db) fn background_workers_enabled(&self) -> bool {
        !self.inner.options.read_only
            && self.inner.options.background_worker_count != 0
            && self.inner.runtime.capabilities().background_threads()
            && self.inner.options.storage_mode.persistent_path().is_some()
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(in crate::db) fn request_background_maintenance(&self) {
        if self.background_workers_enabled() {
            self.inner.maintenance.request(MaintenanceRequest {
                flush: true,
                compaction: true,
            });
        }
    }

    pub(in crate::db) fn request_background_flush(&self) {
        if self.background_workers_enabled() {
            self.inner.maintenance.request(MaintenanceRequest {
                flush: true,
                compaction: false,
            });
        }
    }

    pub(in crate::db) fn request_background_compaction(&self) {
        if self.background_workers_enabled() {
            self.inner.maintenance.request(MaintenanceRequest {
                flush: false,
                compaction: true,
            });
        }
    }

    pub(in crate::db) fn take_background_maintenance_error(&self) -> Result<()> {
        if let Some(error) = self.inner.maintenance.take_error() {
            Err(error)
        } else {
            Ok(())
        }
    }

    pub(in crate::db) fn record_cooperative_maintenance_yield(&self) {
        self.inner
            .maintenance_cooperative_yields
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::db) fn record_maintenance_budget_exhaustion(&self) {
        self.inner
            .maintenance_budget_exhaustions
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::db) fn background_maintenance_budget(&self) -> MaintenanceBudget {
        MaintenanceBudget::new(
            self.inner.options.max_immutable_memtables,
            self.inner.options.max_l0_files.saturating_add(1),
        )
    }

    pub(in crate::db) fn background_flush_request_threshold(&self) -> usize {
        self.inner
            .options
            .max_immutable_memtables
            .saturating_sub(1)
            .max(3)
    }

    pub(in crate::db) const fn background_maintenance_progress_wait() -> Duration {
        BACKGROUND_MAINTENANCE_PROGRESS_WAIT
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(in crate::db) fn run_background_maintenance(
        &self,
        request: MaintenanceRequest,
    ) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Ok(());
        }

        let Some(path) = self.persistent_path() else {
            return Ok(());
        };
        let db_path = path.to_path_buf();
        let mut should_compact = request.compaction || self.l0_pressure_exceeded()?;
        let budget = self.background_maintenance_budget();

        if request.flush && self.has_immutable_memtables()? {
            let (flush_should_compact, _) =
                self.run_flush_once_with_budget(&db_path, false, budget)?;
            should_compact |= flush_should_compact;
        }

        if should_compact {
            self.run_compaction_once_with_budget(&db_path, &KeyRange::all(), true, budget)?;
        }
        if self.has_immutable_memtables()? {
            self.request_background_flush();
        }
        if self.l0_pressure_exceeded()? {
            self.request_background_compaction();
        }

        Ok(())
    }

    pub(crate) fn get_at_sequence(
        &self,
        bucket: &str,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<Option<Vec<u8>>> {
        self.get_at_with_pin_state(bucket, key, read_sequence, false)
    }

    pub(crate) async fn get_at_sequence_async(
        &self,
        bucket: &str,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<Option<Vec<u8>>> {
        self.get_at_with_pin_state_async(bucket, key, read_sequence, false)
            .await
    }

    pub(crate) fn get_at_with_pin_state(
        &self,
        bucket: &str,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<Vec<u8>>> {
        let state = self.bucket_state(bucket)?;
        self.get_at_state_with_pin_state(&state, key, read_sequence, read_pin_held)
    }

    pub(crate) async fn get_at_with_pin_state_async(
        &self,
        bucket: &str,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<Vec<u8>>> {
        let state = self.bucket_state(bucket)?;
        self.get_at_state_with_pin_state_async(&state, key, read_sequence, read_pin_held)
            .await
    }

    pub(crate) fn get_at_state_with_pin_state(
        &self,
        state: &LsmTree,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<Vec<u8>>> {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state.read_visible_point(
            key,
            read_sequence,
            self.persistent_path(),
            Some(self.inner.block_cache.as_ref()),
            Some(self.inner.blob_reads.as_ref()),
        )
    }

    pub(crate) async fn get_at_state_with_pin_state_async(
        &self,
        state: &LsmTree,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<Vec<u8>>> {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state
            .read_visible_point_async(
                &self.inner.native_storage,
                key,
                read_sequence,
                self.persistent_path(),
                Some(self.inner.block_cache.as_ref()),
                Some(self.inner.blob_reads.as_ref()),
            )
            .await
    }

    pub(crate) fn get_value_at_state_snapshot_with_pin_state(
        &self,
        state: &LsmTree,
        read_snapshot: &LsmPointReadSnapshot,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<PointValue>> {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state.read_visible_point_value_in_snapshot(
            read_snapshot,
            key,
            read_sequence,
            self.persistent_path(),
            Some(self.inner.block_cache.as_ref()),
            Some(self.inner.blob_reads.as_ref()),
        )
    }

    pub(crate) fn get_values_at_state_snapshot_with_pin_state<K>(
        &self,
        state: &LsmTree,
        read_snapshot: &LsmPointReadSnapshot,
        keys: &[K],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Vec<Option<PointValue>>>
    where
        K: AsRef<[u8]>,
    {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state.read_visible_point_values_in_snapshot(
            read_snapshot,
            keys,
            read_sequence,
            self.persistent_path(),
            Some(self.inner.block_cache.as_ref()),
            Some(self.inner.blob_reads.as_ref()),
        )
    }

    pub(crate) async fn get_value_at_state_snapshot_with_pin_state_async(
        &self,
        state: &LsmTree,
        read_snapshot: &LsmPointReadSnapshot,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<PointValue>> {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state
            .read_visible_point_value_in_snapshot_async(
                read_snapshot,
                key,
                read_sequence,
                AsyncPointReadIo::new(
                    &self.inner.native_storage,
                    self.persistent_path(),
                    Some(self.inner.block_cache.as_ref()),
                    Some(self.inner.blob_reads.as_ref()),
                ),
            )
            .await
    }

    pub(crate) async fn get_values_at_state_snapshot_with_pin_state_async<K>(
        &self,
        state: &LsmTree,
        read_snapshot: &LsmPointReadSnapshot,
        keys: &[K],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Vec<Option<PointValue>>>
    where
        K: AsRef<[u8]>,
    {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state
            .read_visible_point_values_in_snapshot_async(
                read_snapshot,
                keys,
                read_sequence,
                AsyncPointReadIo::new(
                    &self.inner.native_storage,
                    self.persistent_path(),
                    Some(self.inner.block_cache.as_ref()),
                    Some(self.inner.blob_reads.as_ref()),
                ),
            )
            .await
    }

    pub(crate) fn reader_for_state<'snapshot>(
        &self,
        state: &Arc<LsmTree>,
        snapshot: &'snapshot Snapshot,
    ) -> Result<BucketReader<'snapshot>> {
        self.reader_for_state_at_sequence(
            state,
            self.snapshot_sequence(snapshot)?,
            snapshot.is_pinned(),
        )
    }

    pub(crate) fn reader_for_state_at_sequence<'snapshot>(
        &self,
        state: &Arc<LsmTree>,
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<BucketReader<'snapshot>> {
        self.ensure_open()?;
        let read_pin =
            (!read_pin_held).then(|| self.inner.snapshots.pinned_snapshot(read_sequence));
        let read_snapshot = state.point_read_snapshot(read_sequence)?;
        Ok(BucketReader::new(
            self.clone(),
            Arc::clone(state),
            read_snapshot,
            read_sequence,
            read_pin,
        ))
    }

    pub(crate) fn reader_for_state_keys_at_sequence<'snapshot, K>(
        &self,
        state: &Arc<LsmTree>,
        keys: &[K],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<BucketReader<'snapshot>>
    where
        K: AsRef<[u8]>,
    {
        self.ensure_open()?;
        let read_pin =
            (!read_pin_held).then(|| self.inner.snapshots.pinned_snapshot(read_sequence));
        let read_snapshot = state.point_read_snapshot_for_keys(keys, read_sequence)?;
        Ok(BucketReader::new(
            self.clone(),
            Arc::clone(state),
            read_snapshot,
            read_sequence,
            read_pin,
        ))
    }

    fn scan_sources_for_state_at_sequence(
        &self,
        state: &Arc<LsmTree>,
        selector: &ScanSelector,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<ScanSourceInput> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);
        let scan = state.scan(
            selector,
            direction,
            read_sequence,
            Some(&self.inner.block_cache),
        )?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());
        Ok(ScanSourceInput {
            read_sequence,
            read_pin,
            db_path,
            native_storage,
            blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
            scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
            range_tombstones: scan.range_tombstones,
            sources: scan.sources,
        })
    }

    async fn scan_sources_for_state_at_sequence_async(
        &self,
        state: &Arc<LsmTree>,
        selector: &ScanSelector,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<ScanSourceInput> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);
        let scan = state
            .scan_async(
                selector,
                direction,
                read_sequence,
                Some(&self.inner.block_cache),
            )
            .await?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());
        Ok(ScanSourceInput {
            read_sequence,
            read_pin,
            db_path,
            native_storage,
            blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
            scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
            range_tombstones: scan.range_tombstones,
            sources: scan.sources,
        })
    }

    pub(crate) fn range_at_state_sequence(
        &self,
        state: &Arc<LsmTree>,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        let input = self.scan_sources_for_state_at_sequence(
            state,
            &ScanSelector::Range(range.clone()),
            read_sequence,
            direction,
        )?;
        Ok(Iter::from_sources(direction, input))
    }

    pub(crate) async fn range_at_state_sequence_async(
        &self,
        state: &Arc<LsmTree>,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        let input = self
            .scan_sources_for_state_at_sequence_async(
                state,
                &ScanSelector::Range(range.clone()),
                read_sequence,
                direction,
            )
            .await?;
        Ok(Iter::from_sources(direction, input))
    }

    pub(crate) fn range_lazy_at_state_sequence(
        &self,
        state: &Arc<LsmTree>,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        let input = self.scan_sources_for_state_at_sequence(
            state,
            &ScanSelector::Range(range.clone()),
            read_sequence,
            direction,
        )?;
        Ok(LazyIter::from_sources(direction, input))
    }

    pub(crate) async fn range_lazy_at_state_sequence_async(
        &self,
        state: &Arc<LsmTree>,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        let input = self
            .scan_sources_for_state_at_sequence_async(
                state,
                &ScanSelector::Range(range.clone()),
                read_sequence,
                direction,
            )
            .await?;
        Ok(LazyIter::from_sources(direction, input))
    }

    pub(crate) fn prefix_at_state_sequence(
        &self,
        state: &Arc<LsmTree>,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        let input = self.scan_sources_for_state_at_sequence(
            state,
            &ScanSelector::Prefix(prefix.to_vec()),
            read_sequence,
            direction,
        )?;
        Ok(Iter::from_sources(direction, input))
    }

    pub(crate) async fn prefix_at_state_sequence_async(
        &self,
        state: &Arc<LsmTree>,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        let input = self
            .scan_sources_for_state_at_sequence_async(
                state,
                &ScanSelector::Prefix(prefix.to_vec()),
                read_sequence,
                direction,
            )
            .await?;
        Ok(Iter::from_sources(direction, input))
    }

    pub(crate) fn prefix_lazy_at_state_sequence(
        &self,
        state: &Arc<LsmTree>,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        let input = self.scan_sources_for_state_at_sequence(
            state,
            &ScanSelector::Prefix(prefix.to_vec()),
            read_sequence,
            direction,
        )?;
        Ok(LazyIter::from_sources(direction, input))
    }

    pub(crate) async fn prefix_lazy_at_state_sequence_async(
        &self,
        state: &Arc<LsmTree>,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        let input = self
            .scan_sources_for_state_at_sequence_async(
                state,
                &ScanSelector::Prefix(prefix.to_vec()),
                read_sequence,
                direction,
            )
            .await?;
        Ok(LazyIter::from_sources(direction, input))
    }

    pub(crate) fn range_at_sequence(
        &self,
        bucket: &str,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Range(range.clone());
        let scan = state.scan(
            &selector,
            direction,
            read_sequence,
            Some(&self.inner.block_cache),
        )?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) async fn range_at_sequence_async(
        &self,
        bucket: &str,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Range(range.clone());
        let scan = state
            .scan_async(
                &selector,
                direction,
                read_sequence,
                Some(&self.inner.block_cache),
            )
            .await?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) fn range_lazy_at_sequence(
        &self,
        bucket: &str,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Range(range.clone());
        let scan = state.scan(
            &selector,
            direction,
            read_sequence,
            Some(&self.inner.block_cache),
        )?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) async fn range_lazy_at_sequence_async(
        &self,
        bucket: &str,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Range(range.clone());
        let scan = state
            .scan_async(
                &selector,
                direction,
                read_sequence,
                Some(&self.inner.block_cache),
            )
            .await?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) fn prefix_at_sequence(
        &self,
        bucket: &str,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Prefix(prefix.to_vec());
        let scan = state.scan(
            &selector,
            direction,
            read_sequence,
            Some(&self.inner.block_cache),
        )?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) async fn prefix_at_sequence_async(
        &self,
        bucket: &str,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Prefix(prefix.to_vec());
        let scan = state
            .scan_async(
                &selector,
                direction,
                read_sequence,
                Some(&self.inner.block_cache),
            )
            .await?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) fn prefix_lazy_at_sequence(
        &self,
        bucket: &str,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Prefix(prefix.to_vec());
        let scan = state.scan(
            &selector,
            direction,
            read_sequence,
            Some(&self.inner.block_cache),
        )?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) async fn prefix_lazy_at_sequence_async(
        &self,
        bucket: &str,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Prefix(prefix.to_vec());
        let scan = state
            .scan_async(
                &selector,
                direction,
                read_sequence,
                Some(&self.inner.block_cache),
            )
            .await?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }
}
