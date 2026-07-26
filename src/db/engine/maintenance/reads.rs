use super::{
    Arc, AsyncPointReadIo, BucketReader, Db, Direction, Iter, KeyRange, LazyIter,
    LsmPointReadSnapshot, LsmTree, Path, PointValue, Result, ScanSelector, ScanSourceInput,
    Sequence, Snapshot,
};

impl Db {
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

        let backend = self.inner.storage.read_backend();
        state
            .read_visible_point_async(
                &backend,
                key,
                read_sequence,
                self.storage_read_path()?,
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

        let backend = self.inner.storage.read_backend();
        state
            .read_visible_point_value_in_snapshot_async(
                read_snapshot,
                key,
                read_sequence,
                AsyncPointReadIo::new(
                    &backend,
                    self.storage_read_path()?,
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

        let backend = self.inner.storage.read_backend();
        state
            .read_visible_point_values_in_snapshot_async(
                read_snapshot,
                keys,
                read_sequence,
                AsyncPointReadIo::new(
                    &backend,
                    self.storage_read_path()?,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());
        Ok(ScanSourceInput {
            read_sequence,
            read_pin,
            db_path,
            read_backend,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());
        Ok(ScanSourceInput {
            read_sequence,
            read_pin,
            db_path,
            read_backend,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                read_backend,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                read_backend,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                read_backend,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                read_backend,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                read_backend,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                read_backend,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                read_backend,
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
        let db_path = self.storage_read_path()?.map(Path::to_path_buf);
        let read_backend = db_path.as_ref().map(|_| self.inner.storage.read_backend());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                read_backend,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                scan_waste: Some(Arc::clone(&self.inner.scan_waste)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }
}
