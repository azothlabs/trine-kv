use crate::{
    db::{Db, MaintenanceBudget, MaintenanceOutcome},
    error::{Error, Result},
    types::KeyRange,
};

impl Db {
    /// Compacts table files that overlap `range`.
    ///
    /// Compaction rewrites overlapping table files into lower levels according
    /// to the current bucket options, drops overwritten point versions and
    /// covered range-deleted data that are no longer visible to active
    /// snapshots, and publishes a new manifest. It does not change the
    /// caller-visible result of reads; it changes the on-disk layout and future
    /// read cost.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range whose overlapping table files should be
    ///   considered for compaction. Use [`KeyRange::all`] to compact the whole
    ///   keyspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadOnly`] for read-only handles, [`Error::Closed`] for
    /// closed handles, [`Error::UnsupportedBackend`] when this target requires
    /// async maintenance, or storage/format errors from reading, rewriting, or
    /// publishing table metadata.
    #[allow(clippy::needless_pass_by_value)]
    pub fn compact_range_sync(&self, range: KeyRange) -> Result<()> {
        self.take_background_maintenance_error()?;
        self.compact_range_internal(range)
    }

    /// Compacts table files that overlap `range` within `budget`.
    ///
    /// This is the cooperative form of [`Db::compact_range_sync`]. It performs
    /// only the amount of compaction admitted by `budget` and reports whether
    /// more eligible work remains.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range whose overlapping table files should be
    ///   considered.
    /// - `budget`: maximum flush and compaction inputs to process during this
    ///   call. Zero limits are treated as one by [`MaintenanceBudget::new`].
    #[allow(clippy::needless_pass_by_value)]
    pub fn compact_range_with_budget_sync(
        &self,
        range: KeyRange,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.take_background_maintenance_error()?;
        self.compact_range_with_budget_internal(range, budget)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(in crate::db) fn compact_range_internal(&self, range: KeyRange) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent compaction requires async maintenance",
            ));
        }
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return Err(Error::unsupported_backend(
                "object-store compaction requires the async API",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(());
        };
        let db_path = path.to_path_buf();
        self.run_compaction_barrier(&db_path, &range, false)?;
        // Run cleanup after the compaction call returns so its input table
        // handles are dropped; obsolete files are then uniquely owned by the
        // cleanup queue and reclaimed without waiting for a later pass.
        self.cleanup_pending_obsolete_table_files(&db_path)?;

        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(in crate::db) fn compact_range_with_budget_internal(
        &self,
        range: KeyRange,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent compaction requires async maintenance",
            ));
        }
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return Err(Error::unsupported_backend(
                "object-store compaction requires the async API",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(MaintenanceOutcome::default());
        };
        let db_path = path.to_path_buf();
        self.run_compaction_once_with_budget(&db_path, &range, false, budget)
    }
}
