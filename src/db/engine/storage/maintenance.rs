use crate::{
    db::{Db, MaintenanceBudget, MaintenanceOutcome},
    error::{Error, Result},
    types::KeyRange,
};

impl Db {
    /// Runs cooperative flush and compaction work within `budget`.
    ///
    /// This method lets applications do foreground maintenance in small pieces.
    /// It first tries flush work, then compaction work, and returns a
    /// [`MaintenanceOutcome`] describing completed work, budget exhaustion, or
    /// contention with another maintenance worker.
    ///
    /// # Parameters
    ///
    /// - `budget`: carries separate limits for flush inputs and compaction
    ///   inputs. A call may consume work from both limits; if flushing reports
    ///   busy or budget exhaustion, this call skips the follow-up compaction
    ///   pass.
    ///
    /// # Errors
    ///
    /// Returns the same categories as [`Db::flush_sync`] and
    /// [`Db::compact_range_sync`].
    pub fn run_maintenance_with_budget_sync(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.take_background_maintenance_error()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent maintenance requires async maintenance",
            ));
        }
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return Err(Error::unsupported_backend(
                "object-store maintenance requires the async API",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(MaintenanceOutcome::default());
        };
        let db_path = path.to_path_buf();
        let mut outcome = MaintenanceOutcome::default();
        let mut should_compact = self.l0_pressure_exceeded()?;

        if self.has_immutable_memtables()? {
            let (flush_should_compact, flush_outcome) =
                self.run_flush_once_with_budget(&db_path, false, budget)?;
            should_compact |= flush_should_compact;
            outcome.add_assign(flush_outcome);
        }

        if should_compact {
            let compaction_outcome =
                self.run_compaction_once_with_budget(&db_path, &KeyRange::all(), true, budget)?;
            outcome.add_assign(compaction_outcome);
        }

        if outcome.made_progress() {
            self.cleanup_pending_obsolete_table_files(&db_path)?;
            self.cleanup_pending_obsolete_blob_files(&db_path)?;
        }
        self.take_background_maintenance_error()?;
        Ok(outcome)
    }
}
