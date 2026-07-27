use crate::{
    db::{DatabaseStorageRef, Db, MaintenanceBudget, MaintenanceOutcome},
    error::{Error, Result},
    types::KeyRange,
};

impl Db {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn flush_browser_async(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.take_background_maintenance_error()?;

        let DatabaseStorageRef::Browser(resources) = self.inner.storage.resources() else {
            return Err(Error::unsupported_backend(
                "browser flush requires browser storage",
            ));
        };
        let db_path = resources.root;
        let target_sequence = self.freeze_public_flush_target()?;
        let mut should_compact = false;

        while self.has_immutable_memtables_at_or_below(target_sequence)? {
            self.take_background_maintenance_error()?;
            let (flush_should_compact, outcome) = self
                .run_flush_once_with_budget_host_async(
                    db_path,
                    false,
                    MaintenanceBudget::unbounded(),
                )
                .await?;
            if outcome.busy {
                return Err(Error::runtime_busy(
                    "browser persistent flush is already active",
                ));
            }
            should_compact |= flush_should_compact;
            if outcome.flushes == 0 {
                break;
            }
        }

        if should_compact
            || self.l0_pressure_exceeded()?
            || self.foreground_l0_overlap_pressure_exceeded()?
        {
            let outcome = self
                .run_compaction_once_with_budget_host_async(
                    db_path,
                    &KeyRange::all(),
                    true,
                    MaintenanceBudget::unbounded(),
                )
                .await?;
            if outcome.busy {
                return Err(Error::runtime_busy(
                    "browser persistent compaction is already active",
                ));
            }
        }
        self.cleanup_pending_obsolete_table_files_browser_async(db_path)
            .await?;
        self.cleanup_pending_obsolete_blob_files_browser_async(db_path)
            .await?;
        self.take_background_maintenance_error()?;

        Ok(())
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn compact_range_browser_async(&self, range: KeyRange) -> Result<()> {
        self.take_background_maintenance_error()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        let DatabaseStorageRef::Browser(resources) = self.inner.storage.resources() else {
            return Err(Error::unsupported_backend(
                "browser compaction requires browser storage",
            ));
        };
        let outcome = self
            .run_compaction_once_with_budget_host_async(
                resources.root,
                &range,
                false,
                MaintenanceBudget::unbounded(),
            )
            .await?;
        if outcome.busy {
            return Err(Error::runtime_busy(
                "browser persistent compaction is already active",
            ));
        }
        Ok(())
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn compact_range_with_budget_browser_async(
        &self,
        range: KeyRange,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.take_background_maintenance_error()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        let DatabaseStorageRef::Browser(resources) = self.inner.storage.resources() else {
            return Err(Error::unsupported_backend(
                "browser compaction requires browser storage",
            ));
        };
        self.run_compaction_once_with_budget_host_async(resources.root, &range, false, budget)
            .await
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn run_maintenance_with_budget_browser_async(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.take_background_maintenance_error()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        let DatabaseStorageRef::Browser(resources) = self.inner.storage.resources() else {
            return Err(Error::unsupported_backend(
                "browser maintenance requires browser storage",
            ));
        };
        let db_path = resources.root;
        let mut outcome = MaintenanceOutcome::default();
        let mut should_compact = self.l0_pressure_exceeded()?;

        if self.has_immutable_memtables()? {
            let (flush_should_compact, flush_outcome) = self
                .run_flush_once_with_budget_host_async(db_path, false, budget)
                .await?;
            should_compact |= flush_should_compact;
            outcome.add_assign(flush_outcome);
        }

        if should_compact {
            let compaction_outcome = self
                .run_compaction_once_with_budget_host_async(db_path, &KeyRange::all(), true, budget)
                .await?;
            outcome.add_assign(compaction_outcome);
        }

        if outcome.made_progress() {
            self.cleanup_pending_obsolete_table_files_browser_async(db_path)
                .await?;
            self.cleanup_pending_obsolete_blob_files_browser_async(db_path)
                .await?;
        }
        self.take_background_maintenance_error()?;
        Ok(outcome)
    }
}
