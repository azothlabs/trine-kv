#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use super::{Arc, background_worker_loop, lock_poisoned};
use super::{
    BACKGROUND_MAINTENANCE_PROGRESS_WAIT, Db, Duration, KeyRange, MaintenanceBudget,
    MaintenanceRequest, Ordering, Result,
};

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
}
