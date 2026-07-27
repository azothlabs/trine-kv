#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::{path::Path, sync::atomic::Ordering};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::db::{MaintenanceBudget, shutdown_background_workers};
use crate::{
    db::Db,
    error::{Error, Result},
    types::KeyRange,
};

impl Db {
    /// Flushes committed memtable data to persistent table files.
    ///
    /// Flush freezes the currently committed in-memory data up to a stable
    /// sequence, writes immutable memtables to level-0 table files, publishes
    /// the updated manifest, and then removes flushed immutable memtables from
    /// the read path. Readers keep seeing a consistent snapshot while this
    /// happens.
    ///
    /// In-memory databases have no table files, so this returns successfully
    /// without doing storage work. Read-only handles reject flush because it can
    /// publish new durable metadata.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadOnly`] for read-only handles, [`Error::Closed`] for
    /// closed handles, [`Error::UnsupportedBackend`] when the selected backend
    /// requires the async maintenance path, or storage/recovery errors from the
    /// flush and manifest publish steps.
    pub fn flush_sync(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.take_background_maintenance_error()?;
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent flush requires async maintenance",
            ));
        }
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return Err(Error::unsupported_backend(
                "object-store flush requires the async API",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(());
        };
        let db_path = path.to_path_buf();
        let target_sequence = self.freeze_public_flush_target()?;
        let mut should_compact = false;

        while self.has_immutable_memtables_at_or_below(target_sequence)? {
            self.take_background_maintenance_error()?;
            if self.run_flush_once(&db_path, false)? {
                should_compact |= self.l0_pressure_exceeded()?;
                continue;
            }

            self.request_background_flush();
            self.record_cooperative_maintenance_yield();
            self.inner.maintenance.wait_until_flush_idle();
        }

        if should_compact
            || self.l0_pressure_exceeded()?
            || self.foreground_l0_overlap_pressure_exceeded()?
        {
            self.run_compaction_barrier(&db_path, &KeyRange::all(), true)?;
        }
        self.cleanup_pending_obsolete_table_files(&db_path)?;
        self.cleanup_pending_obsolete_blob_files(&db_path)?;
        self.take_background_maintenance_error()?;

        Ok(())
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn flush_native_async(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.take_background_maintenance_error()?;
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return Err(Error::unsupported_backend(
                "object-store flush requires the async API",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(());
        };
        let db_path = path.to_path_buf();
        let target_sequence = self.freeze_public_flush_target()?;
        let mut should_compact = false;

        while self.has_immutable_memtables_at_or_below(target_sequence)? {
            self.take_background_maintenance_error()?;
            let (flush_should_compact, outcome) = self
                .run_flush_once_with_budget_host_async(
                    &db_path,
                    false,
                    MaintenanceBudget::unbounded(),
                )
                .await?;
            if outcome.busy {
                self.request_background_flush();
                self.record_cooperative_maintenance_yield();
                self.inner.maintenance.wait_until_flush_idle();
                continue;
            }
            should_compact |= flush_should_compact;
        }

        if should_compact
            || self.l0_pressure_exceeded()?
            || self.foreground_l0_overlap_pressure_exceeded()?
        {
            self.run_compaction_barrier_native_async(&db_path, &KeyRange::all(), true)
                .await?;
        }
        self.cleanup_pending_obsolete_table_files_native_async(&db_path)
            .await?;
        self.cleanup_pending_obsolete_blob_files_native_async(&db_path)
            .await?;
        self.take_background_maintenance_error()?;

        Ok(())
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn close_native_async(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::Release);
        shutdown_background_workers(
            &self.inner.maintenance,
            &self.inner.runtime_shutdown,
            &self.inner.background_workers,
        );
        self.inner.publish_barrier.close()?;
        if let Some(db_path) = self.persistent_path().map(Path::to_path_buf) {
            self.cleanup_pending_obsolete_table_files_native_async(&db_path)
                .await?;
            self.cleanup_pending_obsolete_blob_files_native_async(&db_path)
                .await?;
        }
        crate::db::release_browser_writer_lease(&self.inner);
        self.inner.substrate.release_writer_lease();
        #[cfg(feature = "platform-io")]
        self.inner.storage.close_platform_io()?;
        Ok(())
    }
}
