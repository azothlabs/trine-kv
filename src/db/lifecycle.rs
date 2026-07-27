//! Database handle cloning, shutdown, and backend resource release.

use super::{
    Arc, DatabaseStorageRef, Db, DbInner, Ordering, cleanup_pending_obsolete_blob_files,
    cleanup_pending_obsolete_table_files, shutdown_background_workers,
};

pub(super) fn release_browser_writer_lease(inner: &DbInner) {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    inner.storage.release_writer_lease();
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    let _ = inner;
}

impl Drop for DbInner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        shutdown_background_workers(
            &self.maintenance,
            &self.runtime_shutdown,
            &self.background_workers,
        );
        if let DatabaseStorageRef::Filesystem(resources) = self.storage.resources() {
            let _ = cleanup_pending_obsolete_table_files(
                resources.files,
                Some(resources.root),
                &self.pending_obsolete_tables,
            );
            let _ = cleanup_pending_obsolete_blob_files(
                resources.files,
                Some(resources.root),
                &self.snapshots,
                self.manifest.as_ref(),
            );
        }
        release_browser_writer_lease(self);
        #[cfg(feature = "platform-io")]
        let _ = self.storage.close_platform_io();
    }
}

impl Clone for Db {
    fn clone(&self) -> Self {
        if self.counts_as_user_handle {
            self.inner.user_handles.fetch_add(1, Ordering::AcqRel);
        }
        Self {
            inner: Arc::clone(&self.inner),
            counts_as_user_handle: self.counts_as_user_handle,
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.counts_as_user_handle {
            return;
        }
        if self.inner.user_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.closed.store(true, Ordering::Release);
            shutdown_background_workers(
                &self.inner.maintenance,
                &self.inner.runtime_shutdown,
                &self.inner.background_workers,
            );
            let _ = self.inner.publish_barrier.close();
            release_browser_writer_lease(&self.inner);
            self.inner.substrate.release_writer_lease();
            #[cfg(feature = "platform-io")]
            let _ = self.inner.storage.close_platform_io();
        }
    }
}
