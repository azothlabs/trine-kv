use std::future::Future;
use std::path::Path;

use crate::{
    db::{DatabaseStorageRef, Db},
    error::{Error, Result},
};

impl Db {
    pub(crate) fn block_on_sync_api<T>(
        &self,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        if self.inner.options.storage_mode.is_object_store_persistent()
            || self.inner.options.storage_mode.is_browser_persistent()
        {
            return Err(Error::unsupported_backend(
                "selected storage backend requires the async API",
            ));
        }
        futures::executor::block_on(future)
    }

    pub(in crate::db) fn storage_read_path(&self) -> Option<&Path> {
        match self.inner.storage.resources() {
            DatabaseStorageRef::Memory(_) => None,
            DatabaseStorageRef::Filesystem(resources) => Some(resources.root),
            DatabaseStorageRef::ObjectStore(resources) => Some(resources.prefix),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            DatabaseStorageRef::Browser(resources) => Some(resources.root),
        }
    }
}
