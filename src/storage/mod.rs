//! Storage capability contracts and their built-in backend implementations.

use std::{
    fs::{self, File, OpenOptions},
    future::Future,
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    block::BlockReadSource,
    durability::{
        requires_parent_dir_sync_after_rename, sync_dir_after_renames, sync_parent_dir_after_rename,
    },
    error::{Error, Result},
    io::{
        BlockingAdapterIoDriver, InlineIoDriver, IoAppendObject, IoCompletion, IoDriver,
        IoReadObject,
    },
    options::DurabilityMode,
    runtime::Runtime,
    stats::{PlatformIoOperationStats, StorageOperationMetric, StorageOperationStats},
};

#[cfg(feature = "platform-io")]
use crate::io::{
    PlatformIoAppendSession, PlatformIoDriver, PlatformIoOperation, PlatformIoPublishPlan,
    PlatformIoTaskClass,
};
#[cfg(feature = "platform-io")]
use crate::stats::PlatformIoClassCounters;

mod capabilities;
mod memory;
mod native_backend;
mod object;
mod read_contract;
mod traits;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[allow(dead_code)]
mod browser;
#[cfg(test)]
pub(crate) mod fault_injection;
mod metrics;
mod native_file;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) use browser::{BrowserStorageBackend, BrowserStorageObject, BrowserWriterLease};
pub(crate) use capabilities::{StorageCapabilities, StorageCapability};
pub(crate) use memory::{MemoryStorageBackend, MemoryStorageObject};
pub(crate) use metrics::{NativeFileStorageMetrics, NativeFileStorageStats};
pub(crate) use native_backend::NativeFileBackend;
pub(crate) use native_file::*;
#[cfg(feature = "platform-io")]
pub(crate) use object::max_whole_object_read_bytes;
pub(crate) use object::{
    OBJECT_LIST_PAGE_SIZE, StorageDirectoryFile, StorageDirectoryId, StorageObjectId,
    StorageObjectKind, StorageObjectListPage, StorageObjectListRequest,
    ensure_whole_object_read_len,
};
pub(crate) use read_contract::{
    StorageFuture, StorageReadBuffer, StorageReadFuture, StorageSharedBound, StorageThreadBound,
};
pub(crate) use traits::*;

use metrics::{StorageOperation, record_timed_storage_future, record_timed_storage_result};

fn poll_ready_storage_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => Err(Error::unsupported_backend(
            "runtime for pending storage future",
        )),
    }
}

fn allocate_read_buffer(len: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| Error::invalid_options("storage read length exceeds addressable memory"))?;
    bytes.resize(len, 0);
    Ok(bytes)
}

fn usize_to_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u64::MAX")))
}

fn u64_to_usize(value: u64, field: &'static str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| Error::invalid_options(format!("{field} exceeds usize::MAX")))
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[allow(dead_code)]
mod browser_storage_bound_check {
    use std::{cell::Cell, rc::Rc};

    use super::{StorageObjectId, StorageObjectKind, StorageReadFuture, StorageReadObject};

    struct LocalReadObject {
        object: StorageObjectId,
        byte: Rc<Cell<u8>>,
    }

    impl LocalReadObject {
        fn new() -> Self {
            Self {
                object: StorageObjectId::memory(StorageObjectKind::Temporary, "local"),
                byte: Rc::new(Cell::new(0)),
            }
        }
    }

    impl StorageReadObject for LocalReadObject {
        fn object(&self) -> &StorageObjectId {
            &self.object
        }

        fn len(&self) -> StorageReadFuture<'_, u64> {
            let byte = Rc::clone(&self.byte);
            Box::pin(async move { Ok(u64::from(byte.get())) })
        }

        fn read_exact_at<'op>(
            &'op self,
            _offset: usize,
            bytes: &'op mut [u8],
        ) -> StorageReadFuture<'op, ()> {
            let byte = Rc::clone(&self.byte);
            Box::pin(async move {
                bytes.fill(byte.get());
                Ok(())
            })
        }
    }

    fn accepts_thread_local_read_object() -> LocalReadObject {
        LocalReadObject::new()
    }
}

#[cfg(test)]
mod tests;
