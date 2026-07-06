use std::{
    fs,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::{
    future::Future,
    task::{Context, Poll, Wake, Waker},
};

use super::{
    BACKGROUND_MAINTENANCE_PROGRESS_WAIT, CompactionReservation, Db, Error, MaintenanceBudget,
    MaintenanceCoordinator, compaction_reservations_conflict, record_maintenance_success,
    shutdown_background_workers,
};
use crate::{
    bucket::DEFAULT_BUCKET_NAME,
    object_store::{
        ETag, ObjectClient, ObjectFuture, ObjectMeta, Precondition, PutIf,
        verify_object_client_contract,
    },
    options::{BucketOptions, DbOptions, DurabilityMode, ObjectClientTrustMode},
    runtime::CancellationToken,
    storage::{StorageCapability, StorageReadBackend},
    substrate::ObjectWriterLease,
    types::{KeyRange, ReadVersion, Sequence},
    write_batch::BatchOperation,
};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
struct ThreadWaker {
    thread: thread::Thread,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn block_on_test_future<T>(future: impl Future<Output = crate::Result<T>>) -> crate::Result<T> {
    let waker = Waker::from(Arc::new(ThreadWaker {
        thread: thread::current(),
    }));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => thread::park_timeout(Duration::from_secs(1)),
        }
    }
}

struct UnsafePutIfObjectStore {
    inner: Arc<dyn ObjectClient>,
}

impl ObjectClient for UnsafePutIfObjectStore {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        self.inner.get(key)
    }

    fn get_range<'op>(&'op self, key: &str, offset: u64, len: u64) -> ObjectFuture<'op, Arc<[u8]>> {
        self.inner.get_range(key, offset, len)
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        self.inner.put(key, bytes)
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        self.inner.delete(key)
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        self.inner.head(key)
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        _precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        let inner = Arc::clone(&self.inner);
        let key = key.to_owned();
        Box::pin(async move {
            let etag = inner.put(&key, bytes).await?;
            Ok(PutIf::Stored { etag })
        })
    }
}

mod basic;
mod maintenance;
