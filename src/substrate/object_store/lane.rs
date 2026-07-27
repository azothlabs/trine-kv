use std::{
    future::Future,
    io,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::{self, JoinHandle},
};

use crate::{
    error::{Error, Result},
    types::Sequence,
    wal,
};

#[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
use super::lease_state::block_on_substrate_future;
use super::{
    OBJECT_LEASE_RENEW_INTERVAL, OBJECT_WAL_GROUP_COMMIT_DELAY, OBJECT_WAL_MAX_GROUP_FRAME_BYTES,
    OBJECT_WAL_QUEUE_CAPACITY, ObjectWriterLease, lease_state::lock_poisoned_error,
};

pub(super) struct ObjectWalLane {
    sender: Mutex<Option<mpsc::SyncSender<ObjectWalCommand>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for ObjectWalLane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectWalLane")
            .finish_non_exhaustive()
    }
}

impl ObjectWalLane {
    pub(super) fn spawn(lease: ObjectWriterLease, db_path: PathBuf) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(OBJECT_WAL_QUEUE_CAPACITY);
        let future_driver = ObjectWalFutureDriver::new()?;
        let worker = thread::Builder::new()
            .name("trine-object-wal".to_owned())
            .spawn(move || run_object_wal_worker(lease, &db_path, &receiver, &future_driver))
            .map_err(Error::Io)?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
        })
    }

    pub(super) fn accept_commit(&self, sequence: Sequence, frame: Arc<[u8]>) -> Result<()> {
        let completion = self.enqueue_commit(sequence, frame)?;
        completion.wait()
    }

    pub(super) fn enqueue_commit(
        &self,
        sequence: Sequence,
        frame: Arc<[u8]>,
    ) -> Result<Arc<ObjectWalCompletion>> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.try_send(ObjectWalCommand::Accept(ObjectWalAccept {
            sequence,
            frame,
            completion: Arc::clone(&completion),
        }))?;
        Ok(completion)
    }

    pub(super) fn persist(&self) -> Result<()> {
        let mut waiter = self.enqueue_persist()?;
        let completion = waiter.completions.pop().ok_or_else(|| Error::Corruption {
            message: "object WAL persist waiter has no completion".to_owned(),
        })?;
        completion.wait()
    }

    pub(super) fn enqueue_persist(&self) -> Result<ObjectWalWaiter> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.try_send(ObjectWalCommand::Persist {
            completion: Arc::clone(&completion),
        })?;
        Ok(ObjectWalWaiter {
            completions: vec![completion],
        })
    }

    pub(super) fn rewrite_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.send(ObjectWalCommand::Rewrite {
            replay_floor,
            completion: Arc::clone(&completion),
        })?;
        completion.wait()
    }

    pub(super) fn release_writer_lease(&self) -> Result<()> {
        let completion = Arc::new(ObjectWalCompletion::new());
        self.send(ObjectWalCommand::Release {
            completion: Arc::clone(&completion),
        })?;
        completion.wait()
    }

    pub(super) fn send(&self, command: ObjectWalCommand) -> Result<()> {
        let sender = self
            .sender
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL sender"))?;
        let Some(sender) = sender.as_ref() else {
            return Err(Error::Closed);
        };
        sender.send(command).map_err(|_| Error::Closed)
    }

    fn try_send(&self, command: ObjectWalCommand) -> Result<()> {
        let sender = self
            .sender
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL sender"))?;
        let Some(sender) = sender.as_ref() else {
            return Err(Error::Closed);
        };
        sender.try_send(command).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => Error::runtime_busy("object WAL queue is full"),
            mpsc::TrySendError::Disconnected(_) => Error::Closed,
        })
    }
}

impl Drop for ObjectWalLane {
    fn drop(&mut self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
        if let Ok(mut worker) = self.worker.lock()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

pub(super) enum ObjectWalCommand {
    Accept(ObjectWalAccept),
    Persist {
        completion: Arc<ObjectWalCompletion>,
    },
    Rewrite {
        replay_floor: Sequence,
        completion: Arc<ObjectWalCompletion>,
    },
    Release {
        completion: Arc<ObjectWalCompletion>,
    },
}

pub(super) struct ObjectWalAccept {
    pub(super) sequence: Sequence,
    pub(super) frame: Arc<[u8]>,
    pub(super) completion: Arc<ObjectWalCompletion>,
}

pub(super) struct ObjectWalCompletion {
    result: Mutex<Option<Result<()>>>,
    completed: std::sync::atomic::AtomicBool,
    ready: Condvar,
    waker: Mutex<Option<std::task::Waker>>,
}

impl ObjectWalCompletion {
    pub(super) fn new() -> Self {
        Self {
            result: Mutex::new(None),
            completed: std::sync::atomic::AtomicBool::new(false),
            ready: Condvar::new(),
            waker: Mutex::new(None),
        }
    }

    fn complete(&self, result: Result<()>) {
        if self
            .completed
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        if let Ok(mut slot) = self.result.lock() {
            *slot = Some(result);
            self.ready.notify_all();
        }
        if let Ok(mut waker) = self.waker.lock()
            && let Some(waker) = waker.take()
        {
            waker.wake();
        }
    }

    pub(super) fn wait(&self) -> Result<()> {
        let mut slot = self
            .result
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL completion"))?;
        loop {
            if let Some(result) = slot.take() {
                return result;
            }
            slot = self
                .ready
                .wait(slot)
                .map_err(|_| lock_poisoned_error("object WAL completion"))?;
        }
    }

    fn poll_result(&self, context: &mut std::task::Context<'_>) -> std::task::Poll<Result<()>> {
        let Ok(mut slot) = self.result.lock() else {
            return std::task::Poll::Ready(Err(lock_poisoned_error("object WAL completion")));
        };
        if let Some(result) = slot.take() {
            return std::task::Poll::Ready(result);
        }
        drop(slot);
        match self.waker.lock() {
            Ok(mut waker) => *waker = Some(context.waker().clone()),
            Err(_) => {
                return std::task::Poll::Ready(Err(lock_poisoned_error(
                    "object WAL completion waker",
                )));
            }
        }
        let Ok(mut slot) = self.result.lock() else {
            return std::task::Poll::Ready(Err(lock_poisoned_error("object WAL completion")));
        };
        match slot.take() {
            Some(result) => std::task::Poll::Ready(result),
            None => std::task::Poll::Pending,
        }
    }
}

pub(crate) struct ObjectWalWaiter {
    pub(super) completions: Vec<Arc<ObjectWalCompletion>>,
}

impl ObjectWalWaiter {
    #[cfg(not(target_os = "wasi"))]
    pub(super) fn ready() -> Self {
        Self {
            completions: Vec::new(),
        }
    }

    pub(crate) async fn wait(self) -> Result<()> {
        for completion in self.completions {
            std::future::poll_fn(|context| completion.poll_result(context)).await?;
        }
        Ok(())
    }
}

enum ObjectWalFutureDriver {
    #[cfg(all(feature = "s3", not(target_family = "wasm")))]
    TokioHandle(tokio::runtime::Handle),
    #[cfg(all(feature = "s3", not(target_family = "wasm")))]
    OwnedTokio(tokio::runtime::Runtime),
    #[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
    Inline,
}

impl ObjectWalFutureDriver {
    #[allow(clippy::unnecessary_wraps)]
    fn new() -> Result<Self> {
        #[cfg(all(feature = "s3", not(target_family = "wasm")))]
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                return Ok(Self::TokioHandle(handle));
            }
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name("trine-object-wal-io")
                .build()
                .map_err(Error::Io)?;
            Ok(Self::OwnedTokio(runtime))
        }
        #[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
        {
            Ok(Self::Inline)
        }
    }

    fn block_on<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        match self {
            #[cfg(all(feature = "s3", not(target_family = "wasm")))]
            Self::TokioHandle(handle) => handle.block_on(future),
            #[cfg(all(feature = "s3", not(target_family = "wasm")))]
            Self::OwnedTokio(runtime) => runtime.block_on(future),
            #[cfg(not(all(feature = "s3", not(target_family = "wasm"))))]
            Self::Inline => block_on_substrate_future(future),
        }
    }
}

fn run_object_wal_worker(
    mut lease: ObjectWriterLease,
    db_path: &std::path::Path,
    receiver: &mpsc::Receiver<ObjectWalCommand>,
    future_driver: &ObjectWalFutureDriver,
) {
    let mut deferred = None;
    loop {
        let command = match deferred.take() {
            Some(command) => command,
            None => match receiver.recv_timeout(OBJECT_LEASE_RENEW_INTERVAL) {
                Ok(command) => command,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _ = future_driver.block_on(lease.renew());
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            },
        };
        if run_object_wal_command(
            &mut lease,
            db_path,
            receiver,
            future_driver,
            command,
            &mut deferred,
        ) {
            return;
        }
    }
}

fn run_object_wal_command(
    lease: &mut ObjectWriterLease,
    db_path: &std::path::Path,
    receiver: &mpsc::Receiver<ObjectWalCommand>,
    future_driver: &ObjectWalFutureDriver,
    command: ObjectWalCommand,
    deferred: &mut Option<ObjectWalCommand>,
) -> bool {
    match command {
        ObjectWalCommand::Accept(first) => {
            let accepts = collect_object_wal_accepts(first, receiver, deferred);
            let completions = accepts
                .iter()
                .map(|accept| Arc::clone(&accept.completion))
                .collect::<Vec<_>>();
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                complete_object_wal_accepts(lease, db_path, future_driver, accepts)
            })) {
                Ok(false) => {}
                Ok(true) => {
                    fail_object_wal_terminal(
                        receiver,
                        deferred.take(),
                        "object WAL entered a terminal failed state",
                    );
                    return true;
                }
                Err(_) => {
                    complete_object_wal_worker_panic(completions);
                    fail_object_wal_terminal(
                        receiver,
                        deferred.take(),
                        "object WAL worker panicked after durable mutation may have started",
                    );
                    return true;
                }
            }
        }
        ObjectWalCommand::Persist { completion } => {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                future_driver.block_on(lease.renew())
            }));
            let Ok(result) = result else {
                complete_object_wal_worker_panic(vec![completion]);
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL worker panicked while renewing its lease",
                );
                return true;
            };
            let failed = result.is_err();
            completion.complete(result);
            if failed {
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL lease renewal failed and ownership is no longer trusted",
                );
                return true;
            }
        }
        ObjectWalCommand::Rewrite {
            replay_floor,
            completion,
        } => {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rewrite_object_wal(lease, db_path, replay_floor, future_driver)
            }));
            let Ok(result) = result else {
                complete_object_wal_worker_panic(vec![completion]);
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL worker panicked while rewriting the WAL",
                );
                return true;
            };
            let failed = result.is_err();
            completion.complete(result);
            if failed {
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL rewrite failed after storage mutation may have started",
                );
                return true;
            }
        }
        ObjectWalCommand::Release { completion } => {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                future_driver.block_on(lease.release())
            }));
            let Ok(result) = result else {
                complete_object_wal_worker_panic(vec![completion]);
                fail_object_wal_terminal(
                    receiver,
                    deferred.take(),
                    "object WAL worker panicked while releasing its lease",
                );
                return true;
            };
            completion.complete(result);
            fail_object_wal_terminal(receiver, deferred.take(), "object WAL lane was released");
            return true;
        }
    }
    false
}

fn collect_object_wal_accepts(
    first: ObjectWalAccept,
    receiver: &mpsc::Receiver<ObjectWalCommand>,
    deferred: &mut Option<ObjectWalCommand>,
) -> Vec<ObjectWalAccept> {
    let mut accept_bytes = first.frame.len();
    let mut accepts = vec![first];
    while let Ok(command) = receiver.recv_timeout(OBJECT_WAL_GROUP_COMMIT_DELAY) {
        match command {
            ObjectWalCommand::Accept(accept)
                if accept_bytes
                    .checked_add(accept.frame.len())
                    .is_some_and(|bytes| bytes <= OBJECT_WAL_MAX_GROUP_FRAME_BYTES) =>
            {
                accept_bytes += accept.frame.len();
                accepts.push(accept);
            }
            other => {
                *deferred = Some(other);
                break;
            }
        }
        while let Ok(command) = receiver.try_recv() {
            match command {
                ObjectWalCommand::Accept(accept)
                    if accept_bytes
                        .checked_add(accept.frame.len())
                        .is_some_and(|bytes| bytes <= OBJECT_WAL_MAX_GROUP_FRAME_BYTES) =>
                {
                    accept_bytes += accept.frame.len();
                    accepts.push(accept);
                }
                other => {
                    *deferred = Some(other);
                    break;
                }
            }
        }
        if deferred.is_some() {
            break;
        }
    }
    accepts
}

fn rewrite_object_wal(
    lease: &mut ObjectWriterLease,
    db_path: &std::path::Path,
    replay_floor: Sequence,
    future_driver: &ObjectWalFutureDriver,
) -> Result<()> {
    future_driver.block_on(async {
        let deleted = lease
            .rewrite_segment_after_replay_floor(db_path, replay_floor)
            .await?;
        for key in deleted {
            lease.client.delete(&key).await?;
        }
        wal::delete_object_wal_at_or_below_with_backend_async(
            &crate::object_store::ObjectStoreBackend::new(Arc::clone(&lease.client)),
            db_path,
            replay_floor,
        )
        .await
    })
}

fn complete_object_wal_worker_panic(completions: Vec<Arc<ObjectWalCompletion>>) {
    for completion in completions {
        completion.complete(Err(Error::Corruption {
            message:
                "object WAL worker panicked after durable mutation may have started; reopen the database"
                    .to_owned(),
        }));
    }
}

fn fail_object_wal_terminal(
    receiver: &mpsc::Receiver<ObjectWalCommand>,
    deferred: Option<ObjectWalCommand>,
    message: &str,
) {
    if let Some(command) = deferred {
        complete_object_wal_command_terminal(command, message);
    }
    // Stay alive in a terminal failed state until every sender is gone. That
    // closes the race where a sender successfully enqueues after a one-shot
    // drain but before the receiver is dropped, which would otherwise strand
    // its waiter forever.
    while let Ok(command) = receiver.recv() {
        complete_object_wal_command_terminal(command, message);
    }
}

fn complete_object_wal_command_terminal(command: ObjectWalCommand, message: &str) {
    let completion = match command {
        ObjectWalCommand::Accept(accept) => accept.completion,
        ObjectWalCommand::Persist { completion }
        | ObjectWalCommand::Rewrite { completion, .. }
        | ObjectWalCommand::Release { completion } => completion,
    };
    completion.complete(Err(Error::Corruption {
        message: message.to_owned(),
    }));
}

fn complete_object_wal_accepts(
    lease: &mut ObjectWriterLease,
    db_path: &std::path::Path,
    future_driver: &ObjectWalFutureDriver,
    mut accepts: Vec<ObjectWalAccept>,
) -> bool {
    accepts.sort_by_key(|accept| accept.sequence);
    if let Err(error) = future_driver.block_on(lease.refresh_current()) {
        let message = error.to_string();
        let mut accepts = accepts.into_iter();
        if let Some(first) = accepts.next() {
            first.completion.complete(Err(error));
        }
        for accept in accepts {
            accept.completion.complete(Err(Error::runtime_busy(format!(
                "object WAL refresh failed before grouped commit: {message}"
            ))));
        }
        return true;
    }
    let mut previous = lease.state.committed_sequence;
    for accept in &accepts {
        if accept.sequence <= previous {
            let message = format!(
                "object WAL group commit received non-increasing sequence after {}: got {}",
                previous.get(),
                accept.sequence.get()
            );
            for accept in accepts {
                accept.completion.complete(Err(Error::Corruption {
                    message: message.clone(),
                }));
            }
            return true;
        }
        previous = accept.sequence;
    }
    let result = future_driver.block_on(lease.publish_commit_batch(db_path, &accepts));
    match result {
        Ok(()) => {
            for accept in accepts {
                accept.completion.complete(Ok(()));
            }
            false
        }
        Err(error) if accepts.len() == 1 => {
            if let Some(accept) = accepts.pop() {
                accept.completion.complete(Err(error));
            }
            true
        }
        Err(error) => {
            let message = format!("object WAL group commit failed: {error}");
            for accept in accepts {
                accept
                    .completion
                    .complete(Err(Error::Io(io::Error::other(message.clone()))));
            }
            true
        }
    }
}

/// A writer lease held against an object store.
///
/// The lease object carries both a monotonically increasing fencing epoch and a
/// wall-clock expiry. A second writer may acquire only after the observed
/// expiry has passed; while the owner is alive, the WAL worker extends the
/// expiry with CAS writes. A previous holder is fenced out when its lower epoch
/// is rejected before publishing a durable WAL commit or manifest edit.
pub(super) fn object_wal_group_frame_bytes(
    committed_sequence: Sequence,
    accepts: &[ObjectWalAccept],
) -> Result<usize> {
    let empty_frame_bytes = wal::encode_batch_frame(Sequence::ZERO, &[])?.len();
    let mut expected_sequence = committed_sequence
        .checked_next()
        .ok_or_else(|| Error::Corruption {
            message: "object WAL cannot advance past u64::MAX".to_owned(),
        })?
        .get();
    let mut total_bytes = 0usize;
    for (index, accept) in accepts.iter().enumerate() {
        let gap = accept
            .sequence
            .get()
            .checked_sub(expected_sequence)
            .ok_or_else(|| Error::Corruption {
                message: format!(
                    "object WAL expected sequence at least {expected_sequence}, got {}",
                    accept.sequence.get()
                ),
            })?;
        let gap = usize::try_from(gap).map_err(|_| Error::Corruption {
            message: "object WAL skipped sequence count exceeds usize".to_owned(),
        })?;
        let gap_bytes = gap
            .checked_mul(empty_frame_bytes)
            .ok_or_else(|| Error::Corruption {
                message: "object WAL skipped frame size overflow".to_owned(),
            })?;
        total_bytes = total_bytes
            .checked_add(gap_bytes)
            .and_then(|total| total.checked_add(accept.frame.len()))
            .ok_or_else(|| Error::Corruption {
                message: "object WAL group size overflow".to_owned(),
            })?;
        if total_bytes > OBJECT_WAL_MAX_GROUP_FRAME_BYTES {
            return Err(Error::Corruption {
                message: format!(
                    "object WAL group frame length {total_bytes} exceeds maximum {OBJECT_WAL_MAX_GROUP_FRAME_BYTES}"
                ),
            });
        }
        if index + 1 != accepts.len() {
            expected_sequence =
                accept
                    .sequence
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Corruption {
                        message: "object WAL group sequence overflow".to_owned(),
                    })?;
        }
    }
    Ok(total_bytes)
}
