#[cfg(not(target_os = "wasi"))]
use super::{Arc, PathBuf, mpsc};
use super::{
    AtomicBool, DurabilityMode, Error, NativeFileBackend, Ordering, Path, PendingWalAppend, Result,
    Sequence, WAL_FILE_NAME, WAL_SHARD_FILE_DIGITS, WAL_SHARD_FILE_PREFIX, WalBatch,
    WalFrontDoorLane, WalLaneCommand, WalLaneCompletion, WalLaneReply, WalLaneWaiter, WalWriter,
    delete_confirmed_wal_marker_with_backend, invalid_wal, is_wal_rewrite_temporary_file_name,
    read_confirmed_wal_marker_with_backend, read_wal_object_with_backend_async,
    rewrite_batches_after_with_backend_async, wait_for_wal_storage_future,
    write_confirmed_wal_marker_with_backend,
};

pub(super) fn send_wal_lane_command(
    lane: &WalFrontDoorLane,
    command: impl FnOnce(WalLaneReply) -> WalLaneCommand,
) -> Result<()> {
    enqueue_wal_lane_command(lane, command)?.wait()
}

pub(super) fn enqueue_wal_lane_command(
    lane: &WalFrontDoorLane,
    command: impl FnOnce(WalLaneReply) -> WalLaneCommand,
) -> Result<WalLaneWaiter> {
    #[cfg(target_os = "wasi")]
    {
        let (reply, waiter) = WalLaneCompletion::pair();
        let mut state = lane
            .state
            .lock()
            .map_err(|_| wal_front_door_completion_poisoned())?;
        process_wal_lane_batch(
            &lane.backend,
            &lane.path,
            &lane.writer_open,
            &mut state,
            vec![command(reply)],
        );
        if waiter
            .completion
            .result
            .lock()
            .map_err(|_| wal_front_door_completion_poisoned())?
            .is_none()
        {
            waiter.completion.complete(Err(Error::Corruption {
                message: "WASI WAL lane command did not complete synchronously".to_owned(),
            }));
        }
        return Ok(waiter);
    }

    #[cfg(not(target_os = "wasi"))]
    {
        let sender = lane
            .sender
            .as_ref()
            .ok_or_else(wal_front_door_worker_stopped)?;
        let (reply, waiter) = WalLaneCompletion::pair();
        sender
            .send(command(reply))
            .map_err(|_| wal_front_door_worker_stopped())?;
        Ok(waiter)
    }
}

#[cfg(not(target_os = "wasi"))]
pub(super) fn try_enqueue_wal_lane_command(
    lane: &WalFrontDoorLane,
    command: impl FnOnce(WalLaneReply) -> WalLaneCommand,
) -> Result<WalLaneWaiter> {
    let sender = lane
        .sender
        .as_ref()
        .ok_or_else(wal_front_door_worker_stopped)?;
    let (reply, waiter) = WalLaneCompletion::pair();
    match sender.try_send(command(reply)) {
        Ok(()) => Ok(waiter),
        Err(mpsc::TrySendError::Full(_)) => Err(Error::runtime_busy(
            "WAL admission queue is full; retry after pending commits drain",
        )),
        Err(mpsc::TrySendError::Disconnected(_)) => Err(wal_front_door_worker_stopped()),
    }
}

#[allow(clippy::needless_pass_by_value)]
/// Maximum commits coalesced into one group-commit fsync. Bounds the latency and
/// memory of a single drain pass when the queue is flooded.
#[cfg(not(target_os = "wasi"))]
pub(super) const WAL_LANE_BATCH_MAX: usize = 1024;

#[derive(Debug, Default)]
pub(super) struct WalLaneWorkerState {
    writer: Option<WalWriter>,
    persisted_level: Option<DurabilityMode>,
    last_appended_sequence: Option<Sequence>,
    confirmed_sequence: Option<Sequence>,
    terminal_error: Option<String>,
}

// Thread entry point: it owns its lane state for the worker's lifetime.
#[allow(clippy::needless_pass_by_value)]
#[cfg(not(target_os = "wasi"))]
pub(super) fn run_wal_lane_worker(
    backend: NativeFileBackend,
    path: PathBuf,
    writer_open: Arc<AtomicBool>,
    receiver: mpsc::Receiver<WalLaneCommand>,
) {
    let mut state = WalLaneWorkerState::default();
    // Group commit: block for one command, then drain everything already queued
    // and serve the whole batch with a single fsync. Concurrent writers (or one
    // writer with many in-flight async commits) amortize the fsync cost; each
    // writer is still only completed after the fsync that covers its frame.
    while let Ok(first) = receiver.recv() {
        let mut batch = Vec::with_capacity(WAL_LANE_BATCH_MAX);
        batch.push(first);
        while batch.len() < WAL_LANE_BATCH_MAX {
            match receiver.try_recv() {
                Ok(command) => batch.push(command),
                Err(_) => break,
            }
        }
        let panic_replies = batch.iter().map(WalLaneCommand::reply).collect::<Vec<_>>();
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            process_wal_lane_batch(&backend, &path, &writer_open, &mut state, batch);
        }))
        .is_err()
        {
            writer_open.store(false, Ordering::Release);
            for reply in panic_replies {
                reply.complete(Err(wal_lane_worker_panicked()));
            }
            // Remain alive only to fail commands that raced with the panic.
            // Dropping queued replies would strand their waiters forever.
            while let Ok(command) = receiver.recv() {
                command.reply().complete(Err(wal_lane_worker_panicked()));
            }
            return;
        }
    }
}

#[cfg(not(target_os = "wasi"))]
fn wal_lane_worker_panicked() -> Error {
    Error::Corruption {
        message:
            "WAL lane worker panicked after storage mutation may have started; reopen the database"
                .to_owned(),
    }
}

pub(super) fn process_wal_lane_batch(
    backend: &NativeFileBackend,
    path: &Path,
    writer_open: &AtomicBool,
    state: &mut WalLaneWorkerState,
    batch: Vec<WalLaneCommand>,
) {
    // Appended-but-not-yet-persisted waiters and the strongest durability any
    // of them requested. They are completed together by the next persist.
    let mut pending: Vec<PendingWalAppend> = Vec::new();
    let mut pending_durability = DurabilityMode::Buffered;

    for command in batch {
        if let Some(message) = state.terminal_error.as_deref() {
            command.reply().complete(Err(wal_lane_failed(message)));
            continue;
        }
        match command {
            WalLaneCommand::Append {
                sequence,
                frame,
                durability,
                reply,
            } => process_wal_append(
                backend,
                path,
                writer_open,
                state,
                &mut pending,
                &mut pending_durability,
                PendingWalAppend { sequence, reply },
                &frame,
                durability,
            ),
            WalLaneCommand::Persist { durability, reply } => {
                let combined =
                    if wal_durability_rank(durability) > wal_durability_rank(pending_durability) {
                        durability
                    } else {
                        pending_durability
                    };
                let result =
                    flush_wal_lane_batch(backend, path, writer_open, state, combined, &mut pending);
                reply.complete(duplicate_wal_lane_result(&result));
                pending_durability = DurabilityMode::Buffered;
            }
            WalLaneCommand::Rewrite {
                replay_floor,
                reply,
            } => {
                // A rewrite changes the file; flush queued appends first.
                let flush_result = flush_wal_lane_batch(
                    backend,
                    path,
                    writer_open,
                    state,
                    pending_durability,
                    &mut pending,
                );
                pending_durability = DurabilityMode::Buffered;
                let result = flush_result.and_then(|()| {
                    rewrite_wal_lane_after_replay_floor(
                        backend,
                        path,
                        &mut state.writer,
                        &mut state.persisted_level,
                        replay_floor,
                    )
                });
                if let Err(error) = &result
                    && state.terminal_error.is_none()
                {
                    let message = format!(
                        "WAL rewrite failed after the file namespace may have changed: {error}; \
                         the lane is closed until database reopen"
                    );
                    state.writer = None;
                    state.persisted_level = None;
                    state.terminal_error = Some(message);
                    writer_open.store(false, Ordering::Release);
                }
                reply.complete(result);
            }
        }
    }

    let _ = flush_wal_lane_batch(
        backend,
        path,
        writer_open,
        state,
        pending_durability,
        &mut pending,
    );
}

#[allow(clippy::too_many_arguments)]
fn process_wal_append(
    backend: &NativeFileBackend,
    path: &Path,
    writer_open: &AtomicBool,
    state: &mut WalLaneWorkerState,
    pending: &mut Vec<PendingWalAppend>,
    pending_durability: &mut DurabilityMode,
    append: PendingWalAppend,
    frame: &[u8],
    durability: DurabilityMode,
) {
    // Append without persisting; the batch persist below covers it.
    match append_wal_lane_frame(
        backend,
        path,
        &mut state.writer,
        writer_open,
        frame,
        DurabilityMode::Buffered,
    ) {
        Ok(()) => {
            if wal_durability_rank(durability) > wal_durability_rank(*pending_durability) {
                *pending_durability = durability;
            }
            // Mark the lane dirty: these bytes are not yet covered by the
            // requested storage boundary, so a later persist must touch the
            // backend.
            state.persisted_level = Some(DurabilityMode::Buffered);
            state.last_appended_sequence = Some(append.sequence);
            pending.push(append);
        }
        Err(error) => {
            let message = format!(
                "WAL append failed after the file may have changed: {error}; \
                 the lane is closed until database reopen"
            );
            state.writer = None;
            state.persisted_level = None;
            state.terminal_error = Some(message.clone());
            writer_open.store(false, Ordering::Release);
            for pending_append in pending.drain(..) {
                pending_append
                    .reply
                    .complete(Err(wal_lane_failed(message.as_str())));
            }
            append.reply.complete(Err(error));
        }
    }
}

/// Persist the buffered appends with one backend request and complete waiters.
///
/// When `pending` is non-empty there are freshly appended bytes, so a persist at
/// `durability` is forced; for a standalone persist with no new appends the
/// existing `persisted_level` can satisfy it without a redundant backend call.
pub(super) fn flush_wal_lane_batch(
    backend: &NativeFileBackend,
    path: &Path,
    writer_open: &AtomicBool,
    state: &mut WalLaneWorkerState,
    durability: DurabilityMode,
    pending: &mut Vec<PendingWalAppend>,
) -> Result<()> {
    let has_new_appends = !pending.is_empty();
    let pending_max_sequence = pending.iter().map(|append| append.sequence).max();
    let result = persist_wal_lane_batch(
        backend,
        path,
        state,
        durability,
        has_new_appends,
        pending_max_sequence.or(state.last_appended_sequence),
    );
    if let Err(error) = &result {
        let message = format!(
            "WAL persist failed with an uncertain durability boundary: {error}; \
             the lane is closed until database reopen"
        );
        state.writer = None;
        state.persisted_level = None;
        state.terminal_error = Some(message);
        writer_open.store(false, Ordering::Release);
    }
    for pending in pending.drain(..) {
        let reply = pending.reply;
        reply.complete(duplicate_wal_lane_result(&result));
    }
    result
}

fn wal_lane_failed(message: &str) -> Error {
    Error::Corruption {
        message: message.to_owned(),
    }
}

pub(super) fn persist_wal_lane_batch(
    backend: &NativeFileBackend,
    path: &Path,
    state: &mut WalLaneWorkerState,
    durability: DurabilityMode,
    has_new_appends: bool,
    confirm_sequence: Option<Sequence>,
) -> Result<()> {
    let Some(writer) = state.writer.as_mut() else {
        return Ok(());
    };
    if !wal_durability_requires_persist(durability) {
        return Ok(());
    }
    // Freshly appended bytes are unpersisted, so a backend persist is mandatory;
    // a standalone persist can skip when the lane is already covered.
    if has_new_appends || wal_lane_needs_persist(state.persisted_level, durability) {
        writer.persist(durability)?;
        state.persisted_level = Some(durability);
    }
    if let Some(sequence) = confirm_sequence
        && state
            .confirmed_sequence
            .is_none_or(|confirmed| sequence > confirmed)
    {
        write_confirmed_wal_marker_with_backend(backend, path, sequence, durability)?;
        state.confirmed_sequence = Some(sequence);
    }
    Ok(())
}

pub(super) const fn wal_durability_requires_persist(durability: DurabilityMode) -> bool {
    wal_durability_rank(durability) >= wal_durability_rank(DurabilityMode::Flush)
}

/// `Error` is not `Clone`, so reproduce it for each fan-out waiter, preserving
/// the I/O error kind and message for the common fsync-failure case.
pub(super) fn duplicate_wal_lane_result(result: &Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(Error::Io(error)) => Err(Error::Io(std::io::Error::new(
            error.kind(),
            error.to_string(),
        ))),
        Err(error) => Err(Error::Corruption {
            message: format!("group commit persist failed: {error}"),
        }),
    }
}

pub(super) fn append_wal_lane_frame(
    backend: &NativeFileBackend,
    path: &Path,
    writer: &mut Option<WalWriter>,
    writer_open: &AtomicBool,
    frame: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    if writer.is_none() {
        *writer = Some(WalWriter::open_append_with_backend(backend, path)?);
        writer_open.store(true, Ordering::Release);
    }
    writer
        .as_mut()
        .expect("writer opens before append")
        .append_frame(frame, durability)
}

pub(super) fn persist_wal_lane(
    writer: &mut Option<WalWriter>,
    persisted_level: &mut Option<DurabilityMode>,
    durability: DurabilityMode,
) -> Result<()> {
    if let Some(writer) = writer.as_mut()
        && wal_lane_needs_persist(*persisted_level, durability)
    {
        writer.persist(durability)?;
        *persisted_level = Some(durability);
    }
    Ok(())
}

pub(super) fn wal_lane_needs_persist(
    persisted_level: Option<DurabilityMode>,
    durability: DurabilityMode,
) -> bool {
    persisted_level.is_none_or(|level| wal_durability_rank(level) < wal_durability_rank(durability))
}

pub(super) const fn wal_durability_rank(mode: DurabilityMode) -> u8 {
    match mode {
        DurabilityMode::Buffered => 0,
        DurabilityMode::Flush => 1,
        DurabilityMode::SyncData => 2,
        DurabilityMode::SyncAll => 3,
        DurabilityMode::SyncAllStrict => 4,
    }
}

pub(super) fn rewrite_wal_lane_after_replay_floor(
    backend: &NativeFileBackend,
    path: &Path,
    writer: &mut Option<WalWriter>,
    persisted_level: &mut Option<DurabilityMode>,
    replay_floor: Sequence,
) -> Result<()> {
    let rewrite_durability = filesystem_wal_rewrite_durability();
    if writer.is_some() {
        persist_wal_lane(writer, persisted_level, rewrite_durability)?;
    } else if wait_for_wal_storage_future(read_wal_object_with_backend_async(backend, path))?
        .is_none()
    {
        return Ok(());
    }
    wait_for_wal_storage_future(rewrite_batches_after_with_backend_async(
        backend,
        path,
        replay_floor,
    ))?;
    if read_confirmed_wal_marker_with_backend(backend, path)?
        .is_some_and(|sequence| sequence <= replay_floor)
    {
        delete_confirmed_wal_marker_with_backend(backend, path)?;
    }
    if let Some(writer) = writer.as_mut() {
        writer.reopen_append_with_backend(backend, path)?;
        *persisted_level = Some(rewrite_durability);
    }
    Ok(())
}

const fn filesystem_wal_rewrite_durability() -> DurabilityMode {
    #[cfg(target_os = "wasi")]
    {
        DurabilityMode::Flush
    }

    #[cfg(not(target_os = "wasi"))]
    {
        DurabilityMode::SyncAll
    }
}

#[cfg(not(target_os = "wasi"))]
pub(super) fn wal_front_door_worker_stopped() -> Error {
    Error::Corruption {
        message: "WAL front door worker stopped".to_owned(),
    }
}

pub(super) fn wal_front_door_completion_poisoned() -> Error {
    Error::runtime_busy("WAL front door completion state is poisoned")
}

pub(super) fn validate_wal_stream_order(batches: &[WalBatch]) -> Result<()> {
    let mut last_seen = Sequence::ZERO;
    for batch in batches {
        if batch.sequence <= last_seen {
            return Err(invalid_wal("WAL stream sequence did not increase"));
        }
        last_seen = batch.sequence;
    }
    Ok(())
}

pub(super) fn wal_shard_index_from_path(path: &Path) -> Result<usize> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::Corruption {
            message: format!("WAL file name is not valid UTF-8: {}", path.display()),
        })?;
    wal_shard_index_from_file_name(file_name)?.ok_or_else(|| Error::Corruption {
        message: format!("not a WAL shard file: {}", path.display()),
    })
}

pub(super) fn wal_shard_index_from_file_name(file_name: &str) -> Result<Option<usize>> {
    if file_name == WAL_FILE_NAME {
        return Ok(Some(0));
    }
    if is_wal_rewrite_temporary_file_name(file_name) {
        return Ok(None);
    }
    wal_shard_index_from_final_file_name(file_name)
}

pub(super) fn wal_shard_index_from_final_file_name(file_name: &str) -> Result<Option<usize>> {
    let Some(suffix) = file_name.strip_prefix(WAL_SHARD_FILE_PREFIX) else {
        return Ok(None);
    };
    if suffix.len() != WAL_SHARD_FILE_DIGITS || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::Corruption {
            message: format!("malformed WAL shard file name: {file_name}"),
        });
    }
    let shard_index = suffix.parse::<usize>().map_err(|error| Error::Corruption {
        message: format!("malformed WAL shard file name {file_name}: {error}"),
    })?;
    if shard_index == 0 {
        return Err(Error::Corruption {
            message: "WAL shard 0 must use the legacy trine.wal file name".to_owned(),
        });
    }
    Ok(Some(shard_index))
}
