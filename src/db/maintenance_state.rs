//! Maintenance admission, reservations, progress, and RAII guards.

use super::{
    Arc, CancellationToken, CompactionTrigger, Condvar, Duration, Error, KeyRange,
    LsmCompactionInput, LsmCompactionOutput, LsmFlushInput, LsmTree, Mutex, RuntimeTask, table,
};

pub(super) struct NamedFlushInput {
    pub(super) bucket: String,
    pub(super) tree: Arc<LsmTree>,
    pub(super) input: LsmFlushInput,
}

pub(super) struct NamedCompactionInput {
    pub(super) bucket: String,
    pub(super) tree: Arc<LsmTree>,
    pub(super) input: LsmCompactionInput,
}

pub(super) struct NamedCompactionOutput {
    pub(super) bucket: String,
    pub(super) trigger: Option<CompactionTrigger>,
    pub(super) output: LsmCompactionOutput,
}

pub(super) struct PendingCompactionOutputs {
    pub(super) outputs: Vec<NamedCompactionOutput>,
    pub(super) written_table_ids: Vec<table::TableId>,
}

pub(super) struct BlobGcCandidate {
    pub(super) file_id: u64,
    pub(super) total_bytes: u64,
    pub(super) live_bytes: u64,
}

pub(super) struct BlobGcRewriteTable {
    pub(super) bucket: String,
    pub(super) input_table_id: table::TableId,
    pub(super) output_table_id: table::TableId,
    pub(super) level: table::TableLevel,
    pub(super) options: table::TableWriteOptions,
    pub(super) point_records: Vec<table::TablePointRecord>,
    pub(super) range_tombstones: Vec<table::TableRangeTombstone>,
}

pub(super) struct BlobGcRewriteRecord {
    pub(super) internal_key: crate::internal_key::InternalKey,
    pub(super) value: Vec<u8>,
    pub(super) compression: crate::codec::CodecId,
    pub(super) table_index: usize,
    pub(super) record_index: usize,
}

pub(super) struct BlobGcRewritePlan {
    pub(super) candidates: Vec<BlobGcCandidate>,
    pub(super) new_blob_file_id: u64,
    pub(super) tables: Vec<BlobGcRewriteTable>,
    pub(super) records: Vec<BlobGcRewriteRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MaintenanceRequest {
    pub(super) flush: bool,
    pub(super) compaction: bool,
}

impl MaintenanceRequest {
    #[must_use]
    pub(super) const fn any(self) -> bool {
        self.flush || self.compaction
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct WritePressure {
    pub(super) flush: bool,
    pub(super) compaction: bool,
}

impl WritePressure {
    #[must_use]
    pub(super) const fn none(self) -> bool {
        !self.flush && !self.compaction
    }

    #[must_use]
    pub(super) const fn request(self) -> MaintenanceRequest {
        MaintenanceRequest {
            flush: self.flush,
            compaction: self.compaction,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CompactionReservation {
    pub(super) bucket: String,
    pub(super) range: KeyRange,
}

#[derive(Debug)]
pub(super) struct MaintenanceCoordinator {
    pub(super) state: Mutex<MaintenanceState>,
    pub(super) wake: Condvar,
}

#[derive(Debug, Default)]
pub(super) struct MaintenanceState {
    pub(super) flush_requests: usize,
    pub(super) compaction_requests: usize,
    pub(super) active_flushes: usize,
    pub(super) active_compactions: Vec<CompactionReservation>,
    pub(super) progress: u64,
    pub(super) shutdown: bool,
    pub(super) last_error: Option<Error>,
}

#[derive(Debug)]
pub(super) struct MaintenanceFlushGuard {
    pub(super) coordinator: Arc<MaintenanceCoordinator>,
}

#[derive(Debug)]
pub(super) struct MaintenanceCompactionGuard {
    pub(super) coordinator: Arc<MaintenanceCoordinator>,
    pub(super) reservations: Vec<CompactionReservation>,
}

impl MaintenanceCoordinator {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(MaintenanceState::default()),
            wake: Condvar::new(),
        }
    }

    pub(super) fn request(&self, request: MaintenanceRequest) {
        if !request.any() {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            if request.flush {
                state.flush_requests = state.flush_requests.saturating_add(1);
            }
            if request.compaction {
                state.compaction_requests = state.compaction_requests.saturating_add(1);
            }
            self.wake.notify_all();
        }
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(super) fn wait_for_request(&self) -> Option<MaintenanceRequest> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        while state.flush_requests == 0 && state.compaction_requests == 0 && !state.shutdown {
            let Ok(next_state) = self.wake.wait(state) else {
                return None;
            };
            state = next_state;
        }
        if state.shutdown {
            return None;
        }
        let request = MaintenanceRequest {
            flush: state.flush_requests != 0,
            compaction: state.compaction_requests != 0,
        };
        state.flush_requests = 0;
        state.compaction_requests = 0;
        self.wake.notify_all();
        Some(request)
    }

    pub(super) fn progress(&self) -> u64 {
        self.state.lock().map_or(0, |state| state.progress)
    }

    pub(super) fn wait_for_progress(&self, observed_progress: u64, timeout: Duration) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        while state.progress == observed_progress && !state.shutdown && state.last_error.is_none() {
            let Ok((next_state, wait_result)) = self.wake.wait_timeout(state, timeout) else {
                return false;
            };
            state = next_state;
            if wait_result.timed_out() {
                break;
            }
        }
        state.progress != observed_progress || state.shutdown || state.last_error.is_some()
    }

    pub(super) fn wait_until_idle(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while state.active_flushes != 0 || !state.active_compactions.is_empty() {
            let Ok(next_state) = self.wake.wait(state) else {
                return;
            };
            state = next_state;
        }
    }

    pub(super) fn wait_until_flush_idle(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while (state.flush_requests != 0 || state.active_flushes != 0)
            && !state.shutdown
            && state.last_error.is_none()
        {
            let Ok(next_state) = self.wake.wait(state) else {
                return;
            };
            state = next_state;
        }
    }

    pub(super) fn wait_until_compaction_idle(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while (state.compaction_requests != 0 || !state.active_compactions.is_empty())
            && !state.shutdown
            && state.last_error.is_none()
        {
            let Ok(next_state) = self.wake.wait(state) else {
                return;
            };
            state = next_state;
        }
    }

    pub(super) fn has_pending_compaction(&self) -> bool {
        self.state.lock().is_ok_and(|state| {
            state.compaction_requests != 0 || !state.active_compactions.is_empty()
        })
    }

    pub(super) fn try_start_flush(self: &Arc<Self>) -> Option<MaintenanceFlushGuard> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        if state.shutdown || state.active_flushes != 0 || !state.active_compactions.is_empty() {
            return None;
        }
        state.active_flushes = 1;
        Some(MaintenanceFlushGuard {
            coordinator: Arc::clone(self),
        })
    }

    pub(super) fn reserve_compactions(
        self: &Arc<Self>,
        candidates: Vec<CompactionReservation>,
    ) -> Option<MaintenanceCompactionGuard> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        if state.shutdown || state.active_flushes != 0 {
            return None;
        }

        let mut reservations = Vec::new();
        for candidate in candidates {
            if state
                .active_compactions
                .iter()
                .any(|active| compaction_reservations_conflict(active, &candidate))
            {
                continue;
            }
            state.active_compactions.push(candidate.clone());
            reservations.push(candidate);
        }

        if reservations.is_empty() {
            return None;
        }

        Some(MaintenanceCompactionGuard {
            coordinator: Arc::clone(self),
            reservations,
        })
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(super) fn record_error(&self, error: Error) {
        if let Ok(mut state) = self.state.lock() {
            state.last_error = Some(error);
            state.progress = state.progress.saturating_add(1);
            self.wake.notify_all();
        }
    }

    pub(super) fn take_error(&self) -> Option<Error> {
        self.state
            .lock()
            .ok()
            .and_then(|mut state| state.last_error.take())
    }

    pub(super) fn shutdown(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.shutdown = true;
            self.wake.notify_all();
        }
    }

    pub(super) fn finish_flush(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.active_flushes = state.active_flushes.saturating_sub(1);
            state.progress = state.progress.saturating_add(1);
            self.wake.notify_all();
        }
    }

    pub(super) fn finish_compactions(&self, reservations: &[CompactionReservation]) {
        if let Ok(mut state) = self.state.lock() {
            state
                .active_compactions
                .retain(|active| !reservations.iter().any(|finished| finished == active));
            state.progress = state.progress.saturating_add(1);
            self.wake.notify_all();
        }
    }
}

impl Drop for MaintenanceFlushGuard {
    fn drop(&mut self) {
        self.coordinator.finish_flush();
    }
}

impl Drop for MaintenanceCompactionGuard {
    fn drop(&mut self) {
        self.coordinator.finish_compactions(&self.reservations);
    }
}

impl MaintenanceCompactionGuard {
    pub(super) fn contains(&self, bucket: &str, range: &KeyRange) -> bool {
        self.reservations
            .iter()
            .any(|reservation| reservation.bucket == bucket && reservation.range == *range)
    }
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(super) fn record_maintenance_success(_maintenance: &MaintenanceCoordinator) {
    // A later successful maintenance pass must not hide a failure that no
    // caller has observed yet. `take_error` is the only path that clears it.
}

pub(super) fn compaction_reservations_conflict(
    left: &CompactionReservation,
    right: &CompactionReservation,
) -> bool {
    // Every compaction and blob-GC rewrite replaces tables in one bucket's
    // current LSM version. Serializing that replacement boundary prevents two
    // independently planned outputs from installing incompatible views even
    // when their requested key ranges appeared disjoint before either publish.
    left.bucket == right.bucket
}

pub(super) fn shutdown_background_workers(
    maintenance: &Arc<MaintenanceCoordinator>,
    runtime_shutdown: &CancellationToken,
    workers: &Mutex<Vec<RuntimeTask>>,
) {
    runtime_shutdown.cancel();
    maintenance.shutdown();
    let workers = workers
        .lock()
        .map(|mut workers| std::mem::take(&mut *workers))
        .unwrap_or_default();

    for worker in workers {
        if worker.is_current_thread() {
            continue;
        }
        let _ = worker.join();
    }
    maintenance.wait_until_idle();
}
