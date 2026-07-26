use super::{
    CancellationToken, Db, DbInner, Error, MaintenanceCoordinator, Ordering, Weak,
    record_maintenance_success,
};

pub(in crate::db) fn background_worker_loop(
    inner: &Weak<DbInner>,
    maintenance: &MaintenanceCoordinator,
    runtime_shutdown: &CancellationToken,
) {
    while let Some(request) = maintenance.wait_for_request() {
        if runtime_shutdown.is_cancelled() {
            break;
        }
        let Some(inner) = inner.upgrade() else {
            break;
        };
        if inner.closed.load(Ordering::Acquire) || runtime_shutdown.is_cancelled() {
            break;
        }

        let db = Db {
            inner,
            counts_as_user_handle: false,
        };
        match db.run_background_maintenance(request) {
            Ok(()) => record_maintenance_success(maintenance),
            Err(Error::Closed) => break,
            Err(error) => maintenance.record_error(error),
        }
    }
}
