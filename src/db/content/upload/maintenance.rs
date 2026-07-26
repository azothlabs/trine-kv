use super::super::{
    ContentUploadInfo, ContentUploadMaintenanceReport, Db, Error, Result, UploadIdRetirement,
    UploadSessionState,
};
use crate::content::ContentUploadState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadMaintenanceOperation {
    ReapInactive,
    PruneSealed,
}

const fn upload_is_eligible(
    state: ContentUploadState,
    updated_at_unix_ms: u64,
    exclusive_cutoff_unix_ms: u64,
    operation: UploadMaintenanceOperation,
) -> bool {
    if updated_at_unix_ms >= exclusive_cutoff_unix_ms {
        return false;
    }
    matches!(
        (operation, state),
        (
            UploadMaintenanceOperation::ReapInactive,
            ContentUploadState::Open | ContentUploadState::Aborting
        ) | (
            UploadMaintenanceOperation::PruneSealed,
            ContentUploadState::Sealed
        )
    )
}

impl Db {
    /// Lists every durable upload state known to this database.
    ///
    /// The result is ordered by [`crate::UploadId`]. It includes open, sealing,
    /// sealed, and aborting states so an operator can distinguish resumable
    /// work from retained idempotency records. Listing is read-only and does
    /// not reserve quota, resume sealing, or delete chunks.
    ///
    /// # Errors
    ///
    /// Returns storage, listing, decoding, and integrity errors. A malformed
    /// state fails the complete listing instead of being silently skipped.
    pub async fn list_content_uploads(&self) -> Result<Vec<ContentUploadInfo>> {
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        self.list_upload_states().await.map(|states| {
            states
                .into_iter()
                .map(UploadSessionState::maintenance_info)
                .collect()
        })
    }

    /// Removes open or aborting uploads whose last durable update precedes
    /// `inactive_before_unix_ms`.
    ///
    /// Each candidate is locked and reread before cleanup. A concurrent append,
    /// seal, resume, or abort therefore either updates the timestamp or changes
    /// lifecycle and prevents stale cleanup. Successful cleanup deletes chunks,
    /// releases the exact upload reservation, and removes the state object.
    /// Sealing and sealed states are never discarded by this method.
    ///
    /// # Parameters
    ///
    /// - `inactive_before_unix_ms`: exclusive Unix-millisecond cutoff. A state
    ///   updated exactly at the cutoff is retained.
    ///
    /// # Errors
    ///
    /// Returns read-only, storage, quota-accounting, decoding, or cleanup
    /// errors. The pass is idempotent; retrying continues from durable state.
    pub async fn reap_inactive_content_uploads(
        &self,
        inactive_before_unix_ms: u64,
    ) -> Result<ContentUploadMaintenanceReport> {
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        // Tombstones are permanent. Revisit them on every maintenance pass so
        // an eventually consistent listing cannot turn a missed first cleanup
        // into a permanent orphan.
        self.cleanup_retired_upload_chunks().await?;
        let candidates = self.list_upload_states().await?;
        let mut report = ContentUploadMaintenanceReport::default();
        for candidate in candidates {
            report.scanned = report.scanned.saturating_add(1);
            if candidate.updated_at_unix_ms() >= inactive_before_unix_ms {
                continue;
            }

            let upload_id = candidate.upload_id();
            let _upload = self.lock_content_upload(upload_id).await;
            let current = match self.require_upload_state(upload_id).await {
                Ok(state) => state,
                Err(Error::ContentUploadNotFound { .. }) => continue,
                Err(error) => return Err(error),
            };
            let current_info = current.maintenance_info();
            if upload_is_eligible(
                current_info.state(),
                current_info.updated_at_unix_ms(),
                inactive_before_unix_ms,
                UploadMaintenanceOperation::ReapInactive,
            ) {
                self.discard_open_upload(&current).await?;
                report.aborted = report.aborted.saturating_add(1);
            }
        }
        Ok(report)
    }

    /// Removes sealed upload state older than an exclusive cutoff.
    ///
    /// This replaces the full upload-idempotency record with a permanent,
    /// lightweight upload-id tombstone. The immutable content
    /// descriptor, chunks selected by that descriptor, attachment token, and
    /// quota accounting remain unchanged. The tombstone prevents the public
    /// `UploadId` from ever being rebound to the same physical chunk namespace.
    /// After pruning, retrying begin, seal, resume, or abort by the old identity
    /// returns [`Error::ContentUploadSealed`].
    ///
    /// Sealing records are retained because they may still need crash recovery.
    /// # Errors
    ///
    /// Returns read-only, storage, listing, decoding, or write errors. Every
    /// successful retirement is final even if a later candidate fails;
    /// retrying the same cutoff is safe.
    pub async fn prune_sealed_content_uploads(
        &self,
        sealed_before_unix_ms: u64,
    ) -> Result<ContentUploadMaintenanceReport> {
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        // Sealed tombstones retain canonical chunks but no longer need any
        // revisioned partial; aborted tombstones own no live chunks at all.
        self.cleanup_retired_upload_chunks().await?;
        let candidates = self.list_upload_states().await?;
        let mut report = ContentUploadMaintenanceReport::default();
        for candidate in candidates {
            report.scanned = report.scanned.saturating_add(1);
            if candidate.updated_at_unix_ms() >= sealed_before_unix_ms {
                continue;
            }

            let upload_id = candidate.upload_id();
            let _upload = self.lock_content_upload(upload_id).await;
            let current = match self.require_upload_state(upload_id).await {
                Ok(state) => state,
                Err(Error::ContentUploadNotFound { .. }) => continue,
                Err(error) => return Err(error),
            };
            let current = current.maintenance_info();
            if upload_is_eligible(
                current.state(),
                current.updated_at_unix_ms(),
                sealed_before_unix_ms,
                UploadMaintenanceOperation::PruneSealed,
            ) {
                self.retire_upload_id(upload_id, UploadIdRetirement::Sealed)
                    .await?;
                report.pruned_sealed = report.pruned_sealed.saturating_add(1);
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::{ContentUploadState, UploadMaintenanceOperation, upload_is_eligible};

    #[test]
    fn upload_maintenance_eligibility_is_a_complete_state_boundary_table() {
        use ContentUploadState::{Aborting, Open, Sealed, Sealing};
        use UploadMaintenanceOperation::{PruneSealed, ReapInactive};

        for state in [Open, Sealing, Sealed, Aborting] {
            assert!(!upload_is_eligible(state, 100, 100, ReapInactive));
            assert!(!upload_is_eligible(state, 100, 100, PruneSealed));
            assert!(!upload_is_eligible(state, 101, 100, ReapInactive));
            assert!(!upload_is_eligible(state, 101, 100, PruneSealed));
        }

        assert!(upload_is_eligible(Open, 99, 100, ReapInactive));
        assert!(upload_is_eligible(Aborting, 99, 100, ReapInactive));
        assert!(!upload_is_eligible(Sealing, 99, 100, ReapInactive));
        assert!(!upload_is_eligible(Sealed, 99, 100, ReapInactive));

        assert!(upload_is_eligible(Sealed, 99, 100, PruneSealed));
        assert!(!upload_is_eligible(Open, 99, 100, PruneSealed));
        assert!(!upload_is_eligible(Sealing, 99, 100, PruneSealed));
        assert!(!upload_is_eligible(Aborting, 99, 100, PruneSealed));
    }
}
