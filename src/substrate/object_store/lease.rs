use std::{io, sync::Arc};

use crate::{
    error::{Error, Result},
    object_store::{ETag, ObjectClient, Precondition, PutIf, canonical_object_key},
    types::Sequence,
    wal,
};

use super::{
    lane::{ObjectWalAccept, object_wal_group_frame_bytes},
    lease_state::{
        LeaseOwnerObservation, LeaseStatePublish, ObjectLeaseState, ObservedLeaseState,
        current_epoch_millis, encode_lease_state, object_lease_deadline_ms, read_lease_state,
    },
    wal_chain::{
        encode_object_wal_segment, object_wal_segment_identity, put_immutable_object,
        read_object_wal_chain,
    },
};

pub(crate) struct ObjectWriterLease {
    pub(super) client: Arc<dyn ObjectClient>,
    key: String,
    pub(super) etag: ETag,
    pub(super) state: ObjectLeaseState,
}

impl std::fmt::Debug for ObjectWriterLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectWriterLease")
            .field("key", &self.key)
            .field("epoch", &self.state.epoch)
            .field("committed_sequence", &self.state.committed_sequence)
            .field("lease_expires_at_ms", &self.state.lease_expires_at_ms)
            .finish_non_exhaustive()
    }
}

impl ObjectWriterLease {
    /// Acquire the lease by creating it or by taking over an expired owner. The
    /// returned lease carries a higher fencing epoch than the prior owner.
    pub(crate) async fn acquire(
        client: Arc<dyn ObjectClient>,
        key: impl Into<String>,
    ) -> Result<Self> {
        let key = key.into();
        loop {
            let now_ms = current_epoch_millis()?;
            let lease_expires_at_ms = object_lease_deadline_ms(now_ms);
            let mut owner_id = [0_u8; 16];
            getrandom::fill(&mut owner_id).map_err(|error| {
                Error::Io(io::Error::other(format!(
                    "object-store writer owner randomness failed: {error}"
                )))
            })?;
            let (next_state, precondition) = match read_lease_state(&client, &key).await? {
                None => (
                    ObjectLeaseState {
                        epoch: 1,
                        owner_id,
                        committed_sequence: Sequence::ZERO,
                        current_wal_key: None,
                        lease_expires_at_ms,
                    },
                    Precondition::IfNoneMatch,
                ),
                Some(meta) => {
                    if !meta.state.is_expired_at(now_ms) {
                        return Err(Error::lease_unavailable(format!(
                            "{key} is held until {}",
                            meta.state.lease_expires_at_ms
                        )));
                    }
                    let mut state = meta.state.clone();
                    state.epoch = state
                        .epoch
                        .checked_add(1)
                        .ok_or_else(|| Error::Corruption {
                            message: "object-store writer epoch overflow".to_owned(),
                        })?;
                    state.owner_id = owner_id;
                    state.lease_expires_at_ms = lease_expires_at_ms;
                    (state, Precondition::IfMatch(meta.etag))
                }
            };
            let publish = client
                .put_if(&key, encode_lease_state(next_state.clone())?, precondition)
                .await;
            match publish {
                Ok(PutIf::Stored { etag }) => {
                    return Ok(Self {
                        client,
                        key,
                        etag,
                        state: next_state,
                    });
                }
                // Lost the CAS to a concurrent acquirer; re-read and try again.
                Ok(PutIf::PreconditionFailed { .. }) => {}
                Err(error) => {
                    if let Ok(Some(current)) = read_lease_state(&client, &key).await
                        && current.state == next_state
                    {
                        return Ok(Self {
                            client,
                            key,
                            etag: current.etag,
                            state: current.state,
                        });
                    }
                    return Err(error);
                }
            }
        }
    }

    /// The fencing epoch this lease acquired.
    #[cfg(test)]
    pub(crate) fn epoch(&self) -> u64 {
        self.state.epoch
    }

    #[cfg(test)]
    pub(crate) fn committed_sequence(&self) -> Sequence {
        self.state.committed_sequence
    }

    pub(crate) fn lease_state(&self) -> ObjectLeaseState {
        self.state.clone()
    }

    pub(crate) async fn read_current(
        client: Arc<dyn ObjectClient>,
        key: impl Into<String>,
    ) -> Result<Option<ObjectLeaseState>> {
        read_lease_state(&client, &key.into())
            .await
            .map(|state| state.map(|state| state.state))
    }

    pub(super) async fn publish_commit_batch(
        &mut self,
        db_path: &std::path::Path,
        accepts: &[ObjectWalAccept],
    ) -> Result<()> {
        let Some(last) = accepts.last() else {
            return Ok(());
        };
        let total_bytes = object_wal_group_frame_bytes(self.state.committed_sequence, accepts)?;
        let mut frames = Vec::with_capacity(total_bytes);
        let mut expected = self
            .state
            .committed_sequence
            .checked_next()
            .ok_or_else(|| Error::Corruption {
                message: "object WAL cannot advance past u64::MAX".to_owned(),
            })?;
        for accept in accepts {
            while expected < accept.sequence {
                frames.extend_from_slice(&wal::encode_batch_frame(expected, &[])?);
                expected = expected.checked_next().ok_or_else(|| Error::Corruption {
                    message: "object WAL skipped sequence overflow".to_owned(),
                })?;
            }
            frames.extend_from_slice(&accept.frame);
            if expected != accept.sequence {
                return Err(Error::Corruption {
                    message: format!(
                        "object WAL expected sequence {}, got {}",
                        expected.get(),
                        accept.sequence.get()
                    ),
                });
            }
            if accept.sequence != last.sequence {
                expected = accept
                    .sequence
                    .get()
                    .checked_add(1)
                    .map(Sequence::new)
                    .ok_or_else(|| Error::Corruption {
                        message: "object WAL group sequence overflow".to_owned(),
                    })?;
            }
        }
        let segment = encode_object_wal_segment(self.state.current_wal_key.as_deref(), &frames)?;
        let identity = object_wal_segment_identity(&segment);
        let wal_key = canonical_object_key(&wal::object_wal_commit_path(
            db_path,
            self.state.epoch,
            last.sequence,
            &identity,
        ))?;
        put_immutable_object(&self.client, &wal_key, Arc::from(segment)).await?;
        self.publish_commit_head(last.sequence, wal_key).await
    }

    pub(super) async fn refresh_current(&mut self) -> Result<()> {
        let Some(current) = read_lease_state(&self.client, &self.key).await? else {
            return Err(Error::Fenced {
                held_epoch: self.state.epoch,
                current_epoch: 0,
            });
        };
        self.accept_observed(current)
    }

    fn accept_observed(&mut self, current: ObservedLeaseState) -> Result<()> {
        match self.state.observe_owner(&current.state) {
            LeaseOwnerObservation::CurrentOwner => {
                self.etag = current.etag;
                self.state = current.state;
                Ok(())
            }
            LeaseOwnerObservation::Fenced { current_epoch } => Err(Error::Fenced {
                held_epoch: self.state.epoch,
                current_epoch,
            }),
            LeaseOwnerObservation::EpochRegression { current_epoch } => Err(Error::Corruption {
                message: format!(
                    "writer lease {} moved backward from epoch {} to {current_epoch}",
                    self.key, self.state.epoch
                ),
            }),
        }
    }

    pub(super) async fn renew(&mut self) -> Result<()> {
        self.refresh_current().await?;
        let next = self
            .state
            .plan_renew(object_lease_deadline_ms(current_epoch_millis()?));
        let publish = self
            .client
            .put_if(
                &self.key,
                encode_lease_state(next.clone())?,
                Precondition::IfMatch(self.etag.clone()),
            )
            .await;
        match publish {
            Ok(PutIf::Stored { etag }) => {
                self.etag = etag;
                self.state = next;
                Ok(())
            }
            Ok(PutIf::PreconditionFailed { .. }) => {
                let Some(current) = read_lease_state(&self.client, &self.key).await? else {
                    return Err(Error::Fenced {
                        held_epoch: self.state.epoch,
                        current_epoch: 0,
                    });
                };
                self.accept_observed(current)
            }
            Err(error) => {
                if let Ok(Some(current)) = read_lease_state(&self.client, &self.key).await
                    && current.state == next
                {
                    self.etag = current.etag;
                    self.state = current.state;
                    return Ok(());
                }
                Err(error)
            }
        }
    }

    pub(super) async fn release(&mut self) -> Result<()> {
        loop {
            let Some(current) = read_lease_state(&self.client, &self.key).await? else {
                return Ok(());
            };
            self.accept_observed(current)?;
            let next = self.state.plan_release();
            let publish = self
                .client
                .put_if(
                    &self.key,
                    encode_lease_state(next.clone())?,
                    Precondition::IfMatch(self.etag.clone()),
                )
                .await;
            match publish {
                Ok(PutIf::Stored { etag }) => {
                    self.etag = etag;
                    self.state = next;
                    return Ok(());
                }
                Ok(PutIf::PreconditionFailed { .. }) => {}
                Err(error) => {
                    if let Ok(Some(current)) = read_lease_state(&self.client, &self.key).await
                        && current.state == next
                    {
                        self.etag = current.etag;
                        self.state = current.state;
                        return Ok(());
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn publish_commit_head(&mut self, sequence: Sequence, wal_key: String) -> Result<()> {
        loop {
            let next = match self.state.plan_commit_head(
                sequence,
                &wal_key,
                object_lease_deadline_ms(current_epoch_millis()?),
            )? {
                LeaseStatePublish::Publish(next) => next,
                LeaseStatePublish::AlreadyApplied => return Ok(()),
            };
            let publish = self
                .client
                .put_if(
                    &self.key,
                    encode_lease_state(next.clone())?,
                    Precondition::IfMatch(self.etag.clone()),
                )
                .await;
            match publish {
                Ok(PutIf::Stored { etag }) => {
                    self.etag = etag;
                    self.state = next;
                    return Ok(());
                }
                Ok(PutIf::PreconditionFailed { .. }) => {
                    let Some(current) = read_lease_state(&self.client, &self.key).await? else {
                        return Err(Error::Fenced {
                            held_epoch: self.state.epoch,
                            current_epoch: 0,
                        });
                    };
                    self.accept_observed(current)?;
                }
                Err(error) => {
                    if let Ok(Some(current)) = read_lease_state(&self.client, &self.key).await
                        && current.state.epoch == next.epoch
                        && current.state.owner_id == next.owner_id
                        && current.state.committed_sequence >= sequence
                        && current.state.current_wal_key.as_deref() == Some(wal_key.as_str())
                    {
                        self.etag = current.etag;
                        self.state = current.state;
                        return Ok(());
                    }
                    return Err(error);
                }
            }
        }
    }

    pub(super) async fn rewrite_segment_after_replay_floor(
        &mut self,
        db_path: &std::path::Path,
        replay_floor: Sequence,
    ) -> Result<Vec<String>> {
        self.refresh_current().await?;
        loop {
            if self.state.current_wal_key.is_none() {
                return Ok(Vec::new());
            }
            let (batches, delete_keys) =
                read_object_wal_chain(&self.client, db_path, &self.state, replay_floor).await?;
            let current_wal_key = if batches.is_empty() {
                None
            } else {
                let rewritten = wal::encode_batches_after(&batches, replay_floor)?;
                let last_sequence = batches.last().map_or(replay_floor, |batch| batch.sequence);
                let segment = encode_object_wal_segment(None, &rewritten)?;
                let identity = object_wal_segment_identity(&segment);
                let next_key = canonical_object_key(&wal::object_wal_rewrite_path(
                    db_path,
                    self.state.epoch,
                    last_sequence,
                    &identity,
                ))?;
                put_immutable_object(&self.client, &next_key, Arc::from(segment)).await?;
                Some(next_key)
            };
            let next = self.state.plan_rewrite_head(
                current_wal_key,
                object_lease_deadline_ms(current_epoch_millis()?),
            );
            let publish = self
                .client
                .put_if(
                    &self.key,
                    encode_lease_state(next.clone())?,
                    Precondition::IfMatch(self.etag.clone()),
                )
                .await;
            match publish {
                Ok(PutIf::Stored { etag }) => {
                    self.etag = etag;
                    self.state = next;
                    return Ok(delete_keys);
                }
                Ok(PutIf::PreconditionFailed { .. }) => {
                    let Some(current) = read_lease_state(&self.client, &self.key).await? else {
                        return Err(Error::Fenced {
                            held_epoch: self.state.epoch,
                            current_epoch: 0,
                        });
                    };
                    self.accept_observed(current)?;
                }
                Err(error) => {
                    if let Ok(Some(current)) = read_lease_state(&self.client, &self.key).await
                        && current.state == next
                    {
                        self.etag = current.etag;
                        self.state = current.state;
                        return Ok(delete_keys);
                    }
                    return Err(error);
                }
            }
        }
    }
}
