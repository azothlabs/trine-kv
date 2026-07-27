//! Object-store durability: immutable WAL segments and the fencing writer lease.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    error::{Error, Result},
    object_store::canonical_object_key,
    options::DurabilityMode,
    types::Sequence,
    wal::{self, WalFrontDoorStats},
    write_batch::BatchOperation,
};

use lane::ObjectWalLane;
use lease_state::lock_poisoned_error;

#[cfg(not(target_os = "wasi"))]
pub(crate) use lane::ObjectWalWaiter;
pub(crate) use lease::ObjectWriterLease;
pub(crate) use lease_state::ObjectLeaseState;
pub(crate) use wal_chain::object_store_wal_batches_after_replay_floor;

#[cfg(test)]
use lane::{ObjectWalAccept, ObjectWalCommand, ObjectWalCompletion};
#[cfg(test)]
use lease_state::{LeaseOwnerObservation, LeaseStatePublish, encode_lease_state};
#[cfg(test)]
use wal_chain::{
    decode_object_wal_segment, encode_object_wal_segment, object_wal_segment_identity,
};

const OBJECT_WAL_QUEUE_CAPACITY: usize = 64;
const OBJECT_WAL_GROUP_COMMIT_DELAY: Duration = Duration::from_millis(5);
const OBJECT_LEASE_TTL: Duration = Duration::from_secs(30);
const OBJECT_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);
const OBJECT_LEASE_MAGIC: u32 = 0x5452_4c53;
const OBJECT_LEASE_VERSION: u16 = 3;
const OBJECT_LEASE_V3_HEADER_LEN: usize = 50;
const OBJECT_LEASE_MAX_BYTES: u64 = 64 * 1024;
const OBJECT_WAL_SEGMENT_MAGIC: &[u8; 8] = b"TRNOWAL1";
const OBJECT_WAL_SEGMENT_HEADER_LEN: usize = 12;
const OBJECT_WAL_MAX_SEGMENT_BYTES: usize = 128 * 1024 * 1024;
const OBJECT_WAL_MAX_GROUP_FRAME_BYTES: usize = OBJECT_WAL_MAX_SEGMENT_BYTES - 64 * 1024;
const OBJECT_WAL_MAX_CHAIN_SEGMENTS: usize = 16_384;
const OBJECT_WAL_MAX_REPLAY_BYTES: usize = 1024 * 1024 * 1024;

mod lane;
mod lease;
mod lease_state;
mod wal_chain;

#[cfg(feature = "fuzzing")]
mod fuzzing;
#[cfg(feature = "fuzzing")]
pub(crate) use fuzzing::fuzz_decode_object_control;

#[cfg(test)]
#[path = "../tests.rs"]
mod tests;

pub(crate) fn validate_object_lease_wal_key_capacity(db_path: &std::path::Path) -> Result<()> {
    let longest_key = canonical_object_key(&wal::object_wal_commit_path(
        db_path,
        u64::MAX,
        Sequence::new(u64::MAX),
        &"f".repeat(64),
    ))?;
    let encoded_len = OBJECT_LEASE_V3_HEADER_LEN
        .checked_add(longest_key.len())
        .ok_or_else(|| Error::invalid_options("object writer lease length overflow"))?;
    if u64::try_from(encoded_len).map_or(true, |encoded_len| encoded_len > OBJECT_LEASE_MAX_BYTES) {
        return Err(Error::invalid_options(format!(
            "object-store prefix produces writer lease length {encoded_len}, exceeding maximum {OBJECT_LEASE_MAX_BYTES}"
        )));
    }
    Ok(())
}

/// Object-storage durability: immutable WAL segments plus a lease object that
/// doubles as a fencing token and the published remote WAL head.
pub(crate) struct ObjectStoreSubstrate {
    wal_lane: ObjectWalLane,
    buffered: Mutex<Vec<(Sequence, Arc<[u8]>)>>,
    fencing_epoch: u64,
    records_accepted: std::sync::atomic::AtomicU64,
    bytes_accepted: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for ObjectStoreSubstrate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreSubstrate")
            .field("fencing_epoch", &self.fencing_epoch)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreSubstrate {
    pub(crate) fn new(lease: ObjectWriterLease, db_path: PathBuf) -> Result<Self> {
        let fencing_epoch = lease.state.epoch;
        Ok(Self {
            wal_lane: ObjectWalLane::spawn(lease, db_path)?,
            buffered: Mutex::new(Vec::new()),
            fencing_epoch,
            records_accepted: std::sync::atomic::AtomicU64::new(0),
            bytes_accepted: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// The fencing epoch of the held lease (stamped into manifest publishes so a
    /// stale writer is fenced out).
    pub(crate) fn fencing_epoch(&self) -> u64 {
        self.fencing_epoch
    }

    pub(super) async fn fence_mutation_async(&self) -> Result<()> {
        self.wal_lane.enqueue_persist()?.wait().await
    }

    pub(super) fn accept_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        let frame = wal::encode_batch_frame(sequence, operations)?;
        let bytes_accepted = frame.len() as u64;
        self.buffered
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL buffered commits"))?
            .push((sequence, frame.into()));
        self.records_accepted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_accepted
            .fetch_add(bytes_accepted, std::sync::atomic::Ordering::Relaxed);
        if durability == DurabilityMode::Buffered {
            Ok(())
        } else {
            self.flush_buffered()
        }
    }

    #[cfg(not(target_os = "wasi"))]
    pub(super) fn enqueue_commit(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<ObjectWalWaiter> {
        let commit_sequence = sequence;
        let frame = wal::encode_batch_frame(sequence, operations)?;
        let bytes_accepted = frame.len() as u64;
        let mut buffered = self
            .buffered
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL buffered commits"))?;
        buffered.push((sequence, frame.into()));
        if durability == DurabilityMode::Buffered {
            self.records_accepted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.bytes_accepted
                .fetch_add(bytes_accepted, std::sync::atomic::Ordering::Relaxed);
            return Ok(ObjectWalWaiter::ready());
        }

        buffered.sort_by_key(|(sequence, _)| *sequence);
        if buffered
            .last()
            .is_none_or(|(sequence, _)| *sequence != commit_sequence)
        {
            buffered.retain(|(sequence, _)| *sequence != commit_sequence);
            return Err(Error::Corruption {
                message:
                    "object WAL buffered sequence order would admit the current commit before an older commit"
                        .to_owned(),
            });
        }
        let mut completions = Vec::with_capacity(buffered.len());
        let mut enqueued = 0;
        for (sequence, frame) in buffered.iter() {
            match self.wal_lane.enqueue_commit(*sequence, Arc::clone(frame)) {
                Ok(completion) => {
                    completions.push(completion);
                    enqueued += 1;
                }
                Err(error) => {
                    buffered.drain(..enqueued);
                    if let Some(index) = buffered
                        .iter()
                        .position(|(buffered_sequence, _)| *buffered_sequence == commit_sequence)
                    {
                        buffered.remove(index);
                    }
                    return Err(error);
                }
            }
        }
        buffered.clear();
        self.records_accepted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_accepted
            .fetch_add(bytes_accepted, std::sync::atomic::Ordering::Relaxed);
        Ok(ObjectWalWaiter { completions })
    }

    pub(super) fn persist_wal(&self, durability: DurabilityMode) -> Result<()> {
        if durability == DurabilityMode::Buffered {
            return Ok(());
        }
        self.flush_buffered()?;
        self.wal_lane.persist()
    }

    pub(super) fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        self.flush_buffered()?;
        self.wal_lane.rewrite_after_replay_floor(replay_floor)
    }

    fn flush_buffered(&self) -> Result<()> {
        let mut buffered = self
            .buffered
            .lock()
            .map_err(|_| lock_poisoned_error("object WAL buffered commits"))?;
        buffered.sort_by_key(|(sequence, _)| *sequence);
        for (published, (sequence, frame)) in buffered.iter().enumerate() {
            if let Err(error) = self.wal_lane.accept_commit(*sequence, Arc::clone(frame)) {
                buffered.drain(..published);
                return Err(error);
            }
        }
        buffered.clear();
        Ok(())
    }

    pub(super) fn wal_stats(&self) -> WalFrontDoorStats {
        WalFrontDoorStats {
            shards: 1,
            open_shards: 1,
            queue_capacity: OBJECT_WAL_QUEUE_CAPACITY,
            records_accepted: self
                .records_accepted
                .load(std::sync::atomic::Ordering::Acquire),
            bytes_accepted: self
                .bytes_accepted
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }

    pub(super) fn release_writer_lease(&self) {
        let _ = self.wal_lane.release_writer_lease();
    }
}
