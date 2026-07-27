use crate::{types::Sequence, wal};

use super::{lease_state, wal_chain::decode_object_wal_segment};

pub(crate) fn fuzz_decode_object_control(bytes: &[u8]) {
    let _ = lease_state::decode_lease_state("fuzz/LOCK", bytes);
    if let Ok((_previous, frames)) = decode_object_wal_segment("fuzz/wal", bytes) {
        let _ = wal::decode_frames_after(frames, Sequence::ZERO);
    }
}
