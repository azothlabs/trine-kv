use crate::types::Sequence;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSet {
    pub last_sequence: Sequence,
    pub wal_replay_floor: Sequence,
}

impl VersionSet {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            last_sequence: Sequence::ZERO,
            wal_replay_floor: Sequence::ZERO,
        }
    }
}

impl Default for VersionSet {
    fn default() -> Self {
        Self::empty()
    }
}
