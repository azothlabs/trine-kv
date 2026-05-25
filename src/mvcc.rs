use crate::types::Sequence;

pub type SnapshotSequence = Sequence;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Visibility {
    read_sequence: SnapshotSequence,
}

impl Visibility {
    #[must_use]
    pub const fn new(read_sequence: SnapshotSequence) -> Self {
        Self { read_sequence }
    }

    #[must_use]
    pub const fn read_sequence(self) -> SnapshotSequence {
        self.read_sequence
    }

    #[must_use]
    pub const fn can_see(self, sequence: Sequence) -> bool {
        sequence.get() <= self.read_sequence.get()
    }
}
