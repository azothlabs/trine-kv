use crate::types::Sequence;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Visibility {
    read_sequence: Sequence,
}

impl Visibility {
    #[must_use]
    pub(crate) const fn new(read_sequence: Sequence) -> Self {
        Self { read_sequence }
    }

    #[must_use]
    pub(crate) const fn read_sequence(self) -> Sequence {
        self.read_sequence
    }

    #[must_use]
    pub(crate) const fn can_see(self, sequence: Sequence) -> bool {
        sequence.get() <= self.read_sequence.get()
    }
}
