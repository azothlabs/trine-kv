use std::cmp::Ordering;

use crate::types::Sequence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueKind {
    Put,
    PointDelete,
    RangeDelete,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InternalKey {
    user_key: Vec<u8>,
    sequence: Sequence,
    kind: ValueKind,
    batch_index: u32,
}

impl InternalKey {
    #[must_use]
    pub fn new(
        user_key: impl Into<Vec<u8>>,
        sequence: Sequence,
        kind: ValueKind,
        batch_index: u32,
    ) -> Self {
        Self {
            user_key: user_key.into(),
            sequence,
            kind,
            batch_index,
        }
    }

    #[must_use]
    pub fn user_key(&self) -> &[u8] {
        &self.user_key
    }

    #[must_use]
    pub(crate) const fn sequence(&self) -> Sequence {
        self.sequence
    }

    #[must_use]
    pub const fn kind(&self) -> ValueKind {
        self.kind
    }

    #[must_use]
    pub const fn batch_index(&self) -> u32 {
        self.batch_index
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| other.batch_index.cmp(&self.batch_index))
            .then_with(|| self.kind.cmp(&other.kind))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn first_internal_key_for_user(user_key: &[u8]) -> InternalKey {
    InternalKey::new(
        user_key.to_vec(),
        Sequence::new(u64::MAX),
        ValueKind::Put,
        u32::MAX,
    )
}

pub(crate) fn last_internal_key_for_user(user_key: &[u8]) -> InternalKey {
    InternalKey::new(user_key.to_vec(), Sequence::ZERO, ValueKind::RangeDelete, 0)
}

#[cfg(test)]
mod tests {
    use super::{InternalKey, ValueKind};
    use crate::types::Sequence;

    #[test]
    fn internal_keys_sort_user_key_ascending_then_sequence_descending() {
        let mut keys = [
            InternalKey::new("b", Sequence::new(1), ValueKind::Put, 0),
            InternalKey::new("a", Sequence::new(1), ValueKind::Put, 0),
            InternalKey::new("a", Sequence::new(3), ValueKind::Put, 0),
            InternalKey::new("a", Sequence::new(2), ValueKind::Put, 0),
        ];

        keys.sort();

        let ordered = keys.map(|key| (key.user_key().to_vec(), key.sequence().get()));
        assert_eq!(
            ordered,
            [
                (b"a".to_vec(), 3),
                (b"a".to_vec(), 2),
                (b"a".to_vec(), 1),
                (b"b".to_vec(), 1),
            ]
        );
    }

    #[test]
    fn batch_index_sorts_later_batch_operations_first() {
        let mut keys = [
            InternalKey::new("a", Sequence::new(1), ValueKind::Put, 0),
            InternalKey::new("a", Sequence::new(1), ValueKind::PointDelete, 1),
        ];

        keys.sort();

        assert_eq!(keys[0].batch_index(), 1);
        assert_eq!(keys[0].kind(), ValueKind::PointDelete);
    }
}
