use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::{LockResult, RwLock, RwLockReadGuard},
};

use crate::{blob::ValueRef, internal_key::InternalKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemtableState {
    Active,
    Immutable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemtableEntry {
    pub key: InternalKey,
    pub value: Option<ValueRef>,
}

#[derive(Debug, Default)]
pub(crate) struct Memtable {
    entries: RwLock<BTreeMap<InternalKey, Option<ValueRef>>>,
    estimated_bytes: AtomicU64,
}

impl Memtable {
    pub(crate) fn insert(&self, key: InternalKey, value: Option<ValueRef>) -> Result<(), ()> {
        let new_bytes = entry_bytes(&key, value.as_ref());
        let key_for_old_value = key.clone();
        let mut entries = self.entries.write().map_err(|_| ())?;
        let old_value = entries.insert(key, value);

        if let Some(old_value) = old_value {
            self.estimated_bytes.fetch_sub(
                entry_bytes(&key_for_old_value, old_value.as_ref()),
                Ordering::AcqRel,
            );
        }
        self.estimated_bytes.fetch_add(new_bytes, Ordering::AcqRel);

        Ok(())
    }

    pub(crate) fn is_empty(&self) -> Result<bool, ()> {
        self.entries
            .read()
            .map_err(|_| ())
            .map(|entries| entries.is_empty())
    }

    pub(crate) fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn read_entries(
        &self,
    ) -> LockResult<RwLockReadGuard<'_, BTreeMap<InternalKey, Option<ValueRef>>>> {
        self.entries.read()
    }

    #[cfg(test)]
    pub(crate) fn write_entries(
        &self,
    ) -> LockResult<std::sync::RwLockWriteGuard<'_, BTreeMap<InternalKey, Option<ValueRef>>>> {
        self.entries.write()
    }
}

fn entry_bytes(internal_key: &InternalKey, value: Option<&ValueRef>) -> u64 {
    let value_len = value.map_or(0, ValueRef::len);
    usize_to_u64_saturating(internal_key.user_key().len())
        .saturating_add(value_len)
        .saturating_add(16)
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        blob::ValueRef,
        internal_key::{InternalKey, ValueKind},
        types::Sequence,
    };

    use super::Memtable;

    #[test]
    fn estimated_bytes_tracks_insert_and_replace() {
        let memtable = Memtable::default();
        let key = InternalKey::new(b"k".to_vec(), Sequence::new(1), ValueKind::Put, 0);

        memtable
            .insert(key.clone(), Some(ValueRef::Inline(b"v1".to_vec())))
            .expect("insert first value");
        let first_bytes = memtable.estimated_bytes();
        assert!(first_bytes > 0);

        memtable
            .insert(key, Some(ValueRef::Inline(b"larger".to_vec())))
            .expect("replace value");
        let replaced_bytes = memtable.estimated_bytes();

        assert!(replaced_bytes > first_bytes);
        assert!(
            replaced_bytes < first_bytes.saturating_mul(2),
            "replacement should adjust the existing entry estimate"
        );
    }
}
