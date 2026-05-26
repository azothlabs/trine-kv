use std::{
    collections::BTreeMap,
    sync::{LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard},
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
}

impl Memtable {
    pub(crate) fn read_entries(
        &self,
    ) -> LockResult<RwLockReadGuard<'_, BTreeMap<InternalKey, Option<ValueRef>>>> {
        self.entries.read()
    }

    pub(crate) fn write_entries(
        &self,
    ) -> LockResult<RwLockWriteGuard<'_, BTreeMap<InternalKey, Option<ValueRef>>>> {
        self.entries.write()
    }
}
