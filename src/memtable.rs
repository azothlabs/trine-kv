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
