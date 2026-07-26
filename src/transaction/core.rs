use std::collections::BTreeMap;

use crate::{
    types::{KeyRange, Sequence},
    write_batch::WriteBatch,
};

use super::TransactionOptions;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadKey {
    pub(crate) bucket: String,
    pub(crate) key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadRange {
    pub(crate) bucket: String,
    pub(crate) range: KeyRange,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TransactionReadSet {
    pub(crate) point_reads: Vec<ReadKey>,
    pub(crate) range_reads: Vec<ReadRange>,
}

/// Owns all storage-independent state transitions for one transaction.
///
/// Database reads and commit execution stay in [`super::Transaction`]. This
/// state object only records what was observed, what will be written, and the
/// extension claims that must remain stable through commit.
#[derive(Debug, Clone)]
pub(super) struct TransactionCore {
    read_sequence: Sequence,
    options: TransactionOptions,
    writes: WriteBatch,
    read_set: TransactionReadSet,
    extension_claims: BTreeMap<Vec<u8>, [u8; 16]>,
}

impl TransactionCore {
    pub(super) fn new(read_sequence: Sequence, options: TransactionOptions) -> Self {
        Self {
            read_sequence,
            options,
            writes: WriteBatch::new(),
            read_set: TransactionReadSet::default(),
            extension_claims: BTreeMap::new(),
        }
    }

    pub(super) const fn read_sequence(&self) -> Sequence {
        self.read_sequence
    }

    pub(super) const fn options(&self) -> TransactionOptions {
        self.options
    }

    pub(super) const fn writes_mut(&mut self) -> &mut WriteBatch {
        &mut self.writes
    }

    pub(super) fn record_point_read(&mut self, bucket: String, key: Vec<u8>) {
        self.read_set.point_reads.push(ReadKey { bucket, key });
    }

    pub(super) fn record_range_read(&mut self, bucket: String, range: KeyRange) {
        self.read_set.range_reads.push(ReadRange { bucket, range });
    }

    pub(super) fn extension_claim(&self, key: &[u8]) -> Option<[u8; 16]> {
        self.extension_claims.get(key).copied()
    }

    pub(super) fn record_extension_claim(&mut self, key: Vec<u8>, claim: [u8; 16]) {
        self.extension_claims.insert(key, claim);
    }

    pub(super) fn into_commit_parts(
        self,
    ) -> (Sequence, TransactionReadSet, WriteBatch, TransactionOptions) {
        (self.read_sequence, self.read_set, self.writes, self.options)
    }
}
