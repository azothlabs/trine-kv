use crate::{
    db::Db,
    error::Result,
    options::WriteOptions,
    types::{CommitInfo, KeyRange, Sequence, Value},
    write_batch::WriteBatch,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionOptions {
    pub write_options: WriteOptions,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    db: Db,
    read_sequence: Sequence,
    options: TransactionOptions,
    writes: WriteBatch,
    point_reads: Vec<ReadKey>,
    range_reads: Vec<ReadRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadKey {
    pub(crate) keyspace: String,
    pub(crate) key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadRange {
    pub(crate) keyspace: String,
    pub(crate) range: KeyRange,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TransactionReadSet {
    pub(crate) point_reads: Vec<ReadKey>,
    pub(crate) range_reads: Vec<ReadRange>,
}

impl Transaction {
    #[must_use]
    pub(crate) fn new(db: Db, read_sequence: Sequence, options: TransactionOptions) -> Self {
        Self {
            db,
            read_sequence,
            options,
            writes: WriteBatch::new(),
            point_reads: Vec::new(),
            range_reads: Vec::new(),
        }
    }

    #[must_use]
    pub const fn read_sequence(&self) -> Sequence {
        self.read_sequence
    }

    #[must_use]
    pub const fn options(&self) -> TransactionOptions {
        self.options
    }

    pub fn get(&mut self, keyspace: impl Into<String>, key: &[u8]) -> Result<Option<Value>> {
        let keyspace = keyspace.into();
        let value = self.db.get_at(&keyspace, key, self.read_sequence)?;
        // Record the exact user key read at the transaction's read sequence.
        // Commit validation rejects the transaction if a later committed point
        // write, point delete, or covering range delete touched it.
        self.point_reads.push(ReadKey {
            keyspace,
            key: key.to_vec(),
        });

        Ok(value)
    }

    pub fn read_range(&mut self, keyspace: impl Into<String>, range: KeyRange) -> Result<()> {
        self.db.ensure_open()?;
        let keyspace = keyspace.into();
        let iter = self.db.range_at(
            &keyspace,
            &range,
            self.read_sequence,
            crate::Direction::Forward,
        )?;
        // The transaction API records a range that was actually read at the
        // transaction sequence. Consume the cursor here so table/blob read
        // errors are returned before the read set is accepted.
        for item in iter {
            item?;
        }
        // Range reads conflict with any later committed point mutation inside
        // the range, plus any later range tombstone that overlaps it.
        self.range_reads.push(ReadRange { keyspace, range });

        Ok(())
    }

    pub fn insert(
        &mut self,
        keyspace: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) {
        self.writes.insert(keyspace, key, value);
    }

    pub fn remove(&mut self, keyspace: impl Into<String>, key: impl Into<Vec<u8>>) {
        self.writes.remove(keyspace, key);
    }

    pub fn remove_range(&mut self, keyspace: impl Into<String>, range: KeyRange) {
        self.writes.remove_range(keyspace, range);
    }

    pub fn commit(self) -> Result<CommitInfo> {
        let read_set = TransactionReadSet {
            point_reads: self.point_reads,
            range_reads: self.range_reads,
        };

        self.db.commit_transaction(
            self.read_sequence,
            read_set,
            self.writes,
            self.options.write_options,
        )
    }
}
