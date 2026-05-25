use crate::{
    db::Db,
    error::{Error, Result},
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
}

impl Transaction {
    #[must_use]
    pub(crate) fn new(db: Db, read_sequence: Sequence, options: TransactionOptions) -> Self {
        Self {
            db,
            read_sequence,
            options,
            writes: WriteBatch::new(),
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

    pub fn get(&mut self, _keyspace: impl Into<String>, _key: &[u8]) -> Result<Option<Value>> {
        self.db.ensure_open()?;
        Err(Error::unsupported(
            "transaction reads are not implemented yet",
        ))
    }

    pub fn read_range(&mut self, _keyspace: impl Into<String>, _range: KeyRange) -> Result<()> {
        self.db.ensure_open()?;
        Err(Error::unsupported(
            "transaction range tracking is not implemented yet",
        ))
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

    pub fn commit(self) -> Result<CommitInfo> {
        self.db.ensure_open()?;
        Err(Error::unsupported(
            "optimistic transaction commit is not implemented yet",
        ))
    }
}
