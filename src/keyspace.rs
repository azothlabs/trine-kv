use crate::{
    db::Db,
    error::{Error, Result},
    iterator::{Direction, Iter},
    options::{KeyspaceOptions, WriteOptions},
    snapshot::Snapshot,
    types::{KeyRange, Value},
    write_batch::WriteBatch,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyspaceName(String);

impl KeyspaceName {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for KeyspaceName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for KeyspaceName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone)]
pub struct Keyspace {
    db: Db,
    name: KeyspaceName,
    options: KeyspaceOptions,
}

impl Keyspace {
    pub(crate) const fn new(db: Db, name: KeyspaceName, options: KeyspaceOptions) -> Self {
        Self { db, name, options }
    }

    #[must_use]
    pub fn name(&self) -> &KeyspaceName {
        &self.name
    }

    #[must_use]
    pub fn options(&self) -> &KeyspaceOptions {
        &self.options
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.db
            .get_at(self.name.as_str(), key, self.db.last_committed_sequence())
    }

    pub fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.db
            .get_at(self.name.as_str(), key, snapshot.read_sequence())
    }

    pub fn insert(&self, key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.insert(self.name.as_str(), key, value);
        self.db.write(batch, WriteOptions::default()).map(|_| ())
    }

    pub fn remove(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.remove(self.name.as_str(), key);
        self.db.write(batch, WriteOptions::default()).map(|_| ())
    }

    pub fn range(&self, _range: KeyRange) -> Result<Iter> {
        self.db.ensure_open()?;
        Err(Error::unsupported("range iteration is not implemented yet"))
    }

    pub fn prefix(&self, _prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        self.db.ensure_open()?;
        Err(Error::unsupported(
            "prefix iteration is not implemented yet",
        ))
    }

    #[must_use]
    pub fn empty_iter(direction: Direction) -> Iter {
        Iter::empty(direction)
    }
}
