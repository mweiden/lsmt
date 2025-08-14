pub mod storage;
pub mod wal;
pub mod memtable;
pub mod sstable;
pub mod bloom;
pub mod zonemap;
pub mod query;

pub use query::SqlEngine;

/// Core database type combining an in-memory memtable with a persistent
/// storage layer.
pub struct Database<S: storage::Storage> {
    storage: S,
    memtable: memtable::MemTable,
}

impl<S: storage::Storage> Database<S> {
    /// Create a new database instance backed by the provided storage
    /// implementation.
    pub fn new(storage: S) -> Self {
        Self { storage, memtable: memtable::MemTable::new() }
    }

    /// Return a reference to the configured storage backend.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Return a reference to the in-memory memtable used for writes.
    pub fn memtable(&self) -> &memtable::MemTable {
        &self.memtable
    }

    /// Insert a key/value pair into the database.
    pub async fn insert(&self, key: String, value: Vec<u8>) {
        self.memtable.insert(key, value).await;
    }

    /// Retrieve the value associated with `key`, if it exists.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.memtable.get(key).await
    }
}
