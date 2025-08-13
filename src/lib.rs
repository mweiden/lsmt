pub mod storage;
pub mod wal;
pub mod memtable;
pub mod sstable;
pub mod bloom;
pub mod zonemap;
pub mod query;

pub use query::SqlEngine;

pub struct Database<S: storage::Storage> {
    storage: S,
    memtable: memtable::MemTable,
}

impl<S: storage::Storage> Database<S> {
    pub fn new(storage: S) -> Self {
        Self { storage, memtable: memtable::MemTable::new() }
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn memtable(&self) -> &memtable::MemTable {
        &self.memtable
    }

    pub async fn insert(&self, key: String, value: Vec<u8>) {
        self.memtable.insert(key, value).await;
    }

    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.memtable.get(key).await
    }
}
