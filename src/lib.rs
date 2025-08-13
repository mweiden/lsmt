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
}

impl<S: storage::Storage> Database<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}
