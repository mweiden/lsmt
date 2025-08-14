pub mod bloom;
pub mod memtable;
pub mod query;
pub mod sstable;
pub mod storage;
pub mod wal;
pub mod zonemap;

pub use query::SqlEngine;

/// Core database type combining an in-memory memtable with a persistent
/// storage layer.
pub struct Database<S: storage::Storage> {
    storage: S,
    memtable: memtable::MemTable,
    sstables: tokio::sync::RwLock<Vec<sstable::SsTable>>,
    max_memtable_size: usize,
    next_id: std::sync::atomic::AtomicUsize,
}

impl<S: storage::Storage> Database<S> {
    /// Create a new database instance backed by the provided storage
    /// implementation.
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            memtable: memtable::MemTable::new(),
            sstables: tokio::sync::RwLock::new(Vec::new()),
            // default threshold before automatically flushing to disk
            max_memtable_size: 1024,
            next_id: std::sync::atomic::AtomicUsize::new(0),
        }
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
        if self.memtable.len().await >= self.max_memtable_size {
            // best-effort flush; ignore errors for now
            let _ = self.flush().await;
        }
    }

    /// Retrieve the value associated with `key`, if it exists.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(val) = self.memtable.get(key).await {
            return Some(val);
        }
        let tables = self.sstables.read().await;
        for table in tables.iter().rev() {
            if let Ok(Some(v)) = table.get(key, &self.storage).await {
                return Some(v);
            }
        }
        None
    }

    /// Delete a key from the database.
    pub async fn delete(&self, key: &str) {
        self.memtable.delete(key).await;
    }

    /// Return all key/value pairs currently stored.
    pub async fn scan(&self) -> Vec<(String, Vec<u8>)> {
        self.memtable.scan().await
    }

    /// Remove all data from the in-memory table.
    pub async fn clear(&self) {
        self.memtable.clear().await;
    }

    /// Insert a key/value pair into the provided namespace.
    pub async fn insert_ns(&self, ns: &str, key: String, value: Vec<u8>) {
        let namespaced = format!("{}:{}", ns, key);
        self.insert(namespaced, value).await;
    }

    /// Retrieve a value from the given namespace.
    pub async fn get_ns(&self, ns: &str, key: &str) -> Option<Vec<u8>> {
        let namespaced = format!("{}:{}", ns, key);
        self.get(&namespaced).await
    }

    /// Delete a key within the specified namespace.
    pub async fn delete_ns(&self, ns: &str, key: &str) {
        let namespaced = format!("{}:{}", ns, key);
        self.memtable.delete(&namespaced).await;
    }

    /// Scan all key/value pairs for a namespace, stripping the prefix.
    pub async fn scan_ns(&self, ns: &str) -> Vec<(String, Vec<u8>)> {
        let prefix = format!("{}:", ns);
        self.memtable
            .scan()
            .await
            .into_iter()
            .filter_map(|(k, v)| k.strip_prefix(&prefix).map(|rest| (rest.to_string(), v)))
            .collect()
    }

    /// Clear all data for a namespace.
    pub async fn clear_ns(&self, ns: &str) {
        let prefix = format!("{}:", ns);
        self.memtable.delete_prefix(&prefix).await;
    }

    /// Manually flush the current memtable to an on-disk [`SsTable`].
    pub async fn flush(&self) -> Result<(), storage::StorageError> {
        let entries = self.memtable.scan().await;
        if entries.is_empty() {
            return Ok(());
        }
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = format!("sstable_{id}.tbl");
        let table = sstable::SsTable::create(&path, &entries, &self.storage).await?;
        self.memtable.clear().await;
        self.sstables.write().await.push(table);
        Ok(())
    }
}
