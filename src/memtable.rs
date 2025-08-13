use std::collections::BTreeMap;
use tokio::sync::RwLock;

pub struct MemTable {
    data: RwLock<BTreeMap<String, Vec<u8>>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self { data: RwLock::new(BTreeMap::new()) }
    }

    pub async fn insert(&self, key: String, value: Vec<u8>) {
        self.data.write().await.insert(key, value);
    }

    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.data.read().await.get(key).cloned()
    }
}
