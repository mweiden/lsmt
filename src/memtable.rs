use std::collections::BTreeMap;
use tokio::sync::RwLock;

pub struct MemTable {
    data: RwLock<BTreeMap<String, Vec<u8>>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn insert(&self, key: String, value: Vec<u8>) {
        self.data.write().await.insert(key, value);
    }

    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.data.read().await.get(key).cloned()
    }

    pub async fn delete(&self, key: &str) {
        self.data.write().await.remove(key);
    }

    pub async fn scan(&self) -> Vec<(String, Vec<u8>)> {
        self.data
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub async fn clear(&self) {
        self.data.write().await.clear();
    }

    pub async fn delete_prefix(&self, prefix: &str) {
        let mut data = self.data.write().await;
        let keys: Vec<String> = data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        for k in keys {
            data.remove(&k);
        }
    }
}
