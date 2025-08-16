use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Schema information for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub partition_keys: Vec<String>,
    pub clustering_keys: Vec<String>,
    pub columns: Vec<String>,
}

impl TableSchema {
    /// Create a new [`TableSchema`].
    pub fn new(
        partition_keys: Vec<String>,
        clustering_keys: Vec<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            partition_keys,
            clustering_keys,
            columns,
        }
    }

    /// Return the ordered list of key columns (partition + clustering).
    pub fn key_columns(&self) -> Vec<String> {
        self.partition_keys
            .iter()
            .chain(self.clustering_keys.iter())
            .cloned()
            .collect()
    }
}

/// Serialize a row map into bytes.
pub fn encode_row(map: &BTreeMap<String, String>) -> Vec<u8> {
    serde_json::to_vec(map).unwrap_or_default()
}

/// Deserialize row bytes into a map. Missing or invalid data yields an empty map.
pub fn decode_row(data: &[u8]) -> BTreeMap<String, String> {
    serde_json::from_slice(data).unwrap_or_default()
}
