use cass::{sstable::SsTable, storage::local::LocalStorage};

#[tokio::test]
async fn sstable_local_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries = vec![("k".to_string(), b"v".to_vec())];
    let table = SsTable::create("data.tbl", &entries, &storage)
        .await
        .unwrap();
    let loaded = SsTable::load("data.tbl", &storage).await.unwrap();
    assert_eq!(loaded.bloom.to_bytes(), table.bloom.to_bytes());
    assert_eq!(loaded.zone_map.min, table.zone_map.min);
    assert_eq!(loaded.zone_map.max, table.zone_map.max);
    let res = loaded.get("k", &storage).await.unwrap();
    assert_eq!(res, Some(b"v".to_vec()));
}
