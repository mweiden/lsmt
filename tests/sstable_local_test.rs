use lsmt::{sstable::SsTable, storage::local::LocalStorage};

#[tokio::test]
async fn sstable_local_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries = vec![("k".to_string(), b"v".to_vec())];
    let table = SsTable::create("data.tbl", &entries, &storage).await.unwrap();
    let res = table.get("k", &storage).await.unwrap();
    assert_eq!(res, Some(b"v".to_vec()));
}
