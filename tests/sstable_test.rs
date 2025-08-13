use lsmt::{sstable::SsTable, storage::local::LocalStorage};

#[tokio::test]
async fn sstable_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries = vec![
        ("a".to_string(), b"1".to_vec()),
        ("b".to_string(), b"2".to_vec()),
    ];
    let table = SsTable::create("table", &entries, &storage).await.unwrap();
    assert_eq!(table.get("a", &storage).await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(table.get("c", &storage).await.unwrap(), None);
}
