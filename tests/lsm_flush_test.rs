use lsmt::{Database, storage::local::LocalStorage};

#[tokio::test]
async fn flush_and_query_from_sstable() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let wal = dir.path().join("wal.log");
    let db = Database::new(storage, wal).await;

    db.insert("k1".to_string(), b"v1".to_vec()).await;
    db.insert("k2".to_string(), b"v2".to_vec()).await;
    // manually flush current memtable to disk
    db.flush().await.unwrap();

    // data should be retrievable even after memtable cleared
    assert_eq!(db.get("k1").await, Some(b"v1".to_vec()));
    assert_eq!(db.get("k2").await, Some(b"v2".to_vec()));
    assert_eq!(db.get("missing").await, None);
}
