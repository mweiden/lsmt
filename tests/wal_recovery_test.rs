use lsmt::{Database, storage::local::LocalStorage};

#[tokio::test]
async fn wal_recovery_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let wal = path.join("wal.log");
    {
        let storage = LocalStorage::new(&path);
        let db = Database::new(storage, &wal).await;
        db.insert("k1".to_string(), b"v1".to_vec()).await;
    }
    let storage = LocalStorage::new(&path);
    let db = Database::new(storage, &wal).await;
    assert_eq!(db.get("k1").await, Some(b"v1".to_vec()));
}
