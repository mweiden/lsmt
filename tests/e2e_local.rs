use lsmt::{Database, SqlEngine, storage::local::LocalStorage};

#[tokio::test]
async fn e2e_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let wal = dir.path().join("wal.log");
    let db = Database::new(storage, wal).await;
    let engine = SqlEngine::new();
    engine
        .execute(&db, "INSERT INTO kv VALUES ('foo','bar')")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT * FROM kv WHERE key='foo'")
        .await
        .unwrap();
    assert_eq!(res, Some(b"bar".to_vec()));
}
