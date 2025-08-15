use lsmt::{
    Database, SqlEngine,
    storage::{Storage, local::LocalStorage},
};
use std::sync::Arc;

#[tokio::test]
async fn e2e_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    let db = Database::new(storage, "wal.log").await;
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
