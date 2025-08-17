use lsmt::{
    Database, SqlEngine,
    storage::{Storage, local::LocalStorage},
};
use serde_json::{Value, json};
use std::sync::Arc;

#[tokio::test]
async fn e2e_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    let db = Database::new(storage, "wal.log").await;
    let engine = SqlEngine::new();
    engine
        .execute(&db, "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('foo','bar')")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT val FROM kv WHERE id='foo'")
        .await
        .unwrap()
        .unwrap();
    let val: Value = serde_json::from_slice(&res).unwrap();
    assert_eq!(val, json!([{ "val": "bar" }]));
}
