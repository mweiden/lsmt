use cass::storage::{Storage, local::LocalStorage};
use cass::{Database, SqlEngine};
use serde_json::{Value, json};
use std::sync::Arc;

#[tokio::test]
async fn update_delete_and_count() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(&db, "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('a','1')")
        .await
        .unwrap();
    engine
        .execute(&db, "UPDATE kv SET val = '2' WHERE id = 'a'")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT val FROM kv WHERE id = 'a'")
        .await
        .unwrap()
        .unwrap();
    let val: Value = serde_json::from_slice(&res).unwrap();
    assert_eq!(val, json!([{ "val": "2" }]));

    engine
        .execute(&db, "DELETE FROM kv WHERE id = 'a'")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT val FROM kv WHERE id = 'a'")
        .await
        .unwrap()
        .unwrap();
    let v: Value = serde_json::from_slice(&res).unwrap();
    assert_eq!(v, json!([]));

    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('b','3')")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('c','4')")
        .await
        .unwrap();

    let res = engine
        .execute(&db, "SELECT COUNT(*) FROM kv")
        .await
        .unwrap()
        .unwrap();
    let v: Value = serde_json::from_slice(&res).unwrap();
    assert_eq!(v, json!([{"count":2}]));
    let res = engine
        .execute(&db, "SELECT COUNT(*) FROM kv WHERE id = 'b'")
        .await
        .unwrap()
        .unwrap();
    let v: Value = serde_json::from_slice(&res).unwrap();
    assert_eq!(v, json!([{"count":1}]));

    let res = engine
        .execute(&db, "SELECT COUNT(*) FROM kv WHERE val = '3'")
        .await
        .unwrap()
        .unwrap();
    let v: Value = serde_json::from_slice(&res).unwrap();
    assert_eq!(v, json!([{"count":1}]));
}

#[tokio::test]
async fn table_names_and_cast() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(&db, "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('a','001')")
        .await
        .unwrap();

    let tables = engine.execute(&db, "SHOW TABLES").await.unwrap().unwrap();
    let tbls: Value = serde_json::from_slice(&tables).unwrap();
    assert_eq!(tbls, json!(["kv"]));

    let cast = engine
        .execute(&db, "SELECT CAST(val AS INT) FROM kv WHERE id = 'a'")
        .await
        .unwrap()
        .unwrap();
    let castv: Value = serde_json::from_slice(&cast).unwrap();
    assert_eq!(castv, json!([{ "val": "1" }]));
}

#[tokio::test]
async fn multi_row_insert_count() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(&db, "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "INSERT INTO kv (id, val) VALUES ('a','1'),('b','2')")
        .await
        .unwrap()
        .unwrap();
    let ack: Value = serde_json::from_slice(&res).unwrap();
    assert_eq!(ack, json!({"op":"INSERT","unit":"row","count":2}));
}
