use cass::storage::{Storage, local::LocalStorage};
use cass::{Database, SqlEngine};
use std::sync::Arc;

#[tokio::test]
async fn create_insert_select_schema_table() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await;
    let engine = SqlEngine::new();

    engine
        .execute(
            &db,
            "CREATE TABLE users (user_id TEXT, ts TEXT, value TEXT, PRIMARY KEY(user_id, ts))",
        )
        .await
        .unwrap();

    engine
        .execute(
            &db,
            "INSERT INTO users (user_id, ts, value) VALUES ('u1','1','hello')",
        )
        .await
        .unwrap();

    let res = engine
        .execute(&db, "SELECT value FROM users WHERE user_id='u1' AND ts='1'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(res).unwrap(), "hello");
}

#[tokio::test]
async fn create_existing_table_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await;
    let engine = SqlEngine::new();

    engine
        .execute(
            &db,
            "CREATE TABLE users (id TEXT, val TEXT, PRIMARY KEY(id))",
        )
        .await
        .unwrap();

    let res = engine
        .execute(
            &db,
            "CREATE TABLE users (id TEXT, val TEXT, PRIMARY KEY(id))",
        )
        .await;
    assert!(res.is_err());
}
