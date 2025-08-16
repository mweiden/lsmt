use lsmt::storage::{Storage, local::LocalStorage};
use lsmt::{Database, SqlEngine};
use std::sync::Arc;

#[tokio::test]
async fn update_delete_and_count() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await;
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
    assert_eq!(String::from_utf8(res).unwrap(), "2");

    engine
        .execute(&db, "DELETE FROM kv WHERE id = 'a'")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT val FROM kv WHERE id = 'a'")
        .await
        .unwrap();
    assert!(res.is_none());

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
    assert_eq!(String::from_utf8(res).unwrap(), "2");
    let res = engine
        .execute(&db, "SELECT COUNT(*) FROM kv WHERE id = 'b'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(res).unwrap(), "1");

    let res = engine
        .execute(&db, "SELECT COUNT(*) FROM kv WHERE val = '3'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(res).unwrap(), "1");
}

#[tokio::test]
async fn table_names_and_cast() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await;
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
    assert_eq!(String::from_utf8(tables).unwrap().trim(), "kv");

    let cast = engine
        .execute(&db, "SELECT CAST(val AS INT) FROM kv WHERE id = 'a'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(cast).unwrap(), "1");
}
