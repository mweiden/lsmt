use lsmt::storage::local::LocalStorage;
use lsmt::{Database, SqlEngine};

#[tokio::test]
async fn update_delete_and_count() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(tmp.path());
    let wal = tmp.path().join("wal.log");
    let db = Database::new(storage, wal).await;
    let engine = SqlEngine::new();

    engine
        .execute(&db, "INSERT INTO kv VALUES ('a','1')")
        .await
        .unwrap();
    engine
        .execute(&db, "UPDATE kv SET value = '2' WHERE key = 'a'")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT value FROM kv WHERE key = 'a'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(res).unwrap(), "2");

    engine
        .execute(&db, "DELETE FROM kv WHERE key = 'a'")
        .await
        .unwrap();
    let res = engine
        .execute(&db, "SELECT value FROM kv WHERE key = 'a'")
        .await
        .unwrap();
    assert!(res.is_none());

    engine
        .execute(&db, "INSERT INTO kv VALUES ('b','3')")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO kv VALUES ('c','4')")
        .await
        .unwrap();

    let res = engine
        .execute(&db, "SELECT COUNT(*) FROM kv")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(res).unwrap(), "2");

    let res = engine
        .execute(&db, "SELECT key FROM kv ORDER BY key DESC LIMIT 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(res).unwrap(), "c");
}

#[tokio::test]
async fn table_names_group_by_and_cast() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(tmp.path());
    let wal = tmp.path().join("wal.log");
    let db = Database::new(storage, wal).await;
    let engine = SqlEngine::new();

    engine.execute(&db, "CREATE TABLE foo.kv").await.unwrap();
    engine
        .execute(&db, "INSERT INTO foo.kv VALUES ('a','001')")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO foo.kv VALUES ('b','002')")
        .await
        .unwrap();
    engine
        .execute(&db, "INSERT INTO foo.kv VALUES ('c','001')")
        .await
        .unwrap();

    let res = engine
        .execute(&db, "SELECT value FROM kv WHERE key = 'a'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(res).unwrap(), "001");

    let tables = engine.execute(&db, "SHOW TABLES").await.unwrap().unwrap();
    assert_eq!(String::from_utf8(tables).unwrap().trim(), "kv");

    let cast = engine
        .execute(&db, "SELECT CAST(value AS INT) FROM foo.kv WHERE key = 'a'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(cast).unwrap(), "1");

    let grp = engine
        .execute(&db, "SELECT COUNT(*) FROM foo.kv GROUP BY value")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(String::from_utf8(grp).unwrap(), "2");
}
