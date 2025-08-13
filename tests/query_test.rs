use lsmt::{storage::local::LocalStorage, Database, SqlEngine};

#[tokio::test]
async fn query_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let db = Database::new(storage);
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
