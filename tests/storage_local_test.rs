use cass::storage::Storage;
use cass::storage::local::LocalStorage;

#[tokio::test]
async fn local_storage_put_and_get() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(tmp.path());

    storage
        .put("nested/dir/file.txt", b"hello".to_vec())
        .await
        .unwrap();
    let data = storage.get("nested/dir/file.txt").await.unwrap();
    assert_eq!(data, b"hello");
}

#[tokio::test]
async fn local_storage_get_missing_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(tmp.path());
    let res = storage.get("missing").await;
    assert!(res.is_err());
}
