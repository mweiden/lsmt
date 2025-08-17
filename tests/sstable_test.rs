use cass::{
    sstable::SsTable,
    storage::{Storage, local::LocalStorage},
};

#[tokio::test]
async fn sstable_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let entries = vec![
        ("b".to_string(), b"2".to_vec()),
        ("a".to_string(), b"1".to_vec()),
        ("c".to_string(), b"3".to_vec()),
    ];
    let table = SsTable::create("table", &entries, &storage).await.unwrap();
    assert_eq!(table.get("a", &storage).await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(table.get("b", &storage).await.unwrap(), Some(b"2".to_vec()));
    let raw = storage.get("table").await.unwrap();
    let text = String::from_utf8(raw).unwrap();
    let keys: Vec<&str> = text
        .lines()
        .map(|l| l.split('\t').next().unwrap())
        .collect();
    assert_eq!(keys, vec!["a", "b", "c"]);
}
