use cass::memtable::MemTable;

#[tokio::test]
async fn memtable_basic_operations() {
    let table = MemTable::new();

    // insert and get
    table.insert("k1".to_string(), b"v1".to_vec()).await;
    assert_eq!(table.len().await, 1);
    assert_eq!(table.get("k1").await, Some(b"v1".to_vec()));

    // get missing
    assert!(table.get("missing").await.is_none());

    // delete existing and missing
    table.delete("k1").await;
    table.delete("missing").await; // should be no-op
    assert_eq!(table.len().await, 0);
}

#[tokio::test]
async fn memtable_scan_clear_and_prefix() {
    let table = MemTable::new();
    table.insert("a1".to_string(), b"v1".to_vec()).await;
    table.insert("a2".to_string(), b"v2".to_vec()).await;
    table.insert("b1".to_string(), b"v3".to_vec()).await;

    // scan returns sorted pairs
    let scan = table.scan().await;
    assert_eq!(
        scan,
        vec![
            ("a1".to_string(), b"v1".to_vec()),
            ("a2".to_string(), b"v2".to_vec()),
            ("b1".to_string(), b"v3".to_vec()),
        ]
    );

    // delete prefix affects matching keys
    table.delete_prefix("a").await;
    assert_eq!(table.len().await, 1);
    assert!(table.get("a1").await.is_none());
    assert!(table.get("b1").await.is_some());

    // deleting with empty prefix clears all
    table.delete_prefix("").await;
    assert_eq!(table.len().await, 0);

    table.insert("x".to_string(), b"y".to_vec()).await;
    table.clear().await;
    assert_eq!(table.len().await, 0);
}
