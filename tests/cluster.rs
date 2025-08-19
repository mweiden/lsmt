use cass::Database;
use cass::cluster::Cluster;
use cass::storage::local::LocalStorage;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

async fn build_cluster(peers: Vec<String>, vnodes: usize, rf: usize, self_addr: &str) -> Cluster {
    let dir = tempdir().unwrap();
    let storage = Arc::new(LocalStorage::new(dir.path()));
    let db = Arc::new(Database::new(storage, "wal.log").await.unwrap());
    Cluster::new(db, self_addr.to_string(), peers, vnodes, rf)
}

#[tokio::test]
async fn replicas_for_returns_unique_nodes() {
    let self_addr = "http://127.0.0.1:1000".to_string();
    let peer = "http://127.0.0.1:1001".to_string();
    let cluster = build_cluster(vec![peer.clone()], 2, 2, &self_addr).await;
    let reps = cluster.replicas_for("somekey");
    assert_eq!(reps.len(), 2);
    let set: HashSet<_> = reps.iter().cloned().collect();
    assert_eq!(set.len(), 2);
    assert!(set.contains(&self_addr));
    assert!(set.contains(&peer));
}

#[tokio::test]
async fn is_alive_false_when_stale() {
    let self_addr = "http://127.0.0.1:2000".to_string();
    let peer = "http://127.0.0.1:2001".to_string();
    let cluster = build_cluster(vec![peer.clone()], 1, 1, &self_addr).await;
    assert!(cluster.is_alive(&self_addr).await);
    assert!(cluster.is_alive(&peer).await);
    tokio::time::sleep(Duration::from_secs(9)).await;
    assert!(!cluster.is_alive(&peer).await);
}

#[tokio::test]
async fn health_info_reports_tokens() {
    let self_addr = "http://127.0.0.1:3000".to_string();
    let cluster = build_cluster(Vec::new(), 3, 1, &self_addr).await;
    let info = cluster.health_info();
    assert_eq!(info["node"].as_str().unwrap(), self_addr);
    assert_eq!(info["tokens"].as_array().unwrap().len(), 3);
    assert!(info["timestamp"].as_u64().is_some());
}

#[tokio::test]
async fn flip_health_toggles_self() {
    let self_addr = "http://127.0.0.1:4000".to_string();
    let cluster = build_cluster(Vec::new(), 1, 1, &self_addr).await;
    assert!(cluster.is_alive(&self_addr).await);
    assert!(!cluster.flip_health().await);
    assert!(!cluster.is_alive(&self_addr).await);
    assert!(cluster.flip_health().await);
    assert!(cluster.is_alive(&self_addr).await);
}

#[tokio::test]
async fn flush_all_flushes_memtable() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(LocalStorage::new(dir.path()));
    let db = Arc::new(Database::new(storage, "wal.log").await.unwrap());
    let self_addr = "http://127.0.0.1:5000".to_string();
    let cluster = Cluster::new(db.clone(), self_addr, Vec::new(), 1, 1);
    db.insert("k".into(), b"v".to_vec()).await;
    assert_eq!(db.memtable().len().await, 1);
    cluster.flush_all().await.unwrap();
    assert_eq!(db.memtable().len().await, 0);
}
