use std::{
    collections::BTreeMap,
    io::Cursor,
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use murmur3::murmur3_32;
use reqwest::Client;

fn build_ring(nodes: &[String], vnodes: usize) -> BTreeMap<u32, String> {
    let mut ring = BTreeMap::new();
    for node in nodes.iter() {
        for v in 0..vnodes.max(1) {
            let token_key = format!("{}-{}", node, v);
            let mut cursor = Cursor::new(token_key.as_bytes());
            let token = murmur3_32(&mut cursor, 0).unwrap();
            ring.insert(token, node.clone());
        }
    }
    ring
}

fn replicas_for(ring: &BTreeMap<u32, String>, key: &str, rf: usize) -> Vec<String> {
    let mut cursor = Cursor::new(key.as_bytes());
    let token = murmur3_32(&mut cursor, 0).unwrap();
    let mut reps = Vec::new();
    for (_k, node) in ring.range(token..) {
        if !reps.contains(node) {
            reps.push(node.clone());
        }
        if reps.len() == rf {
            return reps;
        }
    }
    for (_k, node) in ring {
        if !reps.contains(node) {
            reps.push(node.clone());
        }
        if reps.len() == rf {
            break;
        }
    }
    reps
}

#[tokio::test]
async fn forwarded_query_returns_remote_result() {
    let base1 = "http://127.0.0.1:18081";
    let base2 = "http://127.0.0.1:18082";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_lsmt");

    let mut child1 = Command::new(bin)
        .args([
            "--data-dir",
            dir1.path().to_str().unwrap(),
            "--node-addr",
            base1,
            "--peer",
            base2,
            "--rf",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut child2 = Command::new(bin)
        .args([
            "--data-dir",
            dir2.path().to_str().unwrap(),
            "--node-addr",
            base2,
            "--peer",
            base1,
            "--rf",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let client = Client::new();
    for _ in 0..20 {
        let ok1 = client
            .post(format!("{}/query", base1))
            .body("")
            .send()
            .await
            .is_ok();
        let ok2 = client
            .post(format!("{}/query", base2))
            .body("")
            .send()
            .await
            .is_ok();
        if ok1 && ok2 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let create_sql = "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))";
    client
        .post(format!("{}/query", base1))
        .body(create_sql)
        .send()
        .await
        .unwrap();

    let ring = build_ring(&vec![base1.to_string(), base2.to_string()], 8);
    let mut key = String::new();
    for i in 0..1000 {
        let candidate = format!("k{}", i);
        let reps = replicas_for(&ring, &candidate, 1);
        if reps[0] == base2 {
            key = candidate;
            break;
        }
    }
    assert!(!key.is_empty());

    client
        .post(format!("{}/query", base1))
        .body(format!("INSERT INTO kv (id, val) VALUES ('{}','v')", key))
        .send()
        .await
        .unwrap();

    let res = client
        .post(format!("{}/query", base1))
        .body(format!("SELECT val FROM kv WHERE id = '{}'", key))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(res, "v");

    child1.kill().unwrap();
    child2.kill().unwrap();
}
