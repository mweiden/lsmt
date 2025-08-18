use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use reqwest::{Client, StatusCode};
use serde_json::Value;

#[tokio::test]
async fn health_endpoint_reports_tokens() {
    let base = "http://127.0.0.1:18085";
    let dir = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_cass");
    let mut child = Command::new(bin)
        .args([
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--node-addr",
            base,
            "--rf",
            "1",
            "--vnodes",
            "4",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let client = Client::new();
    for _ in 0..20 {
        if client
            .get(format!("{}/health", base))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let body = client
        .get(format!("{}/health", base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["node"], base);
    assert_eq!(v["tokens"].as_array().unwrap().len(), 4);

    child.kill().unwrap();
}

#[tokio::test]
async fn errors_when_not_enough_healthy_replicas() {
    let base1 = "http://127.0.0.1:18091";
    let base2 = "http://127.0.0.1:18092";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_cass");

    let mut child1 = Command::new(bin)
        .args([
            "--data-dir",
            dir1.path().to_str().unwrap(),
            "--node-addr",
            base1,
            "--peer",
            base2,
            "--rf",
            "2",
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
            "2",
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

    client
        .post(format!("{}/query", base1))
        .body("CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))")
        .send()
        .await
        .unwrap();

    child2.kill().unwrap();

    thread::sleep(Duration::from_secs(9));

    let res = client
        .post(format!("{}/query", base1))
        .body("INSERT INTO kv (id, val) VALUES ('x','1')")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert!(body.contains("not enough healthy replicas"));

    child1.kill().unwrap();
}
