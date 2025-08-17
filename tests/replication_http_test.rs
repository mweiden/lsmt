use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use reqwest::Client;

#[tokio::test]
async fn union_and_lww_across_replicas() {
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

    let create_sql = "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))";
    client
        .post(format!("{}/query", base1))
        .body(create_sql)
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/internal", base1))
        .body("--ts:1\nINSERT INTO kv (id, val) VALUES ('a','va1')")
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/internal", base2))
        .body("--ts:2\nINSERT INTO kv (id, val) VALUES ('a','va2')")
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/internal", base2))
        .body("--ts:3\nINSERT INTO kv (id, val) VALUES ('b','vb')")
        .send()
        .await
        .unwrap();

    let res_a = client
        .post(format!("{}/query", base1))
        .body("SELECT val FROM kv WHERE id = 'a'")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(res_a, "va2");

    let res_b = client
        .post(format!("{}/query", base1))
        .body("SELECT val FROM kv WHERE id = 'b'")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(res_b, "vb");

    child1.kill().unwrap();
    child2.kill().unwrap();
}
