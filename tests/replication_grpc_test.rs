use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use cass::rpc::{QueryRequest, cass_client::CassClient};
use serde_json::{Value, json};

#[tokio::test]
async fn union_and_lww_across_replicas() {
    let base1 = "http://127.0.0.1:18081";
    let base2 = "http://127.0.0.1:18082";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_cass");

    let mut child1 = Command::new(bin)
        .args([
            "server",
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
            "server",
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

    for _ in 0..20 {
        let ok1 = CassClient::connect(base1.to_string()).await.is_ok();
        let ok2 = CassClient::connect(base2.to_string()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let mut c1 = CassClient::connect(base1.to_string()).await.unwrap();
    let mut c2 = CassClient::connect(base2.to_string()).await.unwrap();

    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
    })
    .await
    .unwrap();

    c1.internal(QueryRequest {
        sql: "--ts:1\nINSERT INTO kv (id, val) VALUES ('a','va1')".into(),
    })
    .await
    .unwrap();
    c2.internal(QueryRequest {
        sql: "--ts:2\nINSERT INTO kv (id, val) VALUES ('a','va2')".into(),
    })
    .await
    .unwrap();
    c2.internal(QueryRequest {
        sql: "--ts:3\nINSERT INTO kv (id, val) VALUES ('b','vb')".into(),
    })
    .await
    .unwrap();

    let res_a = c1
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'a'".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .result;
    let val_a: Value = serde_json::from_slice(&res_a).unwrap();
    assert_eq!(val_a, json!([{ "val": "va2" }]));

    let res_b = c1
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'b'".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .result;
    let val_b: Value = serde_json::from_slice(&res_b).unwrap();
    assert_eq!(val_b, json!([{ "val": "vb" }]));

    let res_c = c1
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id IN ('a', 'b')".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .result;
    let val_c: Value = serde_json::from_slice(&res_c).unwrap();
    assert_eq!(val_c, json!([{ "val": "va2" }, { "val": "vb" }]));

    let ack = c1
        .query(QueryRequest {
            sql: "INSERT INTO kv (id, val) VALUES ('x','1'),('y','2')".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .result;
    let v: Value = serde_json::from_slice(&ack).unwrap();
    assert_eq!(v, json!({"op":"INSERT","unit":"row","count":2}));

    child1.kill().unwrap();
    child2.kill().unwrap();
}
