use cass::rpc::{QueryRequest, cass_client::CassClient};
use serde_json::{Value, json};
use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

#[tokio::test]
async fn grpc_query_roundtrip() {
    let _ = std::fs::remove_dir_all("/tmp/cass-data");

    let mut child = Command::new(env!("CARGO_BIN_EXE_cass"))
        .arg("server")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn server");

    let base = "http://127.0.0.1:8080".to_string();
    for _ in 0..10 {
        if CassClient::connect(base.clone()).await.is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let mut client = CassClient::connect(base.clone()).await.unwrap();
    client
        .query(QueryRequest {
            sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
        })
        .await
        .unwrap();

    client
        .query(QueryRequest {
            sql: "INSERT INTO kv (id, val) VALUES ('foo','bar')".into(),
        })
        .await
        .unwrap();

    let res = client
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'foo'".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .result;
    let val: Value = serde_json::from_slice(&res).unwrap();
    assert_eq!(val, json!([{ "val": "bar" }]));

    let count = client
        .query(QueryRequest {
            sql: "SELECT COUNT(*) FROM kv WHERE id = 'foo'".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .result;
    let cnt: Value = serde_json::from_slice(&count).unwrap();
    assert_eq!(cnt, json!([{"count":1}]));

    child.kill().unwrap();
}
