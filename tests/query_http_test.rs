use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

#[tokio::test]
async fn http_query_roundtrip() {
    let _ = std::fs::remove_dir_all("/tmp/lsmt-data");

    let mut child = Command::new(env!("CARGO_BIN_EXE_lsmt"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn server");

    let base = "http://127.0.0.1:8080";
    let client = reqwest::Client::new();

    for _ in 0..10 {
        if client
            .post(format!("{}/query", base))
            .body("")
            .send()
            .await
            .is_ok()
        {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let insert = client
        .post(format!("{}/query", base))
        .body("INSERT INTO kv VALUES ('foo','bar')")
        .send()
        .await
        .unwrap();
    assert!(insert.status().is_success());

    let res = client
        .post(format!("{}/query", base))
        .body("SELECT value FROM kv WHERE key = 'foo'")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(res, "bar");

    child.kill().unwrap();
}
