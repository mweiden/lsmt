use axum::{Router, extract::State, http::StatusCode, routing::post};
use lsmt::{Database, SqlEngine, storage::local::LocalStorage};
use std::sync::Arc;
use tokio::task::JoinHandle;

async fn start_server() -> (String, JoinHandle<()>) {
    async fn handle_query(
        State(db): State<Arc<Database<LocalStorage>>>,
        body: String,
    ) -> (StatusCode, String) {
        let engine = SqlEngine::new();
        match engine.execute(&db, &body).await {
            Ok(Some(bytes)) => (StatusCode::OK, String::from_utf8_lossy(&bytes).to_string()),
            Ok(None) => (StatusCode::OK, String::new()),
            Err(e) => (StatusCode::BAD_REQUEST, e.to_string()),
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::new(dir.path());
    let db = Arc::new(Database::new(storage));
    let app = Router::new()
        .route("/query", post(handle_query))
        .with_state(db);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (url, server)
}

#[tokio::test]
async fn http_query_roundtrip() {
    let (base, server) = start_server().await;
    let client = reqwest::Client::new();

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

    server.abort();
}
