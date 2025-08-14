use std::{net::SocketAddr, sync::Arc};

use axum::{extract::State, http::StatusCode, routing::post, Router};
use lsmt::{storage::local::LocalStorage, Database, SqlEngine};

/// Handle incoming SQL queries sent to the server.
async fn handle_query(
    State(db): State<Arc<Database<LocalStorage>>>,
    body: String,
) -> (StatusCode, String) {
    let engine = SqlEngine::new();
    match engine.execute(&db, &body).await {
        Ok(Some(bytes)) => (
            StatusCode::OK,
            String::from_utf8_lossy(&bytes).to_string(),
        ),
        Ok(None) => (StatusCode::OK, String::new()),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()),
    }
}

/// Start an HTTP server exposing the database at `/query`.
#[tokio::main]
async fn main() {
    let storage = LocalStorage::new("/tmp/lsmt-data");
    let db = Arc::new(Database::new(storage));

    let app = Router::new().route("/query", post(handle_query)).with_state(db);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    println!("LSMT server listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
