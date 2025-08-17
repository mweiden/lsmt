use std::{net::SocketAddr, sync::Arc};

use axum::{Router, extract::State, http::StatusCode, routing::post};
use clap::{Parser, ValueEnum};
use lsmt::{
    Database,
    cluster::Cluster,
    storage::{Storage, local::LocalStorage, s3::S3Storage},
};
use reqwest::Url;

type DynStorage = Arc<dyn Storage>;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "local", value_enum)]
    storage: StorageKind,
    #[arg(long, default_value = "/tmp/lsmt-data")]
    data_dir: String,
    #[arg(long)]
    bucket: Option<String>,
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    node_addr: String,
    #[arg(long)]
    peer: Vec<String>,
    #[arg(long, default_value_t = 1)]
    rf: usize,
    #[arg(long, default_value_t = 8)]
    vnodes: usize,
}

#[derive(Copy, Clone, ValueEnum)]
enum StorageKind {
    Local,
    S3,
}

#[derive(Clone)]
struct AppState {
    cluster: Arc<Cluster>,
}

/// Handle incoming SQL queries sent by clients.
async fn handle_query(State(state): State<AppState>, body: String) -> (StatusCode, String) {
    match state.cluster.execute(&body, false).await {
        Ok(Some(bytes)) => (StatusCode::OK, String::from_utf8_lossy(&bytes).to_string()),
        Ok(None) => (StatusCode::OK, String::new()),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()),
    }
}

/// Handle internal replication requests from peers.
async fn handle_internal(State(state): State<AppState>, body: String) -> (StatusCode, String) {
    match state.cluster.execute(&body, true).await {
        Ok(Some(bytes)) => (StatusCode::OK, String::from_utf8_lossy(&bytes).to_string()),
        Ok(None) => (StatusCode::OK, String::new()),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()),
    }
}

/// Start an HTTP server exposing the database at `/query`.
#[tokio::main]
async fn main() {
    let args = Args::parse();

    let storage: DynStorage = match args.storage {
        StorageKind::Local => Arc::new(LocalStorage::new(&args.data_dir)),
        StorageKind::S3 => {
            let bucket = args.bucket.expect("--bucket required for s3 storage mode");
            Arc::new(
                S3Storage::new(&bucket)
                    .await
                    .expect("failed to create s3 storage"),
            )
        }
    };
    let db = Arc::new(Database::new(storage, "wal.log").await);
    let cluster = Arc::new(Cluster::new(
        db.clone(),
        args.node_addr.clone(),
        args.peer.clone(),
        args.vnodes,
        args.rf,
    ));
    let state = AppState { cluster };

    let app = Router::new()
        .route("/query", post(handle_query))
        .route("/internal", post(handle_internal))
        .with_state(state);

    let url = Url::parse(&args.node_addr).expect("invalid --node-addr");
    let port = url.port().unwrap_or(80);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("LSMT server listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
