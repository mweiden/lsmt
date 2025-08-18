use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{ConnectInfo, State},
    http::{Request, StatusCode, Version},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use cass::{
    Database,
    cluster::Cluster,
    storage::{Storage, local::LocalStorage, s3::S3Storage},
};
use chrono::Utc;
use clap::{Parser, ValueEnum};
use reqwest::Url;
use serde_json::Value;

type DynStorage = Arc<dyn Storage>;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "local", value_enum)]
    storage: StorageKind,
    #[arg(long, default_value = "/tmp/cass-data")]
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let storage: DynStorage = match args.storage {
        StorageKind::Local => Arc::new(LocalStorage::new(&args.data_dir)),
        StorageKind::S3 => {
            let bucket = args.bucket.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--bucket required for s3 storage mode",
                )
            })?;
            Arc::new(S3Storage::new(&bucket).await?)
        }
    };
    let db = Arc::new(Database::new(storage, "wal.log").await?);
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
        .route("/health", get(handle_health))
        .with_state(state)
        .layer(middleware::from_fn(common_log));

    let url = Url::parse(&args.node_addr)?;
    let port = url.port().unwrap_or(80);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Cass server listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
async fn handle_health(State(state): State<AppState>) -> Json<Value> {
    Json(state.cluster.health_info())
}

async fn common_log(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let version = match req.version() {
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2.0",
        Version::HTTP_3 => "HTTP/3.0",
        _ => "HTTP/?",
    };
    let response = next.run(req).await;
    let status = response.status().as_u16();
    let length = response
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    let now = Utc::now().format("%d/%b/%Y:%H:%M:%S %z");
    println!(
        "{} - - [{}] \"{} {} {}\" {} {}",
        addr.ip(),
        now,
        method,
        uri.path(),
        version,
        status,
        length
    );
    response
}
