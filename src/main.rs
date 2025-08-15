use std::{net::SocketAddr, sync::Arc};

use axum::{Router, extract::State, http::StatusCode, routing::post};
use clap::{Parser, ValueEnum};
use lsmt::{
    Database, SqlEngine,
    storage::{Storage, local::LocalStorage, s3::S3Storage},
};

type DynStorage = Box<dyn Storage>;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "local", value_enum)]
    storage: StorageKind,
    #[arg(long, default_value = "/tmp/lsmt-data")]
    data_dir: String,
    #[arg(long)]
    bucket: Option<String>,
}

#[derive(Copy, Clone, ValueEnum)]
enum StorageKind {
    Local,
    S3,
}

/// Handle incoming SQL queries sent to the server.
async fn handle_query(
    State(db): State<Arc<Database<DynStorage>>>,
    body: String,
) -> (StatusCode, String) {
    let engine = SqlEngine::new();
    match engine.execute(&db, &body).await {
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
        StorageKind::Local => Box::new(LocalStorage::new(&args.data_dir)),
        StorageKind::S3 => {
            let bucket = args.bucket.expect("--bucket required for s3 storage mode");
            Box::new(
                S3Storage::new(&bucket)
                    .await
                    .expect("failed to create s3 storage"),
            )
        }
    };
    let wal_path = std::path::Path::new(&args.data_dir).join("wal.log");
    if let Some(parent) = wal_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let db = Arc::new(Database::new(storage, wal_path).await);

    let app = Router::new()
        .route("/query", post(handle_query))
        .with_state(db);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    println!("LSMT server listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
