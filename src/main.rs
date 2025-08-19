use std::{fs, net::SocketAddr, path::Path, sync::Arc, time::Instant};

use axum::{
    Json, Router,
    body::Body,
    extract::{ConnectInfo, State},
    http::{Request, StatusCode, Version},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use cass::{
    Database,
    cluster::Cluster,
    storage::{Storage, local::LocalStorage, s3::S3Storage},
};
use chrono::Utc;
use clap::{Parser, ValueEnum};
use prometheus::{
    Encoder, Gauge, HistogramVec, IntCounterVec, IntGaugeVec, TextEncoder, register_gauge,
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec,
};
use reqwest::Url;
use serde_json::{Value, json};
use sysinfo::System;

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
    metrics: Arc<Metrics>,
    data_dir: String,
}

#[derive(Clone)]
struct Metrics {
    req_count: IntCounterVec,
    req_duration: HistogramVec,
    health: IntGaugeVec,
    ram: Gauge,
    cpu: Gauge,
    sstable: Gauge,
}

impl Metrics {
    fn new() -> Self {
        Self {
            req_count: register_int_counter_vec!(
                "http_requests_total",
                "HTTP request counts",
                &["method", "path", "status"]
            )
            .unwrap(),
            req_duration: register_histogram_vec!(
                "http_request_duration_seconds",
                "HTTP request latencies",
                &["method", "path", "status"]
            )
            .unwrap(),
            health: register_int_gauge_vec!(
                "node_health",
                "Health status of peer nodes",
                &["peer"]
            )
            .unwrap(),
            ram: register_gauge!("ram_usage_bytes", "RAM usage in bytes").unwrap(),
            cpu: register_gauge!("cpu_usage_percent", "CPU usage percentage").unwrap(),
            sstable: register_gauge!("sstable_disk_usage_bytes", "SSTable disk usage in bytes")
                .unwrap(),
        }
    }
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

async fn handle_flip(State(state): State<AppState>) -> Json<Value> {
    let healthy = state.cluster.flip_health().await;
    Json(json!({ "healthy": healthy }))
}

async fn handle_flush(State(state): State<AppState>) -> (StatusCode, String) {
    match state.cluster.flush_all().await {
        Ok(_) => (StatusCode::OK, String::new()),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn handle_flush_internal(State(state): State<AppState>) -> (StatusCode, String) {
    match state.cluster.flush_self().await {
        Ok(_) => (StatusCode::OK, String::new()),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
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
    let metrics = Arc::new(Metrics::new());
    let state = AppState {
        cluster,
        metrics: metrics.clone(),
        data_dir: args.data_dir.clone(),
    };

    let app = Router::new()
        .route("/query", post(handle_query))
        .route("/internal", post(handle_internal))
        .route("/health", get(handle_health))
        .route("/flush", post(handle_flush))
        .route("/flush_internal", post(handle_flush_internal))
        .route("/flip", post(handle_flip))
        .route("/metrics", get(handle_metrics))
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state, common_log));

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
async fn handle_health(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let status = if state.cluster.self_healthy().await {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(state.cluster.health_info()))
}

async fn handle_metrics(State(state): State<AppState>) -> impl IntoResponse {
    for (peer, alive) in state.cluster.peer_health().await {
        state
            .metrics
            .health
            .with_label_values(&[&peer])
            .set(if alive { 1 } else { 0 });
    }

    let mut sys = System::new_all();
    sys.refresh_memory();
    sys.refresh_cpu();
    let ram = sys.used_memory() as f64 * 1024.0;
    let cpu = sys.global_cpu_info().cpu_usage() as f64;
    state.metrics.ram.set(ram);
    state.metrics.cpu.set(cpu);
    let disk = sstable_disk_usage(&state.data_dir) as f64;
    state.metrics.sstable.set(disk);

    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, encoder.format_type())
        .body(String::from_utf8(buffer).unwrap())
        .unwrap()
}

fn sstable_disk_usage(dir: &str) -> u64 {
    fn visit(path: &Path) -> u64 {
        let mut size = 0;
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    size += visit(&p);
                } else if p.extension().and_then(|e| e.to_str()) == Some("sst") {
                    if let Ok(meta) = entry.metadata() {
                        size += meta.len();
                    }
                }
            }
        }
        size
    }
    visit(Path::new(dir))
}

async fn common_log(
    State(state): State<AppState>,
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
    let start = Instant::now();
    let response = next.run(req).await;
    let status = response.status().as_u16();
    let length = response
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    let now = Utc::now().format("%d/%b/%Y:%H:%M:%S %z");
    // Write the log line atomically so concurrent requests don't interleave
    // and produce stray blank lines in aggregated logs.
    {
        use std::io::{self, Write};
        let line = format!(
            "{} - - [{}] \"{} {} {}\" {} {}\n",
            addr.ip(),
            now,
            method,
            uri.path(),
            version,
            status,
            length
        );
        let _ = io::stdout().write_all(line.as_bytes());
    }
    let status_str = status.to_string();
    let path = uri.path().to_string();
    let elapsed = start.elapsed().as_secs_f64();
    state
        .metrics
        .req_count
        .with_label_values(&[method.as_str(), path.as_str(), &status_str])
        .inc();
    state
        .metrics
        .req_duration
        .with_label_values(&[method.as_str(), path.as_str(), &status_str])
        .observe(elapsed);
    response
}
