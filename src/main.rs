use std::{convert::Infallible, net::SocketAddr, sync::Arc};

use cass::{
    Database,
    cluster::Cluster,
    rpc::{
        FlushRequest, FlushResponse, HealthRequest, HealthResponse, PanicRequest, PanicResponse,
        QueryRequest, QueryResponse,
        cass_client::CassClient,
        cass_server::{Cass, CassServer},
    },
    storage::{Storage, local::LocalStorage, s3::S3Storage},
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use hyper::{
    Body as HttpBody, Request as HttpRequest, Response as HttpResponse, Server as HyperServer,
    header::{CONTENT_TYPE, HeaderValue},
    service::{make_service_fn, service_fn},
};
use serde_json::Value;
use tonic::{Request, Response, Status, transport::Server};
use tonic_prometheus_layer::{MetricsLayer, metrics};
use url::Url;

type DynStorage = Arc<dyn Storage>;

#[derive(Parser)]
#[command(name = "cass")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the gRPC server
    Server(ServerArgs),
    /// Broadcast a flush across the cluster via the target node
    Flush { target: String },
    /// Make the specified node unhealthy for a short period
    Panic { target: String },
    /// Start an interactive SQL REPL against the provided nodes
    Repl { nodes: Vec<String> },
}

#[derive(Args)]
struct ServerArgs {
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
struct CassService {
    cluster: Arc<Cluster>,
}

#[tonic::async_trait]
impl Cass for CassService {
    async fn query(&self, req: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
        let sql = req.into_inner().sql;
        match self.cluster.execute(&sql, false).await {
            Ok(Some(bytes)) => Ok(Response::new(QueryResponse { result: bytes })),
            Ok(None) => Ok(Response::new(QueryResponse { result: Vec::new() })),
            Err(e) => Err(Status::invalid_argument(e.to_string())),
        }
    }

    async fn internal(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let sql = req.into_inner().sql;
        match self.cluster.execute(&sql, true).await {
            Ok(Some(bytes)) => Ok(Response::new(QueryResponse { result: bytes })),
            Ok(None) => Ok(Response::new(QueryResponse { result: Vec::new() })),
            Err(e) => Err(Status::invalid_argument(e.to_string())),
        }
    }

    async fn flush(&self, _req: Request<FlushRequest>) -> Result<Response<FlushResponse>, Status> {
        self.cluster.flush_all().await.map_err(Status::internal)?;
        Ok(Response::new(FlushResponse {}))
    }

    async fn flush_internal(
        &self,
        _req: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        self.cluster
            .flush_self()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(FlushResponse {}))
    }

    async fn panic(&self, _req: Request<PanicRequest>) -> Result<Response<PanicResponse>, Status> {
        self.cluster
            .panic_for(std::time::Duration::from_secs(60))
            .await;
        let healthy = self.cluster.self_healthy().await;
        Ok(Response::new(PanicResponse { healthy }))
    }

    async fn health(
        &self,
        _req: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            info: self.cluster.health_info().to_string(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Server(args) => run_server(args).await?,
        Command::Flush { target } => {
            let mut client = CassClient::connect(target).await?;
            client.flush(FlushRequest {}).await?;
        }
        Command::Panic { target } => {
            let mut client = CassClient::connect(target).await?;
            let resp = client.panic(PanicRequest {}).await?;
            println!("healthy: {}", resp.into_inner().healthy);
        }
        Command::Repl { nodes } => repl(nodes).await?,
    }
    Ok(())
}

async fn run_server(args: ServerArgs) -> Result<(), Box<dyn std::error::Error>> {
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

    let svc = CassService { cluster };
    let url = Url::parse(&args.node_addr)?;
    let port = url.port().unwrap_or(80);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], port + 1000));

    metrics::try_init_settings(Default::default()).ok();
    let metrics_layer = MetricsLayer::new();

    tokio::spawn(async move {
        let make_svc = make_service_fn(|_| async {
            Ok::<_, Infallible>(service_fn(|_req: HttpRequest<HttpBody>| async {
                let body = metrics::encode_to_string().unwrap_or_default();
                let response = HttpResponse::builder()
                    .header(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; version=0.0.4"),
                    )
                    .body(HttpBody::from(body))
                    .unwrap();
                Ok::<_, Infallible>(response)
            }))
        });

        if let Err(e) = HyperServer::bind(&metrics_addr).serve(make_svc).await {
            eprintln!("metrics server error: {e}");
        }
    });

    println!("Cass gRPC server listening on {addr}");
    Server::builder()
        .layer(metrics_layer)
        .add_service(CassServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

async fn repl(nodes: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{self, AsyncBufReadExt};
    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    while let Some(line) = lines.next_line().await? {
        let sql = line.trim();
        if sql.is_empty() {
            continue;
        }
        let mut last_err: Option<Status> = None;
        for node in &nodes {
            match CassClient::connect(node.clone()).await {
                Ok(mut client) => match client
                    .query(QueryRequest {
                        sql: sql.to_string(),
                    })
                    .await
                {
                    Ok(resp) => {
                        let bytes = resp.into_inner().result;
                        if bytes.is_empty() {
                            println!("");
                        } else if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
                            println!("{}", serde_json::to_string_pretty(&v).unwrap());
                        } else {
                            println!("{}", String::from_utf8_lossy(&bytes));
                        }
                        last_err = None;
                        break;
                    }
                    Err(e) => last_err = Some(e),
                },
                Err(e) => last_err = Some(Status::unknown(e.to_string())),
            }
        }
        if let Some(err) = last_err {
            eprintln!("query failed: {}", err.message());
        }
    }
    Ok(())
}
