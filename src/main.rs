#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod api;
mod app;
mod bootstrap;
mod cache;
mod config;
mod db;
mod error;
mod middleware;
mod queue;

use crate::cache::redis::RedisCache;
use crate::db::shard::ShardRouter;
use crate::queue::producer::QueueProducer;

/// Shared application state.
///
/// Fix #30: `circuit_breaker` and `backpressure` live only in middleware.
/// Their metrics are now published eagerly from the middleware layers,
/// so the `/metrics` handler does not need a reference to them.
#[derive(Clone)]
pub struct AppState {
    pub shard_router: ShardRouter,
    pub cache: RedisCache,
    pub queue_producer: QueueProducer,
    pub metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Distroless health probe: `gn-backend --health-check`
    if std::env::args().any(|a| a == "--health-check") {
        return health_probe().await;
    }

    dotenvy::dotenv().ok();
    let app = app::App::new().await?;
    app.run().await
}

/// Lightweight health probe for Docker HEALTHCHECK in Distroless images.
/// Connects to the local HTTP server and checks /health returns 200.
async fn health_probe() -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect("127.0.0.1:3000").await?;
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;

    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let response = String::from_utf8_lossy(&buf[..n]);

    if response.contains("200 OK") {
        std::process::exit(0);
    } else {
        std::process::exit(1);
    }
}
