//! Composition root for the peakload-capstone binary.
//!
//! Phase 4 of the modular-monolith migration split the project into
//! a cargo workspace. Business modules now live in their own crates
//! (`accounts`, `transactions`, `notifications`); cross-cutting
//! infrastructure lives in `shared_kernel`. This crate (`app`) is
//! the only binary, and its sole job is to wire those crates
//! together — `bootstrap.rs` does the wiring, `app.rs` runs the
//! resulting graph, `main.rs` is the entry point.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod app;
mod bootstrap;
mod config;
mod health;
mod middleware;

use shared_kernel::cache::redis::RedisCache;
use shared_kernel::db::shard::ShardRouter;
use shared_kernel::queue::producer::QueueProducer;

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
    // Distroless health probe: `peakload-capstone --health-check`
    if std::env::args().any(|a| a == "--health-check") {
        return health_probe().await;
    }

    // One-shot migrator (D-7): `peakload-capstone --migrate`
    // Reads `MIGRATE_SHARD_URLS` (CSV of shard primary URLs via
    // HAProxy → Patroni), connects directly to each primary (NOT
    // through pgBouncer), and applies pending sqlx migrations.
    // Runs as its own compose service so the app's startup remains
    // pure and so the WP1b server-side `lock_timeout = 500ms`
    // (which would cancel `ALTER TABLE`'s ACCESS EXCLUSIVE wait
    // under racing app instances) is overridden via the migrator's
    // own pool `after_connect` hook.
    if std::env::args().any(|a| a == "--migrate") {
        dotenvy::dotenv().ok();
        return migrate_all_shards().await;
    }

    dotenvy::dotenv().ok();
    let app = app::App::new().await?;
    app.run().await
}

/// Apply pending `sqlx::migrate!` migrations to every shard primary.
///
/// Reads `MIGRATE_SHARD_URLS` (CSV). Each URL must point at a Postgres
/// primary (e.g. `pg-haproxy:5000` / `pg-haproxy:5001`), NOT pgBouncer
/// — startup `options` are needed and pgBouncer's transaction pool
/// rejects them (see `[[project_pgbouncer_transaction_pool]]`).
///
/// Each pool's connections run `SET lock_timeout = '60s'` and
/// `SET statement_timeout = '60s'` on acquire via `after_connect`,
/// overriding the WP1b 2 s/500 ms database-level defaults for the
/// migration session only. Migrations are idempotent (`IF NOT EXISTS`
/// everywhere) so re-running against an already-current shard is a
/// no-op recorded in `_sqlx_migrations`. Fails loudly on the first
/// shard error — a partial schema is worse than refusing to boot.
async fn migrate_all_shards() -> anyhow::Result<()> {
    use anyhow::Context;
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../db/migrations");

    let urls_csv = std::env::var("MIGRATE_SHARD_URLS")
        .context("MIGRATE_SHARD_URLS env (CSV of shard primary URLs) is required")?;
    let urls: Vec<String> = urls_csv
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if urls.is_empty() {
        anyhow::bail!("MIGRATE_SHARD_URLS contained no entries");
    }

    eprintln!("[migrator] migrating {} shard(s)", urls.len());
    for (idx, url) in urls.iter().enumerate() {
        eprintln!("[migrator] shard {}: connecting", idx);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(10))
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::Executor::execute(
                        conn,
                        "SET lock_timeout = '60s'; SET statement_timeout = '60s'",
                    )
                    .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .with_context(|| format!("shard {} connect", idx))?;

        eprintln!("[migrator] shard {}: applying migrations", idx);
        MIGRATOR
            .run(&pool)
            .await
            .with_context(|| format!("shard {} migration", idx))?;
        eprintln!("[migrator] shard {}: done", idx);
        pool.close().await;
    }
    eprintln!("[migrator] all shards migrated");
    Ok(())
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
