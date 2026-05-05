//! Shared test helpers for integration and API tests.
//!
//! Provides ephemeral PostgreSQL and Redis containers via `testcontainers`,
//! along with utility functions for building test configs and rollback transactions.

use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

/// Spin up an ephemeral PostgreSQL container, run init.sql, and return the pool + container handle.
///
/// The container is kept alive as long as the returned `ContainerAsync` is held.
pub async fn spawn_postgres() -> (PgPool, ContainerAsync<Postgres>) {
    let container = Postgres::default().start().await.unwrap();
    let host_port = container.get_host_port_ipv4(5432).await.unwrap();

    let url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        host_port
    );

    let pool = PgPool::connect(&url).await.unwrap();

    // Run the init schema
    // Workspace root: Cargo.toml at .. /.. /.. /.. — db/init.sql sits at the repo root.
    let init_sql = include_str!("../../../../db/init.sql");
    sqlx::raw_sql(init_sql).execute(&pool).await.unwrap();

    (pool, container)
}

