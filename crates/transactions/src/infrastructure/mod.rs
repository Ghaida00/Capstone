//! Adapters for the `transactions` module.
//!
//! The ONLY layer that may import `sqlx`, `redis`, or `amqprs`.
//! Everything else in the module sees these capabilities only
//! through trait objects declared in `domain/`.

pub(crate) mod cache_invalidator;
pub(crate) mod cleanup;
pub(crate) mod consumer;
pub(crate) mod cross_shard_processor;
pub(crate) mod publish_outbox;
pub(crate) mod redis_idempotency;
pub(crate) mod redis_intake;
pub(crate) mod repository;

pub use cache_invalidator::spawn_cache_invalidator;
pub use cleanup::spawn_idempotency_cleanup;
pub use consumer::start_consumer;
pub use cross_shard_processor::spawn_cross_shard_processor;
pub use publish_outbox::spawn_publish_outbox;
pub use redis_intake::{spawn_intake_depth_sampler, spawn_redis_intake};

use std::sync::Arc;

use accounts::ports::DynAccountService;
use shared_kernel::cache::redis::RedisCache;
use shared_kernel::db::shard::ShardRouter;

use super::application::TransactionsService;
use super::domain::{IdempotencyAwareWriter, TransactionRepository};
use super::ports::DynTransactionService;

use redis_idempotency::{HybridIdempotencyWriter, RedisIdempotencyWriter};
use repository::{SqlxIdempotencyWriter, SqlxTransactionRepository};

/// Storage backend for the create-time idempotency reservation.
/// The chosen variant decides which `IdempotencyAwareWriter`
/// `init()` constructs and whether the Redis-intake worker needs
/// to be spawned by the binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyBackend {
    /// Synchronous PG INSERT on the request path. Legacy default.
    Pg,
    /// Redis SETNX on the request path; PG INSERT happens
    /// asynchronously via `spawn_redis_intake`.
    Redis,
    /// Try Redis first; fall back to PG on Redis error.
    Hybrid,
}

impl IdempotencyBackend {
    /// Parse a case-insensitive backend name. Returns `Err(unknown)`
    /// containing the offending string if the name doesn't match
    /// any variant.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "pg" => Ok(Self::Pg),
            "redis" => Ok(Self::Redis),
            "hybrid" => Ok(Self::Hybrid),
            other => Err(other.to_string()),
        }
    }
}

impl std::fmt::Display for IdempotencyBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pg => "pg",
            Self::Redis => "redis",
            Self::Hybrid => "hybrid",
        })
    }
}

impl std::str::FromStr for IdempotencyBackend {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Wiring bundle the bootstrap hands to `super::api::router`.
#[derive(Clone)]
pub struct TransactionsDeps {
    pub service: DynTransactionService,
    /// Cache used by the api layer for response caching. Held as
    /// a concrete type because the Redis facade is shared_kernel
    /// infrastructure.
    pub cache: RedisCache,
}

/// Construct concrete deps from the application's shared
/// infrastructure. Called once at startup.
///
/// `backend` selects which `IdempotencyAwareWriter` impl backs the
/// create handler. When the backend is `Redis` or `Hybrid`, the
/// caller is responsible for spawning `spawn_redis_intake` so the
/// Redis-side reservations land in PG and the broker.
///
/// A successful `Reserved` outcome means the queue message will
/// reach the broker; how soon depends on the chosen backend
/// (~50 ms via the publish-outbox worker for `Pg`, single-digit
/// ms via the intake worker + producer for `Redis`).
pub fn init(
    shards: ShardRouter,
    cache: RedisCache,
    accounts: DynAccountService,
    verify_from_account: bool,
    backend: IdempotencyBackend,
) -> TransactionsDeps {
    let repo: Arc<dyn TransactionRepository> =
        Arc::new(SqlxTransactionRepository::new(shards.clone()));

    let idempotency: Arc<dyn IdempotencyAwareWriter> = match backend {
        IdempotencyBackend::Pg => {
            Arc::new(SqlxIdempotencyWriter::new(shards.clone(), cache.clone()))
        }
        IdempotencyBackend::Redis => Arc::new(RedisIdempotencyWriter::new(cache.clone())),
        IdempotencyBackend::Hybrid => Arc::new(HybridIdempotencyWriter::new(
            RedisIdempotencyWriter::new(cache.clone()),
            SqlxIdempotencyWriter::new(shards.clone(), cache.clone()),
        )),
    };

    let service: DynTransactionService = Arc::new(TransactionsService::new(
        repo,
        idempotency,
        accounts,
        shards.clone(),
        verify_from_account,
    ));

    TransactionsDeps { service, cache }
}
