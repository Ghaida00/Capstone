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
pub(crate) mod repository;

pub use cache_invalidator::spawn_cache_invalidator;
pub use cleanup::spawn_idempotency_cleanup;
pub use consumer::start_consumer;
pub use cross_shard_processor::spawn_cross_shard_processor;
pub use publish_outbox::spawn_publish_outbox;

use std::sync::Arc;

use accounts::ports::DynAccountService;
use shared_kernel::cache::redis::RedisCache;
use shared_kernel::db::shard::ShardRouter;

use super::application::TransactionsService;
use super::domain::{IdempotencyAwareWriter, TransactionRepository};
use super::ports::DynTransactionService;

use repository::{SqlxIdempotencyWriter, SqlxTransactionRepository};

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
/// The service does not hold a queue handle: `create` commits
/// its message into the idempotency outbox and returns 202;
/// broker delivery happens out-of-band in `spawn_publish_outbox`.
pub fn init(
    shards: ShardRouter,
    cache: RedisCache,
    accounts: DynAccountService,
    verify_from_account: bool,
) -> TransactionsDeps {
    let repo: Arc<dyn TransactionRepository> =
        Arc::new(SqlxTransactionRepository::new(shards.clone()));
    let idempotency: Arc<dyn IdempotencyAwareWriter> =
        Arc::new(SqlxIdempotencyWriter::new(shards.clone(), cache.clone()));

    let service: DynTransactionService = Arc::new(TransactionsService::new(
        repo,
        idempotency,
        accounts,
        shards.clone(),
        verify_from_account,
    ));

    TransactionsDeps { service, cache }
}
