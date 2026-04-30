//! Adapters for the `transactions` module.
//!
//! The ONLY layer that may import `sqlx`, `redis`, or `amqprs`.
//! Everything else in the module sees these capabilities only
//! through trait objects declared in `domain/`.

pub(crate) mod cache_invalidator;
pub(crate) mod cleanup;
pub(crate) mod consumer;
pub(crate) mod cross_shard_processor;
pub(crate) mod publisher;
pub(crate) mod repository;

pub use cache_invalidator::spawn_cache_invalidator;
pub use cleanup::spawn_idempotency_cleanup;
pub use consumer::start_consumer;
pub use cross_shard_processor::spawn_cross_shard_processor;

use std::sync::Arc;

use accounts::ports::DynAccountService;
use shared_kernel::cache::redis::RedisCache;
use shared_kernel::db::shard::ShardRouter;
use shared_kernel::queue::producer::QueueProducer;

use super::application::TransactionsService;
use super::domain::{IdempotencyAwareWriter, TransactionPublisher, TransactionRepository};
use super::ports::DynTransactionService;

use publisher::QueueProducerAdapter;
use repository::{SqlxIdempotencyWriter, SqlxTransactionRepository};

/// Wiring bundle the bootstrap hands to `super::api::router`.
#[derive(Clone)]
pub struct TransactionsDeps {
    pub service: DynTransactionService,
    /// Cache used by the api layer for response caching, mirroring
    /// the legacy handler. Held as a concrete type because the
    /// Redis facade is shared_kernel infrastructure.
    pub cache: RedisCache,
}

/// Construct concrete deps from the application's shared
/// infrastructure. Called once at startup.
///
/// `accounts` is the cross-module port — the
/// `TransactionsService` calls `accounts.get_balance(...)` during
/// create to verify the sender exists. The call is gated by
/// `verify_from_account` so the load-test build can skip the
/// extra Redis hop while production / fail-fast environments
/// keep the 400-on-unknown-sender contract.
pub fn init(
    shards: ShardRouter,
    cache: RedisCache,
    queue_producer: QueueProducer,
    accounts: DynAccountService,
    verify_from_account: bool,
) -> TransactionsDeps {
    let repo: Arc<dyn TransactionRepository> =
        Arc::new(SqlxTransactionRepository::new(shards.clone()));
    let idempotency: Arc<dyn IdempotencyAwareWriter> =
        Arc::new(SqlxIdempotencyWriter::new(shards.clone(), cache.clone()));
    let publisher: Arc<dyn TransactionPublisher> =
        Arc::new(QueueProducerAdapter::new(queue_producer));

    let service: DynTransactionService = Arc::new(TransactionsService::new(
        repo,
        idempotency,
        publisher,
        accounts,
        shards.clone(),
        verify_from_account,
    ));

    TransactionsDeps { service, cache }
}
