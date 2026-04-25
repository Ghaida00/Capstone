//! Adapters for the `transactions` module.
//!
//! The ONLY layer that may import `sqlx`, `redis`, or `amqprs`.
//! Everything else in the module sees these capabilities only
//! through trait objects declared in `domain/`.

pub(crate) mod publisher;
pub(crate) mod repository;

use std::sync::Arc;

use crate::cache::redis::RedisCache;
use crate::db::shard::ShardRouter;
use crate::queue::producer::QueueProducer;

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
    /// Redis facade is shared infrastructure (future
    /// shared_kernel::cache).
    pub cache: RedisCache,
}

/// Construct concrete deps from the application's shared
/// infrastructure. Called once at startup.
///
/// `accounts` is the cross-module port — the
/// `TransactionsService` calls
/// `accounts.get_balance(...)` during create to verify the
/// sender exists. This is the modular-monolith
/// dependency-injection seam in active use.
pub fn init(
    shards: ShardRouter,
    cache: RedisCache,
    queue_producer: QueueProducer,
    accounts: crate::modules::accounts::ports::DynAccountService,
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
    ));

    TransactionsDeps { service, cache }
}
