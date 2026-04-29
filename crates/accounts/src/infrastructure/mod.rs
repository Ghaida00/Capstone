//! Adapters for the `accounts` module.
//!
//! The ONLY layer in the module that may import `sqlx`,
//! `redis`, or other I/O crates. Phase 4 split the kernel into
//! its own crate, so I/O facades come in via
//! `shared_kernel::{db, cache}` rather than crate-local paths.
//! Every external interaction the module needs lives here
//! behind a trait declared in `domain/`.
//!
//! `init()` is the single public wiring entry point; the
//! bootstrap calls it to obtain an [`AccountsDeps`] bundle
//! which is then passed into [`api::router`].

pub(crate) mod repository;

use std::sync::Arc;

use shared_kernel::cache::redis::RedisCache;
use shared_kernel::db::shard::ShardRouter;

use super::application::GetBalanceService;
use super::domain::AccountRepository;
use super::ports::DynAccountService;

use repository::SqlxAccountRepository;

/// Wiring bundle the bootstrap hands to [`super::api::router`].
///
/// In Phase 1 this holds only the service. Adding a cache
/// layer, an event publisher, or a second use-case just grows
/// this struct; the api layer never notices.
#[derive(Clone)]
pub struct AccountsDeps {
    pub service: DynAccountService,
    /// Cache handle used inside the api layer for the
    /// balance-response cache. Kept as a concrete type because
    /// the Redis facade is shared_kernel infrastructure, not a
    /// module-private abstraction.
    pub cache: RedisCache,
}

/// Wire a concrete [`AccountsDeps`] from the application's
/// shared infrastructure. Called once at startup from
/// `app::bootstrap::build_router`.
pub fn init(shards: ShardRouter, cache: RedisCache) -> AccountsDeps {
    let repo: Arc<dyn AccountRepository> = Arc::new(SqlxAccountRepository::new(shards));
    let service: DynAccountService = Arc::new(GetBalanceService::new(repo, cache.clone()));
    AccountsDeps { service, cache }
}
