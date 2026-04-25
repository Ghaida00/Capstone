//! Adapters for the `accounts` module.
//!
//! The ONLY layer in the module that may import `sqlx`,
//! `redis`, or other I/O crates. Also the only layer that may
//! touch `crate::cache` / `crate::db` — the shared_kernel
//! facades. Every external interaction that the module needs
//! lives here behind a trait declared in `domain/`.
//!
//! `init()` is the single public wiring entry point; the
//! bootstrap calls it to obtain an [`AccountsDeps`] bundle
//! which is then passed into [`api::router`].

pub(crate) mod repository;

use std::sync::Arc;

use crate::cache::redis::RedisCache;
use crate::db::shard::ShardRouter;

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
/// `bootstrap::build_router`.
///
/// Phase 1 intentionally reaches into `crate::db` and
/// `crate::cache` rather than `crate::shared_kernel` — the
/// move of those facades into `shared_kernel` is a separate
/// migration step (see
/// `docs/architecture/migration-plan.md` §Phase 1 exit
/// criteria). The same call site will keep working afterwards
/// because the facade paths are the only thing that changes.
pub fn init(shards: ShardRouter, cache: RedisCache) -> AccountsDeps {
    let repo: Arc<dyn AccountRepository> =
        Arc::new(SqlxAccountRepository::new(shards));
    let service: DynAccountService = Arc::new(GetBalanceService::new(repo));
    AccountsDeps { service, cache }
}
