//! Sqlx-backed implementation of [`AccountRepository`].
//!
//! This is the ONLY file in the `accounts` module that writes
//! SQL against the `users` table. Any other module that needs
//! balance data must call through
//! [`ports::AccountService`](super::super::ports::AccountService)
//! — direct SELECTs against this table from outside the
//! module are a review blocker.

use async_trait::async_trait;
use rust_decimal::Decimal;
use sqlx::FromRow;

use crate::db::failover::retry_transient;
use crate::db::shard::ShardRouter;

use super::super::domain::{Account, AccountRepository};
use super::super::ports::{AccountId, AccountStatus};

/// Minimal row projection — we only read what the port DTO
/// and the domain entity need. Adding columns here is safe;
/// removing them is a migration.
#[derive(Debug, FromRow)]
struct UsersRow {
    account_number: String,
    full_name: String,
    email: Option<String>,
    balance: Decimal,
    status: String,
}

/// Concrete repo. Holds a clone of the shard router rather
/// than an `Arc<...>` because `ShardRouter` is already `Clone`
/// (cheap — internal `Arc`s).
pub(crate) struct SqlxAccountRepository {
    shards: ShardRouter,
}

impl SqlxAccountRepository {
    pub(crate) fn new(shards: ShardRouter) -> Self {
        Self { shards }
    }
}

#[async_trait]
impl AccountRepository for SqlxAccountRepository {
    async fn find_active_by_id(
        &self,
        id: &AccountId,
    ) -> Result<Option<Account>, String> {
        let shard = ShardRouter::shard_for(id.as_str());
        let account_number = id.as_str().to_owned();
        let router = &self.shards;

        // Reads are idempotent — retry transient connection
        // errors over the read-replica pool. Matches the
        // semantics of the legacy handler in
        // src/api/handlers::get_balance.
        let row: Option<UsersRow> = retry_transient(
            || {
                let account_number = account_number.clone();
                async move {
                    sqlx::query_as::<_, UsersRow>(
                        r#"
                        SELECT account_number, full_name, email, balance, status
                        FROM users
                        WHERE account_number = $1
                          AND status = 'active'
                        "#,
                    )
                    .bind(&account_number)
                    .fetch_optional(router.reader(shard))
                    .await
                }
            },
            2,
            20,
            "accounts.find_active_by_id",
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok(row.map(|r| {
            Account {
                id: AccountId(r.account_number),
                full_name: r.full_name,
                email: r.email,
                amount_str: r.balance.to_string(),
                currency: "IDR".to_string(),
                // DB-level CHECK constraint guarantees the
                // status is one of the three known values;
                // anything else means the DB is ahead of the
                // code and we fail safely as `Blocked`.
                status: match r.status.as_str() {
                    "active" => AccountStatus::Active,
                    "inactive" => AccountStatus::Inactive,
                    _ => AccountStatus::Blocked,
                },
            }
        }))
    }
}
