# `accounts` — user accounts and balances

Leaf module. Owns the `users` table and the read surface for account
balances. Other modules consume it through `accounts::ports` only.

For module shape see [ADR-0003](../../docs/adr/0003-port-adapter-shape.md).

## Tables owned

- `users` — one row per customer account. Only `SqlxAccountRepository`
  in this crate may read or write it.

## Ports exposed ([`ports.rs`](./src/ports.rs))

- `AccountService` trait
  - `get_balance(id: &AccountId) -> Result<Balance, AccountError>`
- DTOs: `AccountId`, `AccountStatus`, `Balance`
- Errors: `AccountError` (`NotFound` / `Validation` / `Infra`)
- Type alias: `DynAccountService = Arc<dyn AccountService>`

Planned: `create_account`, `credit`, `debit`, `set_status`, plus
`CreateAccount` / `AccountUpdate` DTOs.

## Ports consumed

None. Leaf module.

## Events

Not yet wired. Planned:
- `AccountBalanceChanged { id, delta_cents, new_balance_cents }` — on credit/debit
- `AccountStatusChanged { id, from, to }` — on status transitions

Both will publish through `shared_kernel::events`. Notifications is
the expected consumer.

## HTTP surface

`/api/v2/accounts/{account_number}/balance` — canonical balance read.

## Operational notes

- Balance is `DECIMAL(18,2)` in Postgres and serialised as a **string**
  across the port and HTTP response (JSON numbers don't round-trip
  arbitrary precision). Callers that need arithmetic should parse via
  `rust_decimal::Decimal::from_str`.
- Rows are never hard-deleted. `set_status("blocked")` will be the
  soft-delete path when that use case lands.
- Redis cache key is `balance:<acct>`.
