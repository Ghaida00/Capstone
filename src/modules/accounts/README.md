# `accounts` — user accounts and balances (module **B**, leaf)

> **Phase 1 partial.** The module is wired and serving
> `/api/v2/accounts/{account_number}/balance`, alongside the legacy
> `/api/v1/users/{account_number}/balance` which remains the
> production source of truth. Both paths read the same `users` table
> and return identical JSON; the v2 path routes through this module's
> `AccountService`.
>
> **Read the walkthrough first:**
> [../../../docs/architecture/phase1-accounts-walkthrough.md](../../../docs/architecture/phase1-accounts-walkthrough.md)
> — file-by-file tour with a request-flow diagram.

## 1. What this module is for

Owns everything related to customer accounts: the `users` table, the
current balance, status (`active` / `inactive` / `blocked`), and the
domain rules governing reads today (status-gated balance lookups) and
transitions later (credit / debit / block).

This module does **not** know about transactions. When
`transactions` is extracted (migration Phase 2) it will inject
`Arc<dyn AccountService>` and call into this module through
`ports.rs` — never by querying the `users` table directly.

## 2. Tables owned

- `users` — one row per customer account. Only `SqlxAccountRepository`
  in this module may read or write it.

## 3. Ports exposed

**Available today** in [`ports.rs`](./ports.rs):

- `AccountService` trait:
  - `get_balance(id: &AccountId) -> Result<Balance, AccountError>`
- DTOs: `AccountId`, `AccountStatus`, `Balance`.
- Errors: `AccountError` (`NotFound` / `Validation` / `Infra`).
- Type alias: `DynAccountService = Arc<dyn AccountService>`.

**Planned for Phase 2**:

- `create_account`, `credit`, `debit`, `set_status`.
- Related DTOs: `CreateAccount`, `AccountUpdate`.

## 4. Ports consumed

**None.** This module is a leaf of the module graph.

## 5. Events published

**None in Phase 1.** Planned once the event bus lands:

- `AccountBalanceChanged { id, delta_cents, new_balance_cents }`
  — emitted on successful `credit` / `debit`. Consumer:
  `notifications` (Phase 3).
- `AccountStatusChanged { id, from, to }` — emitted on status
  transitions.

## 6. Events consumed

**None.** The module has no subscribers.

## 7. Operational notes

- Balance is stored as `DECIMAL(18,2)` in Postgres and serialised
  as a **string** (`"1234.56"`) across the port and HTTP response
  because JSON numbers do not round-trip arbitrary precision.
  Callers that need arithmetic should parse this themselves via
  `rust_decimal::Decimal::from_str`.
- A row is never hard-deleted. `set_status("blocked")` will be
  the soft-delete path when that use case lands.
- The v2 endpoint reuses the v1 Redis cache key (`balance:<acct>`)
  so both paths populate and hit the same cached entries —
  smoothing any future cutover.

## 8. Intentional gaps (Phase 1 partial)

These will be closed before the legacy endpoint is retired:

| Gap                                              | Tracked in                                                   |
|--------------------------------------------------|--------------------------------------------------------------|
| No middleware parity on `/api/v2/*` (rate-limit, auth, circuit breaker, backpressure) | `bootstrap.rs` comment + migration-plan §Phase 1 exit        |
| Legacy handler still live                        | migration-plan §Phase 1 exit criteria                        |
| No integration test hitting the v2 path          | migration-plan §Phase 1 exit criteria                        |
| Events not yet wired                             | migration-plan §Phase 2 / Phase 3                            |
| Cross-module dep enforcement is convention-only  | migration-plan §Phase 4 (workspace crate split)              |
