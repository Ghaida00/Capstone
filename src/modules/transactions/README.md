# `transactions` — money movement (module **A**, depends on `accounts`)

> **Phase 2.** The module is wired and serves all four endpoints
> under `/api/v2/transactions/*`. The legacy `/api/v1/transactions/*`
> handlers in [`src/api/handlers.rs`](../../api/handlers.rs) remain
> live; both paths share idempotency rows, Redis cache keys, and the
> RabbitMQ queue, so a v2 POST and a v1 GET interoperate seamlessly.
>
> **Read first:**
> [../../../docs/architecture/phase2-transactions-walkthrough.md](../../../docs/architecture/phase2-transactions-walkthrough.md)
> — file-by-file walkthrough mirroring the Phase 1 doc.

## 1. What this module is for

Owns money movement between accounts: submission, lifecycle
(`pending` → `processing` → `completed` / `failed` / `reversed`),
the `transactions` table, and `idempotency_keys`. Wraps the
RabbitMQ `peakload.transactions` exchange behind a domain port so
the application layer never imports `amqprs`.

## 2. Tables owned

- `transactions`        — one row per money-movement request.
- `idempotency_keys`    — request-replay protection. Lives here
                          because the create use case is the only
                          producer; will move to
                          `shared_kernel::idempotency` once a
                          second module needs it.

No other module SELECTs or UPDATEs these tables.

## 3. Ports exposed

[`ports.rs`](./ports.rs) exposes:

- `TransactionService` trait:
  - `create(input)               -> TransactionAccepted`
  - `get_by_id(id)               -> TransactionView`
  - `list(filter)                -> Vec<TransactionView>`
  - `get_status_by_reference(rid)-> TransactionStatusView`
- DTOs: `TransactionId`, `CreateTransactionInput`,
  `TransactionAccepted`, `TransactionView`,
  `TransactionStatusView`, `ListFilter`.
- Errors: `TransactionError` (`NotFound`, `Validation`,
  `IdempotencyConflict`, `Infra`).
- Type alias: `DynTransactionService`.

## 4. Ports consumed

- `accounts::ports::DynAccountService` — **injected and
  exercised**. `TransactionsService::create` calls
  `accounts.get_balance(...)` to verify `from_account` exists
  before reserving the idempotency row. First behavioural
  divergence from v1: v2 fails fast with a 400 if the sender
  is missing; v1 accepts and lets the consumer surface a
  `failed` row downstream. See
  [phase2 walkthrough §6.2](../../../docs/architecture/phase2-transactions-walkthrough.md).

## 5. Events published

**None in Phase 2.** Planned for Phase 3 (when the event bus
lands in `shared_kernel`):

- `TransactionAcceptedEvent`  — emitted on successful `create`.
- `TransactionCommittedEvent` — emitted by the consumer after
                                 a successful balance update.
- `TransactionFailedEvent`    — emitted on terminal failure.

## 6. Events consumed

**None.** A future iteration may consume
`accounts::AccountStatusChanged` to fail in-flight transactions
against newly-blocked accounts.

## 7. Operational notes

- **Idempotency is keyed by `txn:<shard>:<reference_id>`**; the
  shard prefix matters because the same `reference_id` against
  different `from_account`s must NOT collide.
- **The Redis fast-path** mirrors the legacy handler — same
  cache key, same 24h TTL — so v1 and v2 see each other's
  reservations.
- **Cross-shard reads** (`get_by_id`, `list`,
  `get_status_by_reference`) fan out N parallel queries (one per
  shard). The first hit wins for `find_by_id`; `list` merges and
  resorts.
- **Debit/credit atomicity** is NOT a property of this module
  — it lives in the consumer (`src/queue/consumer.rs`), which
  has not yet been rewired. Phase 2 follow-up.

## 8. Intentional gaps (Phase 2)

| Gap                                                       | Tracked in                                                   |
|-----------------------------------------------------------|--------------------------------------------------------------|
| Queue consumer not rewired through this module's port     | walkthrough §6.1 + migration-plan §Phase 2 exit              |
| No middleware parity on `/api/v2/*`                       | Phase 1 README; same gap                                     |
| Legacy v1 handlers still live                             | Phase 1 README; same gap                                     |
| `idempotency_keys` ownership will move to shared_kernel   | migration-plan Phase 3+                                       |
| No new integration tests                                  | walkthrough §6.4                                              |
