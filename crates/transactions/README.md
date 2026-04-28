# `transactions` — money movement

Owns the full write path: HTTP handler → application service →
RabbitMQ producer → consumer → DB write. Depends on `accounts`
through `accounts::ports` only.

For module shape see [ADR-0003](../../docs/adr/0003-port-adapter-shape.md).

## Tables owned

- `transactions` — one row per money-movement request.
- `idempotency_keys` — request-replay protection. Will move to
  `shared_kernel::idempotency` when a second module needs it.

## Ports exposed ([`ports.rs`](./src/ports.rs))

- `TransactionService` trait
  - `create(input)                -> TransactionAccepted`
  - `get_by_id(id)                -> TransactionView`
  - `list(filter)                 -> Vec<TransactionView>`
  - `get_status_by_reference(rid) -> TransactionStatusView`
- DTOs: `TransactionId`, `CreateTransactionInput`, `TransactionAccepted`,
  `TransactionView`, `TransactionStatusView`, `ListFilter`
- Errors: `TransactionError` (`NotFound`, `Validation`,
  `IdempotencyConflict`, `Infra`)
- Type alias: `DynTransactionService`

## Ports consumed

- `accounts::ports::DynAccountService` — `create` calls `get_balance`
  to verify `from_account` exists before reserving the idempotency
  row. Fails fast with 400 for missing accounts.

## Events published

After Step-A consumer rewire, on successful commit:
- `TransactionCommitted` — published from
  [`infrastructure/consumer.rs`](./src/infrastructure/consumer.rs)
  via `shared_kernel::events`

Planned: `TransactionAcceptedEvent`, `TransactionFailedEvent`.

## HTTP surface

```
POST /api/v2/transactions
GET  /api/v2/transactions
GET  /api/v2/transactions/{id}
GET  /api/v2/transactions/status/{reference_id}
```

## Operational notes

- **Idempotency key**: `txn:<shard>:<reference_id>`. The shard prefix
  matters because the same `reference_id` against different `from_account`s
  must NOT collide.
- **Cross-shard reads** (`get_by_id`, `list`, `get_status_by_reference`)
  fan out one query per shard. First-hit wins for `get_by_id`; `list`
  merges and re-sorts.
- **Debit/credit atomicity** is enforced in
  [`infrastructure/consumer.rs`](./src/infrastructure/consumer.rs)
  inside a single transaction.
- **DLQ**: bad-payload messages NACK to `transactions.dead_letter`.
- **Metrics**: `transactions_processed_total`, `transactions_batch_size`,
  `dlq_messages_total`, `events_published_total`.

## Tests

[`tests/event_flow.rs`](./tests/event_flow.rs) — end-to-end:
spin up Postgres + Redis + RabbitMQ via `testcontainers`, POST,
await consumer flush, assert both a `transactions` row and a
`notifications/recent` entry. Requires Docker.
