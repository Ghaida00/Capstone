# Phase 2 Walkthrough — The `transactions` Module

> **Audience:** anyone who already read
> [phase1-accounts-walkthrough.md](./phase1-accounts-walkthrough.md).
> Phase 2 follows the same shape; this doc focuses on what is
> **different** — the cross-module dependency, the queue port, and
> the idempotency dance.

## 1. The new pieces vs. accounts

The shape (`ports / domain / application / infrastructure / api /
mod.rs`) is identical. The interesting deltas:

1. **A second outbound port (`TransactionPublisher`)** in
   `domain/`. Idiomatic dependency inversion: the application
   layer says "I publish a TransactionCreated message" without
   knowing anything about RabbitMQ.
2. **An idempotency port (`IdempotencyAwareWriter`)**. The
   create-time idempotency dance reads + writes `idempotency_keys`
   AND the Redis cache, spanning two infrastructure systems —
   a perfect candidate for a port that hides the choreography.
3. **A cross-module injection** in `infrastructure::init`:
   ```rust
   pub fn init(
       shards: ShardRouter,
       cache: RedisCache,
       queue_producer: QueueProducer,
       _accounts: accounts::ports::DynAccountService,   // ◄── here
   ) -> TransactionsDeps
   ```
   The `_accounts` parameter is reserved on purpose. Phase 2
   does NOT yet exercise it because the legacy create handler
   does not validate `from_account` exists; adding that check
   would change behaviour. The seam exists; flipping it on is
   one line in a future PR.
4. **Four use-case methods on one service struct** rather than
   four structs. Their dependency sets are subsets of one
   another; splitting at the trait method level matches the
   port shape that the rest of the codebase will inject. We
   split into per-use-case structs only when a method's deps
   diverge.

## 2. Request flow — `POST /api/v2/transactions`

```
  HTTP request
        │
        ▼
  axum Router  (bootstrap.rs)
        │ .nest_service("/api/v2/transactions", transactions_router)
        ▼
  transactions::api::handlers::create
        │
        │ 1. Parse CreateRequest → CreateTransactionInput (port DTO)
        │ 2. Call deps.service.create(input)
        ▼
  transactions::application::TransactionsService::create
        │
        │ 1. Validate (amount, accounts, reference_id, currency)
        │ 2. Compute shard, idempotency_key, request_hash
        │ 3. Build TransactionAccepted response payload
        │
        │ 4. self.idempotency.reserve(...) ──────────┐
        │                                              ▼
        │                  infrastructure::SqlxIdempotencyWriter
        │                              ├─ Redis fast-path Replay?
        │                              ├─ INSERT idempotency_keys (ON CONFLICT)
        │                              ├─ Same hash → Replay
        │                              ├─ Different hash → HashConflict
        │                              └─ Failed/expired → revive → Reserved
        │
        │ ◄─────────────────── ReserveOutcome
        │
        │ 5. If Reserved: self.publisher.publish_created(payload)
        │                                              │
        │                                              ▼
        │                  infrastructure::QueueProducerAdapter
        │                              └─ amqprs publish to peakload.transactions
        │
        │ 6. Return TransactionAccepted
        ▼
  api::handlers maps to AcceptedResponse → ApiResponse → HTTP 202
```

The application layer **owns** the choreography; the
infrastructure ports own the I/O details. Swap RabbitMQ for SNS
tomorrow → only `infrastructure/publisher.rs` changes.

## 3. The four use-case methods

| Method                        | Pattern                            | Cache     |
|-------------------------------|------------------------------------|-----------|
| `create`                      | Idempotency + publish              | 24 h TTL  |
| `get_by_id`                   | Cross-shard fan-out, first hit     | 5 min     |
| `list`                        | Cross-shard fan-out, merge + sort  | 1 s       |
| `get_status_by_reference`     | Cross-shard fan-out, first hit     | 60 s      |

All four are byte-compatible with the legacy v1 endpoints —
same JSON shapes, same Redis keys, same TTLs.

## 4. Why the publisher is a trait, not a `QueueProducer` field

Consider the alternative:

```rust
struct TransactionsService {
    repo: Arc<dyn TransactionRepository>,
    queue: QueueProducer,  // ◄── concrete type
}
```

That would force `application/` to import `crate::queue::producer`
(an `amqprs` user) and tightly couple the use case to the
specific message bus. Tests would need a real RabbitMQ or a
fake-`QueueProducer` factory.

The trait version (`Arc<dyn TransactionPublisher>`) lets us:

- Inject a fake in unit tests with three lines of code.
- Swap the message bus by writing a new adapter.
- Lift this module into its own service later without dragging
  the AMQP client along.

The cost is one trait declaration in `domain/` + one tiny
adapter in `infrastructure/publisher.rs`. Worth it.

## 5. The idempotency port — why it exists at this level

The create-time idempotency dance is **infrastructure** because
it spans Postgres + Redis. But it is also **business-relevant**
because the application layer needs to know whether to publish
or replay. So the port lives at the domain boundary returning a
business-shaped outcome:

```rust
enum ReserveOutcome {
    Reserved,                    // Caller should publish.
    Replay(serde_json::Value),   // Caller should return cached.
    HashConflict,                // Caller should 400.
}
```

The application layer pattern-matches on this and decides; the
infrastructure layer handles all the SQL + Redis details.

## 6. Phase 2 follow-ups (not done in this iteration)

These are the gaps documented in the module README. They are
intentional, scoped, and tracked.

### 6.1 Queue consumer rewire

The consumer in `src/queue/consumer.rs` still:

- Reads `QueuePayload` directly from the queue.
- Writes to the `transactions` and `users` tables itself via
  raw `sqlx::query(...)` calls.
- Does the cross-shard debit/credit dance with its own logic.

A clean rewire moves all that DB work into a new use case,
e.g. `transactions::application::ProcessBatch`, exposed
through a new port method like
`TransactionService::process_batch(messages) -> Result<...>`,
backed by a sqlx implementation in `infrastructure/`. The
consumer becomes a thin AMQP adapter that calls into the
service.

The reason it was deferred: 446 lines of consumer logic
including batched cross-shard transactions, error handling,
DLQ semantics, and graceful shutdown. Doing it justice would
double the size of this iteration.

The consumer continues to work today because the queue
**message shape** is unchanged: the new `QueueProducerAdapter`
serialises the exact JSON the legacy producer sent, which is
what the consumer expects.

### 6.2 Cross-module dep activation — **DONE**

The `accounts` port is now actively called inside
`TransactionsService::create`:

```rust
match self.accounts
    .get_balance(&AccountId(input.from_account.clone())).await
{
    Ok(_) => {}
    Err(AccountError::NotFound(_)) => {
        return Err(TransactionError::Validation(format!(
            "from_account {} does not exist or is not active",
            input.from_account
        )));
    }
    Err(AccountError::Validation(m)) => {
        return Err(TransactionError::Validation(m));
    }
    Err(AccountError::Infra(m)) => {
        return Err(TransactionError::Infra(m));
    }
}
```

This is the **first behavioural divergence between v1 and v2**:

- `/api/v1/transactions` queues blindly, the consumer
  discovers the missing sender at debit time and emits a
  `failed` row.
- `/api/v2/transactions` 400s up front with a clear message.

That divergence is intentional and improves the contract;
when v1 is retired, v2's behaviour becomes canonical. Until
then, integration tests that POST to v2 must seed a `users`
row first (or use an existing seeded one).

**Compile-time invariant proven**: a grep over the
`transactions` module shows the only `accounts` import is its
`ports`, satisfying
[dependency-rules.md](./dependency-rules.md) Rule 3:

```
$ rg 'use crate::modules::accounts' src/modules/transactions
src\modules\transactions\application\mod.rs:11: ... accounts::ports::{...}
src\modules\transactions\infrastructure\mod.rs:46: ... accounts::ports::DynAccountService
```

No `accounts::domain`, no `accounts::infrastructure`, no
`accounts::api`. The seal holds; the dep is real.

### 6.3 Middleware parity

Same gap as Phase 1 — v2 routes skip auth/rate-limit/circuit-
breaker/backpressure. Will be addressed when a single
middleware-stack helper lands in `bootstrap.rs`.

### 6.4 Integration tests

No new tests added. `cargo check` proves the shape compiles;
end-to-end coverage against a Postgres + RabbitMQ testcontainer
is the natural next step.

## 7. Compile-time invariants you can prove

After this phase, you can verify the dependency rules concretely:

```bash
# 1. accounts has no module-level dependency on transactions:
rg 'use crate::modules::transactions' src/modules/accounts
#   → no matches.

# 2. transactions depends on accounts ONLY through ports:
rg 'use crate::modules::accounts' src/modules/transactions
#   → only `accounts::ports::*` lines.

# 3. neither module imports legacy paths beyond shared_kernel-eq:
rg 'use crate::api::|use crate::db::models|use crate::error' src/modules
#   → only the legitimate cross-cuts (AppError bridge, ApiResponse,
#     ShardRouter, RedisCache, QueueProducer). All future Phase 4
#     work converts these into shared_kernel imports.
```

Those greps are the manual version of the lint check that
Phase 4 (workspace crate split) will hoist into the compiler.

## 8. What you have now, end-to-end

Two real modules wired:

- `accounts` — leaf, exposes `AccountService` (1 method).
- `transactions` — depends on `accounts::ports::DynAccountService`
  (injected, not yet exercised), exposes `TransactionService`
  (4 methods).

Both serve under `/api/v2/*`, both compile clean, both share
caches and idempotency rows with their v1 counterparts.

The next sensible step is closing one of the documented gaps:
the smallest ROI win is the cross-module dep activation (§6.2);
the largest is the consumer rewire (§6.1). Either one builds
real confidence that the modular shape holds at production
load — the current state is "the compiler likes it, integration
testing is up to you".
