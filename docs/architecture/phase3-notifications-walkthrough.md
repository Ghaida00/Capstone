# Phase 3 Walkthrough — The `notifications` Module

> **Audience:** anyone who already read
> [phase1-accounts-walkthrough.md](./phase1-accounts-walkthrough.md)
> and [phase2-transactions-walkthrough.md](./phase2-transactions-walkthrough.md).
> Phase 3 reuses the same module shape; this doc focuses on what
> is **different** — the cross-module event bus, the long-running
> dispatch task, and the deliberately-narrow read API.
>
> **Post-Phase-4 note**: this walkthrough was written when the
> kernel + module lived at `src/shared_kernel/` +
> `src/modules/notifications/`. After Phase 4 the same files live
> at `crates/shared_kernel/src/` + `crates/notifications/src/`.
> The event-bus contract (two trait objects) is unchanged. See
> [phase4-workspace-walkthrough.md](./phase4-workspace-walkthrough.md).

---

## 1. What is new vs. Phases 1 / 2

The shape inside the module
(`ports / domain / application / infrastructure / api / mod.rs`)
is identical to the other two. The interesting deltas:

1. **No inbound or outbound module dependencies.** Grep proves it:
   ```
   $ rg 'use crate::modules' src/modules/notifications
   (no matches)
   ```
   This is the canonical "event-consuming" module described in
   [modular-monolith.md](./modular-monolith.md). Coupling to the
   rest of the app happens **only** through the kernel-owned
   `Event` envelope, never through a Rust type from another
   module.
2. **Subscribes to `shared_kernel::events`.** The Phase 3
   submodule [`shared_kernel::events`](../../src/shared_kernel/events.rs)
   provides a neutral event bus — see §2 below.
3. **Long-running dispatcher task.** Other modules expose only
   request/response handlers. This one additionally spawns a
   tokio task that drains the broadcast channel for the lifetime
   of the process.
4. **In-memory store.** The Phase 3 implementation uses a bounded
   `VecDeque<NotificationEntry>` ring buffer instead of a
   database table. The future `notification_log` table will live
   alongside the in-memory impl as a sibling
   `infrastructure/repository.rs`, selected at `init` time. Both
   satisfy the same `domain::NotificationStore` trait, so the
   application layer is unaware of which is in play.

## 2. The event bus

Two trait-object views on a single
`tokio::sync::broadcast` channel:

- `EventPublisher::publish(&self, Event) -> Result<usize, String>`
- `EventSubscriber::subscribe(&self) -> broadcast::Receiver<Event>`

The bootstrap creates one `InProcessEventBus`, clones it, and
hands one Arc out as each trait. Publishers cannot subscribe;
subscribers cannot publish. That separation keeps the
dependency graph honest.

The envelope:

```rust
struct Event {
    id: Uuid,
    name: String,                  // e.g. "transactions.committed"
    occurred_at: DateTime<Utc>,
    payload: serde_json::Value,    // opaque to the bus
}
```

The payload is a `serde_json::Value` on purpose — the publisher
serialises whatever struct it owns, the subscriber deserialises
into whatever struct **it** owns. The two structs do not need to
share a Rust definition. The boundary between them is the wire
format, not a type.

Today the transport is in-process. The trait split is the swap
surface for an AMQP-backed impl in Phase 4 / 5; nothing in
either module changes when the swap happens, only the constructor
in `app.rs`.

## 3. Request / event flow

### 3.1 `transactions.committed` publication

```
  POST /api/v2/transactions
        │
        ▼
  transactions::application::TransactionsService::create
        │   (validates, reserves idempotency, publishes to RabbitMQ)
        ▼
  RabbitMQ queue "transactions.process"
        │
        ▼
  src/queue/consumer.rs   — BatchTransactionConsumer
        │   (buffers messages, flushes in batches of 50 / 100 ms)
        ▼
  flush_batch_to_shards   (DEBIT sender, CREDIT receiver, INSERT row)
        │
        │ Ok(_) — durably committed in Postgres
        ▼
  publish_committed_events(&batch, &events)
        │   one Event per row, name = "transactions.committed"
        ▼
  shared_kernel::events::InProcessEventBus
        │   (broadcast::Sender::send)
        ▼
  every active subscriber receives a clone of the Event
```

### 3.2 `notifications` consumption

```
  notifications::infrastructure::init
        │   spawns notifications::application::EventDispatcher
        ▼
  EventDispatcher loop
        │   tokio::select! over (rx.recv, cancel.cancelled)
        ▼
  map_event_to_entry   (filters on event.name)
        │   "transactions.committed" → NotificationEntry
        ▼
  domain::NotificationStore::append
        │   (in-memory ring buffer — capacity 512)
        ▼
  metrics::counter!("notifications_appended_total").increment(1)
```

### 3.3 `GET /api/v2/notifications/recent`

```
  GET /api/v2/notifications/recent?limit=50
        │
        ▼
  notifications::api::handlers::recent
        │
        ▼
  ports::NotificationLog::recent(limit)
        │   (clamped to MAX_RECENT = 200)
        ▼
  application::NotificationLogService → store.recent(N)
        │
        ▼
  Json<ApiResponse<Vec<NotificationEntry>>>
```

## 4. Files, in the order you should read them

1. [`shared_kernel/events.rs`](../../src/shared_kernel/events.rs)
   — the bus, traits, envelope. Read first because every other
   file references it.
2. [`notifications/ports.rs`](../../src/modules/notifications/ports.rs)
   — the public contract: `NotificationKind`, `NotificationEntry`,
   `NotificationError`, `NotificationLog`. The api layer and any
   future caller import from this file and nothing else inside
   the module.
3. [`notifications/domain/mod.rs`](../../src/modules/notifications/domain/mod.rs)
   — the module-private deserialise target
   (`TransactionCommittedDomainEvent`) and the
   `NotificationStore` trait. **No I/O imports.**
4. [`notifications/application/mod.rs`](../../src/modules/notifications/application/mod.rs)
   — `EventDispatcher` (the long-running task) plus
   `NotificationLogService` (the read-side service satisfying
   `ports::NotificationLog`). The event-name filter
   (`map_event_to_entry`) lives here.
5. [`notifications/infrastructure/mod.rs`](../../src/modules/notifications/infrastructure/mod.rs)
   — `init(subscriber, cancel) -> (NotificationsDeps, JoinHandle)`.
   Builds the in-memory store, wires the service, spawns the
   dispatcher.
6. [`notifications/infrastructure/store.rs`](../../src/modules/notifications/infrastructure/store.rs)
   — the `InMemoryNotificationStore` (`VecDeque` + `RwLock`).
7. [`notifications/api/mod.rs`](../../src/modules/notifications/api/mod.rs)
   + [`api/handlers.rs`](../../src/modules/notifications/api/handlers.rs)
   — one route, one handler.
8. [`bootstrap.rs`](../../src/bootstrap.rs) — the `init` call,
   the `nest_service` mount, and the
   `apply_protection_stack` wrapping (auth, rate limit, circuit
   breaker, backpressure all apply, identical to the other v2
   sub-routers).
9. [`app.rs`](../../src/app.rs) — where the `InProcessEventBus`
   is created, split into publisher + subscriber, and threaded
   into the consumer + bootstrap.

## 5. Compile-time invariants you can prove

After Phase 3, the dependency seal is verifiable:

```bash
# 1. notifications imports nothing from other modules:
rg 'use crate::modules' src/modules/notifications
#   → no matches.

# 2. notifications imports kernel only via shared_kernel::events:
rg 'use crate::shared_kernel' src/modules/notifications
#   → only EventSubscriber + Event references.

# 3. shared_kernel imports nothing from any module:
rg 'use crate::modules' src/shared_kernel
#   → no matches. (It must NEVER produce a match.)
```

That third grep is the one to wire into CI before Phase 4:
adding a `crate::modules::*` import inside `shared_kernel/`
breaks the kernel's neutrality, which breaks the entire
modular-monolith story.

## 6. What was deliberately left for later

Phase 3 took the shortest path that proves the shape. Tracked in
[migration-plan.md §Phase 3](./migration-plan.md):

1. **In-memory store, not a `notification_log` table.** Restarts
   lose history. Sibling sqlx repository is a future PR.
2. **Single event kind subscribed.** `transactions.committed`
   only. `accounts::AccountStatusChanged` and
   `accounts::AccountBalanceChanged` (also planned per the README)
   are not yet published — `accounts` does not have a write path
   today.
3. **No dispatch channels.** Email / push / banner adapters are
   not implemented; the dispatcher writes to the log only.
4. **No subscriptions table.** Opt-in / opt-out is not modelled.
5. **Bus is in-process, not AMQP.** A second binary (Phase 5
   service extraction) cannot subscribe to the same bus until
   the AMQP-backed impl lands.
6. **No automated event-smoke test.** A Postgres + RabbitMQ
   testcontainer test that posts a transaction and asserts a
   matching `notifications/recent` entry shows up is the next
   reasonable add.

If you are picking up Phase 3 follow-ups: start with #1 (the
persistent log) — it has the largest payoff for the smallest
diff and unlocks the audit / replay use cases.

## 7. How this lines up with Phase 4

The `apply_protection_stack` helper, the `init`-returns-deps
shape, and the `nest_service` mount all match the other v2
sub-routers, so Phase 4 (workspace crate split) treats
`notifications` identically to `accounts` and `transactions`:

- `crates/notifications/Cargo.toml` depends on
  `shared_kernel` only.
- `crates/notifications/src/lib.rs` re-exports `ports::*` and
  `init` and `router` — same surface this module exposes today.
- The `crates/app` binary's bootstrap function is the only
  caller of `init`.

Because the seal is real today, Phase 4 should be a Cargo-only
change with no Rust edits inside `src/modules/notifications/`.
