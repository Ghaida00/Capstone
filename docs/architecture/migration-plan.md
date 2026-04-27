# Migration Plan — Monolith → Modular Monolith → Microservices

The current codebase is a working monolith. This document maps the
incremental path to modular monolith first, and then to
per-module microservices extraction **when and only when** the load
profile justifies it.

Each phase is individually shippable: the app must stay running,
tests must stay green, and the monolith must keep serving traffic
throughout. There is no big-bang cutover at any point.

---

## Phase 0 — Groundwork (this iteration)

**Already done**:

- `docs/architecture/` with the four design docs.
- `src/modules/` skeleton with `_template/` and the three target
  module directories (`accounts/`, `transactions/`, `notifications/`)
  as empty stubs.
- `src/shared_kernel/` README placeholder.
- Dependency rules, visibility conventions, and the file-by-file
  shape documented.

**Deliberately NOT done**:

- No production code has moved into `src/modules/`.
- Neither `lib.rs` nor `bootstrap.rs` has any reference to the new
  tree — the compiler sees the groundwork as dead code until we
  wire it in.
- No workspace split. Still one crate. Still one binary.

**Exit criteria**: the team can read the design docs and understand
where future code will live, and a PR creating the first real module
(Phase 1) has a clear template to copy.

---

## Phase 1 — First real module: `accounts`

Move the account-related logic out of the existing flat layout into
`src/modules/accounts/`. This is the **proof-of-shape** phase: we
use `accounts` because it is a leaf module (no outbound module
dependencies) and therefore the cleanest first extraction.

Scope:

1. **Identify the account code** currently scattered across:
   - `src/api/handlers.rs` — handlers that deal with account
     endpoints.
   - `src/api/models.rs` or similar — request / response types.
   - `src/db/` — any account-specific queries or types.
2. **Copy** (don't move — we want to git-blame the original)
   into the module:
   - `domain/` — the `Account`, `Balance`, `AccountStatus` types
     plus any pure business rules.
   - `application/` — one use case per externally-invoked operation
     (`get_balance`, `create_account`, …).
   - `infrastructure/repository.rs` — sqlx queries against the
     `users` table.
   - `api/handlers.rs` — new handler implementations that call
     application services.
3. **Write `ports.rs`** with `AccountService` + DTOs + `AccountError`.
4. **Wire** into `bootstrap.rs`:
   - Build the `AccountService` implementation via
     `accounts::infrastructure::init`.
   - Mount `accounts::api::router` under its HTTP prefix.
5. **Cut over** the original endpoints to the new router in the
   top-level router. Delete the old inline handler code.
6. **Smoke-test**: same contract, same k6 numbers, same test suite
   passing.

Exit criteria:

- `src/modules/accounts/` is the only place account logic lives.
- Nothing in `src/api/` or `src/db/` still references account tables
  directly.
- A failing `cargo build` on a forbidden import (e.g. "what if I
  import `accounts::domain::Account` from `src/api/`?") confirms the
  seal.
- Integration tests against the `accounts` HTTP surface all pass.
- **Middleware parity**: the v2 sub-router carries the same
  protection stack (auth → rate-limit → circuit-breaker →
  backpressure) as v1. ✅ done — see `apply_protection_stack` in
  [bootstrap.rs](../../src/bootstrap.rs); a v2 client experiences
  identical 401/429/503 semantics to v1.

**Time estimate**: 1–2 days for a developer familiar with the
codebase.

**Phase 1 partial — what is still missing for full Phase 1**:

- Cutover step 5 (delete the legacy `/api/v1/users/.../balance`
  handler and route v1 at the new module if URL stability is
  required) is still pending. The two paths currently coexist by
  design.

---

## Phase 2 — Second module: `transactions`

`transactions` is the meatier test because it has a **dependency on
`accounts::ports::AccountService`**. This phase validates that the
port/injection story works end-to-end.

Scope:

1. **Move transaction code** analogously to Phase 1.
2. **Declare the dependency**:
   - `transactions::application::TransferMoney` takes
     `Arc<dyn AccountService>` as a constructor arg.
   - `transactions::infrastructure::init(state, account_service)`
     threads it in.
3. **Bootstrap wires the graph**: `let acct = accounts::init(...); let tx = transactions::init(..., acct.clone());`.
4. **Keep existing queue consumer working** during the move:
   `src/queue/consumer.rs` temporarily becomes a thin shim that
   calls the new `transactions::ports::TransactionService`.

Exit criteria:

- Same as Phase 1 for `transactions`.
- The dependency between the two modules is an explicit
  `Arc<dyn AccountService>` visible in the bootstrap graph — no
  hidden global state.
- **Middleware parity**: same protection stack as v1, applied via
  the shared `apply_protection_stack` helper. ✅ done — both v2
  sub-routers go through the identical layer chain.

**Key test**: change an internal of `accounts::infrastructure` (e.g.
swap SELECT shape) without touching `transactions` and watch only
`accounts` rebuild. This is the user-requested invariant made
tangible.

**Time estimate**: 2–3 days.

---

## Phase 3 — Third module: `notifications`

Implements the **event-driven, independent module** story.

Scope:

1. ✅ `shared_kernel::events` gets a first real type — see
   [src/shared_kernel/events.rs](../../src/shared_kernel/events.rs).
   The kernel exports a neutral `Event` envelope plus
   `EventPublisher` / `EventSubscriber` traits and an
   `InProcessEventBus` impl backed by `tokio::sync::broadcast`.
2. ✅ `transactions.committed` events are published after a
   successful transfer. Phase 3 publishes from the queue consumer
   (`src/queue/consumer.rs`) once `flush_batch_to_shards` returns
   `Ok(_)` — that is the point at which "committed" is true.
   When the consumer is rewired into the transactions module
   (Phase 2 follow-up) the call site moves but the contract does
   not.
3. ✅ `notifications::infrastructure` subscribes via
   `Arc<dyn EventSubscriber>`. Today the bus is in-process; the
   AMQP-backed swap is a one-file replacement that satisfies the
   same two traits.
4. ✅ `notifications::api::router` exposes
   `GET /api/v2/notifications/recent?limit=N`, returning the
   newest entries from the in-memory ring buffer.

Exit criteria:

- ✅ `notifications` has **zero** module-level `use crate::modules::*`
  imports — verified by
  `rg 'use crate::modules' src/modules/notifications/` returning
  no matches.
- Building `notifications` alone (once we split crates in Phase 4)
  doesn't require `accounts` or `transactions` sources. Pending
  Phase 4 — the file-level seal is in place today.
- ✅ An event-smoke test demonstrates a publish → consume round
  trip. `cargo check` proves compile-shape; the runtime smoke is
  any `POST /api/v2/transactions` followed (~tens of ms later,
  after the consumer batch flush) by a
  `GET /api/v2/notifications/recent` that returns the corresponding
  entry.

**Phase 3 partial — what is still missing**:

- **Persistent log**: the `notification_log` table from
  `src/modules/notifications/README.md` is not yet created;
  Phase 3 uses an in-memory `VecDeque<NotificationEntry>` capped
  at 512. Restarts lose history. Swap is a sibling
  `infrastructure/repository.rs` selected at `init` time.
- **Dispatch channels**: the README's email / push / banner
  channels are not implemented; the dispatcher only writes to the
  log today.
- **Subscriptions table**: opt-in / opt-out is not modelled.
- **AMQP transport**: the bus is in-process. The two traits
  (`EventPublisher`, `EventSubscriber`) are deliberately the
  swap surface for an AMQP-backed impl in Phase 4 / 5.

**Time estimate**: 1–2 days.

---

## Phase 4 — Workspace crate split ✅ DONE

The compiler now enforces the module boundaries. Layout:

```
Cargo.toml                  workspace root: [workspace] members + [workspace.dependencies]
crates/
  app/                      — binary (`peakload-capstone`); bootstrap, config, middleware, legacy v1 handlers, queue consumer
  shared_kernel/            — events, db (shard router + failover + pool), cache (Redis), queue/producer, error, responses
  accounts/                 — leaf module crate
  transactions/             — depends on shared_kernel + accounts
  notifications/            — depends on shared_kernel only
```

See [`docs/architecture/phase4-workspace-walkthrough.md`](./phase4-workspace-walkthrough.md)
for the file-by-file tour.

What landed in this phase:

1. ✅ Each `src/modules/<name>/` directory moved into
   `crates/<name>/src/`. The internal `domain / application /
   infrastructure / api / ports` shape is preserved verbatim;
   each module's `mod.rs` became its `lib.rs`.
2. ✅ Cross-cutting infrastructure (db / cache / queue::producer /
   error / responses) moved into `shared_kernel` because module
   crates cannot depend on `app` (that would be a cycle).
3. ✅ Per-crate `Cargo.toml` files with workspace-level
   `[workspace.dependencies]` so version bumps remain a one-line
   change at the root.
4. ✅ Producer + ShardRouter + RedisCache constructors refactored
   to drop `&Config` — kernel-local config slices
   (`ShardRouterConfig`, `RedisCacheConfig`, plain `&str`
   amqp_url) keep the kernel free of binary-only concerns
   (env-var loading).
5. ✅ `_template/` moved from `src/modules/_template/` to
   [`docs/architecture/module-template/`](./module-template/) —
   it is documentation, not code, so it should not compile.

Exit criteria:

- ✅ `cargo check -p accounts` compiles without pulling in
  `transactions` or `notifications` (verified — they are not in
  `crates/accounts/Cargo.toml`).
- ✅ A forbidden import across crate boundaries fails
  compilation: try adding `use transactions::ports::*;` to
  `crates/accounts/src/...` — `unresolved crate transactions`.
  The seal is now mechanical, not aspirational.
- ✅ All 18 unit tests pass workspace-wide
  (`cargo test --workspace --lib --bins`).
- CI build times per-crate: the workspace builds in `~44s` cold
  on this developer machine; per-crate parallelism via
  `cargo build -p <name>` is now possible.

**What is still in `app` after Phase 4** — these are the items
that the **next** migration steps clear out. See
[`docs/architecture/cutover-readiness.md`](./cutover-readiness.md)
for the gating criteria.

- `crates/app/src/api/handlers.rs` — legacy v1 handlers
  (`/api/v1/transactions/*`, `/api/v1/users/.../balance`,
  `/health`, `/metrics`).
- `crates/app/src/db/models.rs` — legacy DB DTOs read by the
  v1 handlers + the queue consumer.
- ~~`crates/app/src/queue/consumer.rs`~~ — **rewired** (Step A on
  this branch). The consumer now lives at
  `crates/transactions/src/infrastructure/consumer.rs`; the
  `transactions` crate owns its full write path end-to-end and
  the bootstrap calls `transactions::start_consumer(...)`.
- `/api/v1/*` route nest in `bootstrap.rs::build_router` —
  cleared by Step B (v1 cull). Caller catalogue in
  [`v1-caller-inventory.md`](./v1-caller-inventory.md).

---

## Phase 5 — Extract the first microservice (when justified)

Do not do Phase 5 until at least one of:

- **Hot path capacity.** One module's traffic is saturating its
  co-tenants. Extracting it lets us scale it independently.
- **Deploy cadence mismatch.** One module's change-rate is bottle-
  necked by the monolith's release process. Extracting lets it ship
  independently.
- **Ownership split.** Two teams own two modules and the
  co-deployment is becoming a coordination tax.

If none of these are true yet, **stay a modular monolith**. Micro-
services cost observability, deploy complexity, and distributed-
systems debugging. The modular monolith gets us 80% of the benefits
for 20% of the cost.

When the trigger fires, the extraction is mechanical because Phase 4
already shaped the module as an independent crate:

1. Create a new binary crate (`crates/<name>-service`) that boots
   just the module plus its own HAProxy-routed PG shards / cache /
   queue.
2. Replace the in-process call site (in the remaining monolith)
   with an HTTP/AMQP client that implements the same `ports` trait.
3. The module's crate is now compiled into both the old binary
   (transitional) and the new service (canonical). Stand up the
   service, shift traffic, then delete the in-process wiring from
   the monolith.

Because the port trait is the same, there is **one interface** to
keep compatible — not every internal function.

---

## Rollback strategy

Every phase has a clean rollback:

- **Phase 1 / 2 / 3**: revert the PR. Original flat code is still in
  git history; nothing was destructively overwritten (we copied,
  then deleted the copies).
- **Phase 4**: revert the workspace-split PR. All code is still in
  `src/modules/` directory tree, Cargo manifests are the only
  additions.
- **Phase 5**: revert the service-extraction PR. The module crate is
  still compiled into the monolith binary; the new service
  container is just no longer routed.

No phase creates a forward-only data migration. Schema ownership
moves (monolith owns it → the extracted service owns it) but the
table itself is in the same shard until a subsequent explicit
migration step.

---

## What we are NOT promising

- **Not a specific timeline.** The phases are gated by review + test,
  not by calendar. Phase 3 can happen three months after Phase 2
  without anything breaking.
- **Not perfect separation on day one.** The first module extraction
  WILL discover shared concepts we forgot to put in `shared_kernel`.
  That is fine — add them when found.
- **Not zero downtime at every phase.** Individual phases are safe
  and rollback-capable, but a Phase 4 deploy that accidentally
  breaks a `Cargo.toml` entry will still take the build down. Ship
  these phases during normal release windows, not emergency patches.
- **Not "microservices by default".** The whole document exists to
  push microservices as *late* as possible. Phase 5 is an escape
  hatch, not a goal.
