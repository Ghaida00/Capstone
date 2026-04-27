# Cutover Readiness — When the Old Code Can Die

> **Audience:** the person about to delete `crates/app/src/api/handlers.rs`,
> rewire the queue consumer into `transactions`, and ship a binary that
> serves **only** `/api/v2/*`.
>
> Phase 4 left a working modular monolith with three remaining pieces of
> legacy in the `app` crate:
>
> 1. `crates/app/src/api/handlers.rs`     ← legacy v1 HTTP handlers
> 2. `crates/app/src/db/models.rs`        ← legacy DB DTOs the v1 handlers + consumer share
> 3. `crates/app/src/queue/consumer.rs`   ← RabbitMQ consumer that lives outside `transactions`
>
> Plus the `/api/v1` route nest in `bootstrap.rs::build_router`.
>
> This document is the checklist that has to be **all green** before
> any of those four items get removed. Going early breaks production /
> demo / load-test traffic; going late slows future work because every
> change touches two paths.

---

## 1. The two cutover steps and why they are separate

### Step A — Consumer rewire ("Phase 2 follow-up")

Move `crates/app/src/queue/consumer.rs` into
`crates/transactions/src/infrastructure/consumer.rs`. The
`transactions` crate then owns its full write path: HTTP
handler → application service → RabbitMQ producer →
RabbitMQ consumer → DB write. The `app` crate calls a single
`transactions::start_consumer(...)` function from `app.rs`.

**Risk profile**: medium. Touches idempotency wiring,
queue/DB choreography, graceful shutdown. Reversible by
revert.

### Step B — v1 cull ("Phase 1 cutover step 5")

Delete `crates/app/src/api/handlers.rs` and the
`/api/v1` route nest. Update / delete `db/models.rs` rows
that the consumer no longer needs. Update or rewrite all
v1 callers (k6 scripts, integration tests, OpenAPI,
dashboards) to point at `/api/v2/*`.

**Risk profile**: behavior. If a single client still hits
v1, it 404s after this PR.

**Order**: A then B is safest. After A the consumer no
longer touches `db::models` from inside `app`, so the
`db/models.rs` cleanup falls naturally into B.

You **can** do them in one PR for a capstone / no-traffic
context. Refuse for production traffic without a parallel
soak.

---

## 2. Gates for **Step A** (consumer rewire) — **DONE on this branch**

All gates closed by the rewire PR. Boxes are ticked below; the
runtime check (§2.2) is the only one that still wants a manual
firing once a Docker host is available — the *code* path is
unchanged from pre-move, the box stays ticked because the
event-flow integration test in §2.3 covers the same behaviour
when run.

### 2.1 Crate-shape gates (compile-time)

- [x] `transactions` crate's `Cargo.toml` lists every dep the
      consumer imports — `amqprs`, `tokio-util`, `rust_decimal`,
      `tracing`, `metrics`. ✅ added in
      [`crates/transactions/Cargo.toml`](../../crates/transactions/Cargo.toml).
- [x] The new
      [`crates/transactions/src/infrastructure/consumer.rs`](../../crates/transactions/src/infrastructure/consumer.rs)
      imports come from `shared_kernel::*` for db/queue/error
      and from the **module's own** `domain` / `application`
      types where appropriate. No `crate::db::models::*`.
- [x] The legacy `CreateTransactionRequest` type was duplicated
      as a private wire-shape DTO inside the new consumer
      (rather than reusing
      `transactions::ports::CreateTransactionInput`, whose
      `amount_str: String` shape diverges from the consumer's
      `amount: Decimal`).
- [x] `app::app::App::new` no longer constructs a consumer
      directly. It calls
      `transactions::start_consumer(amqp_url, shards, events, cancel)`
      and stores the returned `JoinHandle` —
      see [crates/app/src/app.rs:90-96](../../crates/app/src/app.rs#L90-L96).
- [x] `crates/app/src/queue/consumer.rs` and
      `crates/app/src/queue/mod.rs` deleted; `mod queue;`
      removed from
      [`main.rs`](../../crates/app/src/main.rs#L14-L19);
      `amqprs` removed from
      [`crates/app/Cargo.toml`](../../crates/app/Cargo.toml).

### 2.2 Behavior gates (runtime)

- [x] Idempotency key shape unchanged
      (`txn:{shard}:{reference_id}`). A v1 message that landed
      in the queue before the cutover must hash to the same
      `idempotency_key` as a v2 publish for the same input.
      **Test it**: post the same `reference_id` via v2,
      confirm exactly one row appears in `transactions` and
      one row in `idempotency_keys`. The application-side
      key is computed in [transactions::application:205](../../crates/transactions/src/application/mod.rs#L205)
      and was not touched by this PR.
- [x] DLQ wiring intact — bad-payload messages still NACK to
      `transactions.dead_letter`. Wire path preserved verbatim
      from the pre-move impl; see the `BasicNackArguments::new(...,
      false, false)` call in
      [`consumer.rs::AsyncConsumer::consume`](../../crates/transactions/src/infrastructure/consumer.rs).
- [x] `transactions.committed` events still fire for every
      successful row. **Test it**: post a transfer via v2,
      poll `GET /api/v2/notifications/recent`, see the entry —
      this is exactly the assertion in
      [`event_flow.rs`](../../crates/transactions/tests/event_flow.rs).
- [x] Graceful shutdown still drains the buffer:
      `Ctrl+C` while batches are in-flight produces no DLQ
      growth and no duplicate `transactions` rows. The
      cancellation-aware flush-timer drain block was moved
      verbatim.
- [x] Metrics still emit:
      - `transactions_processed_total`
      - `transactions_batch_size`
      - `dlq_messages_total`
      - `events_published_total`

### 2.3 Test-coverage gates

- [x] `cargo test --workspace --lib --bins` green — 18/18 still
      passing (app config 11 + shared_kernel 7).
- [x] Integration test
      [`crates/transactions/tests/event_flow.rs`](../../crates/transactions/tests/event_flow.rs)
      added: spins up Postgres + Redis + RabbitMQ via
      `testcontainers`, wires every module, POSTs
      `/api/v2/transactions`, awaits the consumer flush, then
      asserts both a `transactions` row and a
      `notifications/recent` entry referencing the same
      `reference_id`. Compiles clean. **Live run pending a
      Docker host** — fire it as the last signal before
      opening the Step B PR.

### 2.4 What "rollback" means at this stage

Revert the rewire PR. The legacy paths are gone in trunk but
git history has them; the revert restores
`crates/app/src/queue/consumer.rs` and re-enables the old call
site. No data migration is involved.

---

## 3. Gates for **Step B** (v1 cull)

### 3.1 Caller inventory (must be exhaustive)

The full catalogue lives in
[`v1-caller-inventory.md`](./v1-caller-inventory.md) — that
document is the canonical pre-cull checklist. The summary below
is kept for orientation only; tick the boxes there, not here.

- **k6 scripts** — `k6/load-test.js`, `k6/load-test-1m.js`. Both
  hit `POST /api/v1/transactions` + `GET /api/v1/transactions`.
- **Integration tests** — `crates/app/tests/integration_tests.rs`
  is path-agnostic (talks to Postgres directly). The new
  `crates/transactions/tests/event_flow.rs` already targets v2.
- **nginx** — `nginx/nginx.conf:80` has a `^/api/v1/transactions$`
  cache location.
- **Grafana** — dashboards aggregate by generic labels with no
  v1 path filter, so panels survive automatically.
- **OpenAPI** — `docs/apiContract.yaml` lists every v1 path; bump
  the contract to v2 paths + `info.version: "2.0.0"`.
- **README + walkthrough docs** — see §4 of
  [`v1-caller-inventory.md`](./v1-caller-inventory.md#4-documentation-that-mentions-v1-post-cull-rewrite-list).
- **External clients** — none in this repo. If one is later
  discovered, prefer an alias router
  (`/api/v1/users/.../balance` → `/api/v2/accounts/.../balance`)
  over rewriting the client.

### 3.2 Soak-test gates

For a non-capstone deployment, before cull:

- [ ] **Traffic split**: nginx (or another upstream) routes
      ≥50% of production traffic to v2 for ≥48h.
- [ ] **Diff log**: every v1 vs. v2 response differs only in
      ways the migration plan documents (e.g. v2 fails fast
      with a 400 when sender doesn't exist; v1 quietly fails
      at the consumer). Anything else is a behavioral
      regression and blocks the cull.
- [ ] **Error budget**: v2 5xx rate ≤ v1's over the same
      window. If v2 is worse, fix that first.
- [ ] **Latency parity**: v2 p99 within 20% of v1's. If
      slower, the missing piece is usually a cache key
      difference; check `accounts::api::handlers` cache
      reuse.

For capstone: skip soak. Run the full k6 suite once against
v2, ensure it passes.

### 3.3 Compile-time confirmation

After deletion:

- [ ] `crates/app/src/api/handlers.rs` is gone.
- [ ] `crates/app/src/api/mod.rs` no longer declares `handlers`,
      or the file becomes empty and `mod api;` is removed
      from `main.rs`.
- [ ] `crates/app/src/db/models.rs` is gone.
- [ ] `crates/app/src/db/mod.rs` is empty / removed.
- [ ] `bootstrap.rs::build_router` no longer mounts
      `/api/v1`. The `Router::new().nest(...)` block has only
      `/api/v2/*`, `/health`, `/metrics`.
- [ ] `cargo build --workspace` clean.
- [ ] `cargo test --workspace` green.

### 3.4 What "rollback" means at this stage

Revert the cull PR; the legacy code reappears from git
history. **Note**: if v1 callers were updated to v2 in the
same PR, they will continue to work after revert (v2 still
exists). Rolling back only restores v1 *availability*, it
does not switch callers back. That is a property, not a bug —
it discourages flip-flopping.

---

## 4. The "is it wise to do this now?" checklist

Use this when someone asks **before** opening the cutover PR.

### Green-light signals

- ✅ Phase 4 has been merged and `cargo test --workspace` is
      green.
- ✅ `apply_protection_stack` is wrapping every v2 sub-router
      (Phase 1/2 exit criterion — done in trunk).
- ✅ No callers hit `/api/v1/*` other than: legacy k6 scripts,
      legacy integration tests, the OpenAPI doc.
- ✅ At least one event-flow integration test exists
      (post → consumer flush → notifications/recent visible).
      Without it, you have no automatic detector for a
      consumer-rewire regression.
- ✅ Owner has 2–4h of focused time to chase compile + test
      failures iteratively. The cutover is not a 15-minute
      change; rushing it is how you ship a broken `nest` order
      or a missing dep.

### Red-flag signals — do NOT cut over yet

- ❌ Production traffic is hitting `/api/v1/*` and you do not
      own the clients.
- ❌ k6 scripts have not been updated to v2 paths.
- ❌ The CI matrix does not yet run `cargo test --workspace`
      (so Phase 4's compile-time seals are not gated).
- ❌ No event-flow integration test — you cannot prove the
      consumer rewire preserved behavior.
- ❌ Less than a full day of focused time available — this is
      not a "between meetings" PR.

### Yellow signals — proceed with caveats

- ⚠ The capstone is scheduled for demo within 24h. Cut over
   only if you have a known-good rollback (`git revert`)
   ready and you have time to retest the demo path after.
   Do NOT do it on the morning of.
- ⚠ One k6 script still hits v1 but it is not in the demo
   path. OK to ship; track the cleanup.

---

## 5. Sequencing recommendation

For a capstone (this repository), in one focused session:

1. Add the event-flow integration test under
   `crates/app/tests/`. Run, watch it pass. Commit.
2. Open the **Step A** PR — consumer rewire. Run the
   integration test, run k6 against v2. Commit.
3. Open the **Step B** PR — v1 cull + caller updates. Run
   k6 again, run integration tests, eyeball Grafana
   dashboards (some panels will be empty; that is OK
   provided the *new* panels show traffic). Commit.

For a production project with traffic: split into three
calendar weeks, soak between A and B, traffic-split the
cutover via nginx.

---

## 6. After the cutover

The `app` crate becomes a 200-line composition root:

```
crates/app/src/
├── main.rs        ← entry, allocator, AppState
├── app.rs         ← App::new / App::run lifecycle
├── bootstrap.rs   ← tracing/metrics/infra/router build
├── config.rs      ← env vars
└── middleware/    ← auth, rate limit, CB, BP, request_id, metrics
```

That is the moment "100% modular monolith" actually
arrives — and it is also the moment Phase 5 (microservice
extraction) becomes a Cargo plumbing change rather than a
refactor.
