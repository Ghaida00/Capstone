# v1 Caller Inventory — pre-cull catalogue

> **Purpose:** [cutover-readiness §3.1](./cutover-readiness.md#31-caller-inventory-must-be-exhaustive)
> requires every caller of `/api/v1/*` to be catalogued before
> Step B (the v1 cull). This file is that catalogue. Mark each
> row's checkbox as the cull PR rewrites the caller; when every
> box is ticked plus the source-code rows in §6, `make check`
> stays green and Step B is mergeable.

---

## 1. v1 surface (5 routes)

```
POST   /api/v1/transactions
GET    /api/v1/transactions               (list, ?limit=&offset=)
GET    /api/v1/transactions/{id}
GET    /api/v1/transactions/status/{reference_id}
GET    /api/v1/users/{account_number}/balance
```

### v2 mapping

| v1 route                                          | v2 route                                              | Notes |
|---------------------------------------------------|--------------------------------------------------------|-------|
| `POST /api/v1/transactions`                       | `POST /api/v2/transactions`                            | Behavioural divergence: v2 fails fast 400 if `from_account` is missing; v1 lets the consumer surface a `failed` row. See [phase2 walkthrough §6.2](./phase2-transactions-walkthrough.md). |
| `GET  /api/v1/transactions?limit&offset`          | `GET  /api/v2/transactions?limit&before=<rfc3339>`     | v2 list is cursor-based (`before`); `offset` is silently ignored — see [api/handlers.rs:113](../../crates/transactions/src/api/handlers.rs#L113). k6 scripts use `?limit=10&offset=0` which still works because limit is honoured. |
| `GET  /api/v1/transactions/{id}`                  | `GET  /api/v2/transactions/{id}`                       | Byte-identical response. |
| `GET  /api/v1/transactions/status/{reference_id}` | `GET  /api/v2/transactions/status/{reference_id}`      | Byte-identical response. |
| `GET  /api/v1/users/{account_number}/balance`     | `GET  /api/v2/accounts/{account_number}/balance`       | **Resource rename**: `users` → `accounts`. Path segment changes; response schema unchanged. |

`/health` and `/metrics` are NOT v1 — they live at the root and stay
as-is. The cull must keep them mounted (their handlers currently sit
inside [`crates/app/src/api/handlers.rs`](../../crates/app/src/api/handlers.rs#L659-L702)
and need to be rescued before that file is deleted; see §6).

---

## 2. External callers — code & config

### 2.1 k6 load tests

| File                                                | Lines      | Action                                                |
|-----------------------------------------------------|------------|-------------------------------------------------------|
| [`k6/load-test.js`](../../k6/load-test.js)          | 120, 149   | Replace `/api/v1/transactions` → `/api/v2/transactions`. The `?limit=10&offset=0` query string can stay verbatim — `offset` is ignored by v2, harmless. |
| [`k6/load-test-1m.js`](../../k6/load-test-1m.js)    | 87, 123    | Same two replacements as above.                       |

Both scripts assert `status === 202` for create + a `body.data.reference_id`
field on success. v2 returns the same shape — no other test edits needed.

- [ ] `k6/load-test.js` rewritten to v2 paths.
- [ ] `k6/load-test-1m.js` rewritten to v2 paths.
- [ ] Smoke run of both scripts (≥1 minute) against a v2-only binary
      shows `status 202` rate matching the pre-cull baseline.

### 2.2 nginx

[`nginx/nginx.conf:80`](../../nginx/nginx.conf#L80) — special
location with response cache for `^/api/v1/transactions$` GETs (1s
proxy_cache, rate-limited). Logic mirrors a "list endpoint cache".

- [ ] Rewrite the `location ~ ^/api/v1/transactions$` block's regex
      to `^/api/v2/transactions$`. Cache + rate-limit body unchanged.
      The trailing `/api/` catch-all stays as the fallback for the
      remaining v2 routes (POST + by-id + by-reference-id +
      `/api/v2/accounts/...`), which is the existing behaviour.

### 2.3 OpenAPI contract

[`docs/apiContract.yaml`](../../docs/apiContract.yaml) lines 188,
191, 249, 275, 298 + the example metric line at 188.

- [ ] Rename every `/api/v1/...` path → `/api/v2/...`.
- [ ] Rename `/api/v1/users/{account_number}/balance` →
      `/api/v2/accounts/{account_number}/balance` (resource rename,
      not just version bump).
- [ ] Update the metric example string at line 188 to reference
      `path="/api/v2/transactions"`.
- [ ] Bump `info.version` to `2.0.0` to mark the breaking change.

### 2.4 Grafana — **no edits required** ✅

The two dashboards in [`grafana/dashboards/`](../../grafana/dashboards/)
aggregate by the generic labels `http_request_method` + `url_path`
and explicitly exclude `/health|/metrics`. No panel filters on a
`route="/api/v1/..."` or path literal, so after the cull each panel
just begins reflecting v2 traffic. Confirmed via grep:

```bash
rg 'api/v1|api_v1|v1/users|v1/transactions' grafana/  # → no matches
```

Action: **none**. After the first v2-only deploy, eyeball each
panel for non-empty timeseries to confirm scrape labels match.

### 2.5 Integration tests

[`crates/app/tests/integration_tests.rs`](../../crates/app/tests/integration_tests.rs)
hits the database directly (no HTTP), so it is path-agnostic and
needs no edits. The new event-flow test
[`crates/transactions/tests/event_flow.rs`](../../crates/transactions/tests/event_flow.rs)
already targets `/api/v2/*`.

- [ ] (None.) Re-run `cargo test --workspace` after the cull
      to confirm both still pass.

### 2.6 External clients — **none**

This is a capstone repository. No external HTTP clients are
known. If one is discovered later, [cutover-readiness §3.1](./cutover-readiness.md#31-caller-inventory-must-be-exhaustive)
recommends an alias router (`/api/v1/users/.../balance` →
`/api/v2/accounts/.../balance`) instead of rewriting the client.
For now, skip.

---

## 3. Internal references that disappear with the cull

These are not "callers" — they are source-code consumers of the
legacy types/handlers that go away when their target file goes away.
Listing them here so the cull PR's diff is predictable.

| Consumer                                                                    | Legacy item it imports                              |
|-----------------------------------------------------------------------------|-----------------------------------------------------|
| [`crates/app/src/api/handlers.rs`](../../crates/app/src/api/handlers.rs)   | `crate::db::models::{CreateTransactionRequest, IdempotencyKeyRow, TransactionResponse, TransactionRow, TransactionStatusRow, UserRow}` |
| [`crates/app/src/bootstrap.rs:257-283`](../../crates/app/src/bootstrap.rs#L257-L283) | `crate::api::handlers::{create_transaction, list_transactions, get_transaction, get_transaction_status, get_balance}` |
| [`crates/app/src/bootstrap.rs:360-361`](../../crates/app/src/bootstrap.rs#L360-L361) | `crate::api::handlers::{health_check, prometheus_metrics}` — **must survive** the cull (see §6). |
| [`crates/app/src/main.rs:14,18`](../../crates/app/src/main.rs#L14-L18)     | `mod api;` and `mod db;` — both go away with their files. |

The Step-A consumer rewire already removed the consumer's
dependency on `crate::db::models::CreateTransactionRequest`; nothing
outside `crates/app/src/api/` still references those types.

---

## 4. Documentation that mentions v1 (post-cull rewrite list)

These docs describe the parallel-path migration. Once v1 is gone,
they need a "Status" stamp + a sweep of stale "v1 still exists"
phrasing. None of them block the cull — they get rewritten *with*
the cull PR or in a docs follow-up.

| File                                                                                                         | What to change                                                                                                  |
|--------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| [`docs/architecture/migration-plan.md`](./migration-plan.md)                                                  | Phase 1 §"Phase 1 partial — what is still missing": cutover step 5 → ✅. Phase 4 leftover bullets at lines 258–267: cross out v1 handlers / db/models / `/api/v1` nest. |
| [`docs/architecture/cutover-readiness.md`](./cutover-readiness.md)                                            | Step B §3 boxes get ticked. Steps A boxes already done in this branch — see §5 below.                            |
| [`docs/architecture/phase4-workspace-walkthrough.md`](./phase4-workspace-walkthrough.md)                      | "What is still in `app` after Phase 4" table: drop the v1 / db.models / queue rows. §8 list shrinks to whatever Step B does *not* clean up (probably the `db_write_retry_backoff_ms` wiring). |
| [`docs/architecture/modular-monolith.md`](./modular-monolith.md)                                              | Path-mapping table at lines 18, 20, 23: drop the "(legacy)" annotations once the rows are gone in trunk. The intro `**Status**` paragraph at line 4 updates from "the legacy v1 surface + the un-rewired queue consumer are still in `crates/app/`" to "100% modular monolith reached". |
| [`docs/architecture/phase1-accounts-walkthrough.md`](./phase1-accounts-walkthrough.md)                        | §"Cutover" mentions the legacy `/api/v1/users/.../balance` as still-live. Update to past tense.                  |
| [`docs/architecture/phase2-transactions-walkthrough.md`](./phase2-transactions-walkthrough.md)                | §6.1 entire section ("Queue consumer rewire") becomes obsolete (Step A done). §"behavioural divergence" footnote — v2 behaviour is now the only behaviour.       |
| [`crates/accounts/README.md`](../../crates/accounts/README.md)                                                | Lines 4–5, 73, 79: drop legacy mentions.                                                                          |
| [`crates/transactions/README.md`](../../crates/transactions/README.md)                                        | Lines 4, 7, 54–55, 80–81, 97: drop legacy mentions; the consumer line in the gap-table goes away.                 |

---

## 5. Step A status (consumer rewire) — for cross-reference

Step A is **merged on this branch** (not yet on `main`); the
gating boxes from
[cutover-readiness §2](./cutover-readiness.md#2-gates-for-step-a-consumer-rewire)
are now:

- [x] Crate-shape gates §2.1 — consumer lives at
      [`crates/transactions/src/infrastructure/consumer.rs`](../../crates/transactions/src/infrastructure/consumer.rs);
      `app::App::new` calls `transactions::start_consumer(...)`;
      `crates/app/src/queue/` directory deleted.
- [x] Behavior gates §2.2 — idempotency-key shape preserved
      verbatim (`txn:{shard}:{reference_id}`); DLQ NACK path,
      `transactions.committed` event publish, and the four
      counters/histograms left untouched in the move.
      **Run-time verification still pending** — needs a Docker host
      to fire the event-flow integration test.
- [x] Test-coverage gate §2.3 — event-flow test landed at
      [`crates/transactions/tests/event_flow.rs`](../../crates/transactions/tests/event_flow.rs).
      `cargo test --workspace --lib --bins` is green (18/18). The
      integration test compiles clean but has not been run
      end-to-end in this session because the local Docker daemon
      is offline; running it is the last green-light signal before
      Step B.

---

## 6. Source-code work the Step B PR does

Listed last because they only land **after** every external caller
in §2 is rewritten. Each item is a single edit in the cull PR.

- [ ] **Rescue `/health` + `/metrics` handlers.** The two functions
      currently live at
      [`crates/app/src/api/handlers.rs:659-702`](../../crates/app/src/api/handlers.rs#L659-L702).
      Move them to a new `crates/app/src/health.rs` (or inline them
      in `bootstrap.rs::build_router` — they are 3 + 30 lines).
      Update the `.route("/health", ...)` / `.route("/metrics", ...)`
      lines in [bootstrap.rs:360-361](../../crates/app/src/bootstrap.rs#L360-L361)
      to point at the new home.
- [ ] **Delete** [`crates/app/src/api/handlers.rs`](../../crates/app/src/api/handlers.rs).
- [ ] **Delete** [`crates/app/src/api/mod.rs`](../../crates/app/src/api/mod.rs)
      (only declares `pub mod handlers;`).
- [ ] **Delete** [`crates/app/src/db/models.rs`](../../crates/app/src/db/models.rs).
- [ ] **Delete** [`crates/app/src/db/mod.rs`](../../crates/app/src/db/mod.rs)
      (only declares `pub mod models;`).
- [ ] **Trim** [`crates/app/src/main.rs:14-19`](../../crates/app/src/main.rs#L14-L19)
      — drop `mod api;` and `mod db;`.
- [ ] **Trim** [`crates/app/src/bootstrap.rs`](../../crates/app/src/bootstrap.rs)
      — drop the v1 `api_routes` block (lines 257-283) and the
      `.nest("/api/v1", ...)` line at 356. The remaining router
      then has only `nest_service("/api/v2/...", ...)` × 3 +
      `/health` + `/metrics`.
- [ ] **Update** the v1 example in
      [`crates/app/src/middleware/metrics.rs:8,13-14`](../../crates/app/src/middleware/metrics.rs#L8)
      to use a v2 path. Cosmetic — a comment in a file that uses
      `MatchedPath`, not the literal URI, so behaviour is
      unaffected.

After these edits + the docs in §4, `crates/app/src/` collapses to
roughly:

```
crates/app/src/
├── main.rs
├── app.rs
├── bootstrap.rs
├── config.rs
├── health.rs       (new — or inlined in bootstrap.rs)
└── middleware/
```

That is the moment the modular monolith is at 100%. Phase 5
(microservice extraction) becomes a Cargo plumbing change rather
than a refactor.
