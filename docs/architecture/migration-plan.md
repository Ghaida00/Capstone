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

**Time estimate**: 1–2 days for a developer familiar with the
codebase.

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

**Key test**: change an internal of `accounts::infrastructure` (e.g.
swap SELECT shape) without touching `transactions` and watch only
`accounts` rebuild. This is the user-requested invariant made
tangible.

**Time estimate**: 2–3 days.

---

## Phase 3 — Third module: `notifications`

Implements the **event-driven, independent module** story.

Scope:

1. `shared_kernel::events` gets a first real type: e.g.
   `TransactionCommitted { id, from, to, amount }`.
2. `transactions::application` publishes this event after a
   successful transfer.
3. `notifications::infrastructure` subscribes to it (via the
   existing RabbitMQ wiring, now threaded through shared_kernel).
4. `notifications::api::router` exposes any HTTP endpoints
   notifications owns (e.g. `/notifications/status/...`).

Exit criteria:

- `notifications` has **zero** module-level `use crate::modules::*`
  imports.
- Building `notifications` alone (once we split crates in Phase 4)
  doesn't require `accounts` or `transactions` sources.
- An event-smoke test demonstrates a publish → consume round trip.

**Time estimate**: 1–2 days.

---

## Phase 4 — Workspace crate split

Until now everything has been one Cargo crate with internal module
boundaries enforced by convention + `pub(crate)` visibility. Phase 4
promotes each module to its own crate so the **compiler** enforces
the boundaries.

Target layout:

```
Cargo.toml                  (workspace root, [workspace] members = [...])
crates/
  app                       — the binary (main.rs + bootstrap)
  shared_kernel             — infra only
  accounts                  — module, depends on shared_kernel
  transactions              — depends on shared_kernel + accounts
  notifications             — depends on shared_kernel
```

Inside each module crate, the old directory structure is preserved
verbatim. The `ports.rs` becomes the crate's `src/lib.rs`-exported
public API; everything else is either `pub(crate)` (within that
crate) or plain private.

Work:

- Move each `src/modules/<name>/` directory into `crates/<name>/src/`.
- Add per-crate `Cargo.toml` with the right dependencies.
- Update `Cargo.toml` workspace root.
- `cargo build` until it compiles; fix forbidden imports (they will
  now be compile errors, which is the point).
- CI matrix runs per-crate tests in parallel.

Exit criteria:

- `cargo check -p accounts` compiles without pulling in
  `transactions` or `notifications`.
- A forbidden import across crate boundaries fails compilation (not
  just review).
- CI build times per-crate are lower than the monolithic build
  (parallelism is the whole point).

**Time estimate**: 2–3 days. Mostly Cargo plumbing, little code
change.

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
