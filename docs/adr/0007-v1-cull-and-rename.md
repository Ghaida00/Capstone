# 0007 — v1 → v2 hard cutover, no alias router

**Status:** Accepted
**Date:** 2026-04-15

## Context

The Phase-1/2 module extractions (see [ADR-0001](0001-modular-monolith.md))
shipped new endpoints under `/api/v2/*` while leaving the original
`/api/v1/*` handlers in `crates/app/src/api/handlers.rs` so traffic
kept flowing during the cutover. By the time Phase 4 closed
([ADR-0002](0002-cargo-workspace-split.md)), the v1 layer was three
pieces of pure duplication:

- `crates/app/src/api/handlers.rs` — legacy v1 HTTP handlers
- `crates/app/src/db/models.rs` — DTOs shared by v1 handlers + consumer
- The `/api/v1` route nest in `bootstrap.rs::build_router`

Every change to a transaction or account had to touch both paths.

There was also a **rename**, not just a version bump: `/api/v1/users/.../balance`
becomes `/api/v2/accounts/.../balance` because we renamed the
resource (the `users` table content was always "accounts" and the
rename clarifies ownership for the `accounts` module — see
[ADR-0003](0003-port-adapter-shape.md)).

All known callers are first-party: k6 scripts, integration tests,
nginx, OpenAPI, dashboards. There are no external clients to coordinate
with.

## Decision

**Hard cutover**: delete the v1 surface, rewrite all callers to v2 in
the same change. Do **not** add an alias router (`/api/v1/users/.../balance`
→ `/api/v2/accounts/.../balance`).

Sequenced as two PRs:
1. **Step A** — move the RabbitMQ consumer from
   `crates/app/src/queue/consumer.rs` into
   `crates/transactions/src/infrastructure/consumer.rs` so the
   `transactions` crate owns its full write path.
2. **Step B** — delete `/api/v1/*`, rescue `/health` + `/metrics` into
   `crates/app/src/health.rs`, rewrite all known callers.

## Consequences

- **`crates/app/` collapses to a pure composition root** (~200 LOC).
  Modular monolith reaches 100%.
- **No v1 fallback.** Any caller that still hits `/api/v1/*` after
  Step B gets a 404. Acceptable because the caller catalogue was
  exhaustive (k6, tests, nginx, OpenAPI — all in-tree).
- **Behavioural divergence is now observable.** v2 fails fast with
  400 when `from_account` doesn't exist (v1 quietly queued and the
  consumer surfaced a `failed` row). After the cutover, fail-fast
  is the only behaviour.
- **Idempotency keys span the cutover.** Key shape (`txn:<shard>:<reference_id>`)
  is unchanged, so a v1 message that landed in the queue before
  Step A hashes to the same key as a v2 publish for the same input.
- **Rollback**: revert the cull PR. v1 reappears from git. Note that
  if callers were updated to v2 in the same PR, they keep working
  after revert (v2 still exists). Rolling back restores v1
  *availability*, not caller routing.

## Alternatives considered

- **Alias router.** `/api/v1/foo` → `/api/v2/bar` with an HTTP 308.
  Justified if external clients exist. Rejected: no external
  clients, and the alias would have made the v1 cull a *deferred*
  job (alias today, delete the old path later), which we have
  empirically learned never happens.
- **Soak-test gate (≥48h ≥50% v2 traffic before cull).** The right
  answer for production deployments; skipped for capstone since the
  full k6 suite passes against v2 and there are no external clients
  to risk.
