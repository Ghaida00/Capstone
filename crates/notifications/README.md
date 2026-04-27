# `notifications` — out-of-band alerts (module **C**, independent)

> Phase 3 of the migration is in. The module subscribes to
> `shared_kernel::events`, currently filters for
> `transactions.committed` events, and exposes
> `GET /api/v2/notifications/recent` backed by an in-memory ring
> buffer. See
> [`docs/architecture/phase3-notifications-walkthrough.md`](../../../docs/architecture/phase3-notifications-walkthrough.md)
> for the file-by-file tour and
> [`docs/architecture/migration-plan.md` §Phase 3](../../../docs/architecture/migration-plan.md)
> for what is still missing (persistent log, dispatch channels,
> subscriptions).

## 1. What this module is for

Sends out-of-band alerts for interesting events: a large transaction,
a newly-blocked account, a repeated idempotency-key collision, etc.
Dispatch channels are abstracted (email, push, in-app banner);
`notifications` does NOT hard-code which channel a given event goes
to — that is policy configured in this module's own tables.

This is the **canonical event-consuming module**: it has no inbound
module dependencies and no other module depends on it. Changes here
never force a rebuild of `accounts` or `transactions`.

## 2. Tables owned

- `notification_subscriptions` — per-account opt-in/opt-out state
  (TBD — to be designed alongside Phase 3).
- `notification_log` — append-only history of what was sent when,
  used for deduplication and user-facing audit.

Neither table is read by any other module.

## 3. Ports exposed (planned)

`notifications::ports` will expose a thin synchronous dispatch
surface for the rare case that another module needs to *trigger* a
notification directly rather than via an event:

- `NotificationDispatcher` trait — `dispatch(kind, recipient, payload)`.
- DTOs — `NotificationKind`, `Recipient`.

Expected usage is still event-driven; this port exists for
completeness.

## 4. Ports consumed

**None** at compile time. Coupling to other modules is exclusively
through `shared_kernel::events`, which is type-agnostic from this
module's perspective (we deserialize event payloads defined in the
shared kernel, not in other modules).

## 5. Events published

- `NotificationSent { id, kind, recipient, channel, sent_at }` —
  emitted after a successful dispatch. Useful for downstream audit
  / analytics; currently no consumer.

## 6. Events consumed

From `shared_kernel::events`:

- `TransactionCommitted` — published by `transactions`.
- `AccountStatusChanged` — published by `accounts`.
- `AccountBalanceChanged` — published by `accounts` (filtered to
  "significant" deltas before dispatch).

The fact that none of these event names name a module is intentional
— consumers depend on the shared-kernel event catalogue, not on the
publishing module's code.

## 7. Operational notes

- All outbound dispatch paths MUST be idempotent at the provider
  level (e.g. same `message_id` → same outcome) because
  `notification_log` is eventually-consistent.
- A notification storm during incident recovery (e.g. mass
  balance-changed replays) is throttled via a token-bucket per
  recipient — see `shared_kernel::rate_limit::PerKeyBucket`.
