# `notifications` — out-of-band alerts

Event-consuming module. Subscribes to `shared_kernel::events`, has no
inbound or outbound module dependencies. Changes here never force a
rebuild of `accounts` or `transactions`.

For module shape see [ADR-0003](../../docs/adr/0003-port-adapter-shape.md).
For why the bus is in-process today see [ADR-0004](../../docs/adr/0004-in-process-event-bus.md).

## Tables owned

- `notification_subscriptions` — per-account opt-in/opt-out (planned).
- `notification_log` — append-only history (planned; in-memory ring
  buffer today).

## Ports exposed (planned)

- `NotificationDispatcher` trait — `dispatch(kind, recipient, payload)`
- DTOs: `NotificationKind`, `Recipient`

For the rare case another module needs to trigger a notification
directly. Expected usage stays event-driven.

## Ports consumed

None at compile time. Coupling to other modules is exclusively through
`shared_kernel::events`, which is type-agnostic from this module's
perspective.

## Events consumed

From `shared_kernel::events`:
- `TransactionCommitted` — published by `transactions`
- `AccountStatusChanged` — published by `accounts` (planned)
- `AccountBalanceChanged` — published by `accounts` (planned, filtered to "significant" deltas)

Consumers depend on the shared-kernel event catalogue, not on the
publishing module's code.

## Events published

- `NotificationSent { id, kind, recipient, channel, sent_at }` — after
  successful dispatch. No consumer today.

## HTTP surface

`GET /api/v2/notifications/recent?limit=N` — returns the newest entries
from the in-memory ring buffer (cap 512).

## Operational notes

- All outbound dispatch paths must be idempotent at the provider level
  (same `message_id` → same outcome) because `notification_log` will
  be eventually-consistent.
- A notification storm is throttled per-recipient via a token bucket
  (`shared_kernel::rate_limit::PerKeyBucket`).
- **Restarts lose history.** Switch to a sibling
  `infrastructure/repository.rs` selected at `init` time when
  persistence becomes load-bearing.
