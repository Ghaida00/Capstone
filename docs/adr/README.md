# Architecture Decision Records

Each ADR captures **one** decision: the context that forced it, the
options considered, what was chosen, and the consequences. ADRs are
**immutable** — once merged, never edited. If a decision is reversed
or refined, write a new ADR that supersedes the old one and update
the index below.

## Index

| #     | Title                                                        | Status      |
|-------|--------------------------------------------------------------|-------------|
| 0001  | [Modular monolith first, microservices later](0001-modular-monolith.md) | Accepted    |
| 0002  | [Cargo workspace split for compiler-enforced module boundaries](0002-cargo-workspace-split.md) | Accepted    |
| 0003  | [Port-and-adapter shape per module](0003-port-adapter-shape.md) | Accepted    |
| 0004  | [In-process event bus, AMQP swap deferred](0004-in-process-event-bus.md) | Accepted    |
| 0005  | [Patroni over pg_auto_failover](0005-patroni-over-pg-auto-failover.md) | Accepted    |
| 0006  | [HAProxy `GET /primary` for write routing](0006-haproxy-primary-routing.md) | Accepted    |
| 0007  | [v1 → v2 hard cutover, no alias router](0007-v1-cull-and-rename.md) | Accepted    |

## Format

Each ADR follows this skeleton:

```
# NNNN — Title

**Status:** Accepted | Superseded by NNNN
**Date:** YYYY-MM-DD

## Context
What forces this decision. Constraints, prior state, what hurts today.

## Decision
What we are doing. One paragraph max.

## Consequences
What changes as a result — good and bad. Include the rollback path.

## Alternatives considered
What we rejected and why.
```

Keep ADRs ≤ 1 page. If you need more space, you are explaining the
*how* (which belongs in code) not the *why* (which belongs here).
