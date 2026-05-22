# Runbook: <Symptom seen by ops>

**Last reviewed:** YYYY-MM-DD
**Owner:** <team or @person>
**Related ADRs / code:** <links>

## Symptom

What the on-call sees first: alert name, log pattern, customer
complaint shape. Be concrete — name the exact alert and metric.

## Severity & blast radius

- Customer-visible? (yes / no / partial)
- Reversible? (yes / no / with-data-loss)
- Estimated time-to-degraded / time-to-broken if untreated.

## Detect / confirm

- Prometheus query / Grafana panel
- Log query (`docker compose logs`)
- DB query if applicable

## Mitigate (stop the bleed)

Steps in order. Each step has "expected result" and "if not, jump to".

## Recover (return to normal)

Steps in order.

## Rollback

What to revert if the mitigation made things worse.

## Postmortem checklist

- Was the alert timely? Was the playbook accurate? Were there
  surprises that should be linked back into this runbook?
