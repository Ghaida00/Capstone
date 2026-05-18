# Independent Bug Hunt — Design Spec

**Date:** 2026-05-18
**Author:** Ghaida (m4gung184@gmail.com), with Claude
**Project:** Capstone — Exploding User Data Scalabilities (Topik B.4)
**Branch:** `Ghaida` @ HEAD
**Status:** Awaiting user review

---

## 1. Purpose

Run an **independent, evidence-driven bug hunt** across the entire Capstone codebase. Produce a single master report where every claim is backed by a runnable probe whose output would FAIL if the claim were wrong.

Distinct from the [2026-05-16 gap audit](2026-05-16-best-practice-gap-audit-design.md): that audit measured *distance from best practice*. This hunt looks for *concrete defects already present* — bugs, vulnerabilities, dead code, broken wiring, contradictions, panic paths, races, money-safety holes.

## 2. Non-goals

- **No fixes in this pass.** Findings get a *recommended* fix (with reference) but no source change is committed by hunters or verifiers.
- **No consultation of the prior audit.** Hunters are told to ignore `docs/audit/` for independence. Re-discovery of an audit finding is acceptable evidence that it is still real.
- **No coverage of `outliers.json` (2.5 GB), `target/`, `Cargo.lock`.** These are data/build artifacts.
- **No new feature design.** This is purely defect discovery.

## 3. What counts as a finding

A finding is anything the hunter can show with file+line evidence that fits one of:

| Class | Examples |
|-------|----------|
| **Bug** | Logic error, off-by-one, type/coercion error, wrong status code, misuse of API |
| **Vulnerability** | OWASP class issue: AuthN/AuthZ gap, injection, SSRF, IDOR, secret leak, unsafe deserialization, missing/weak crypto, header miss |
| **Money-safety** | Decimal scale loss, currency mismatch, balance non-conservation, double-spend race, outbox/inbox dedupe hole, idempotency replay hole |
| **Concurrency** | Lock held across `.await`, races, cancellation unsafety, orphan task, channel misuse, deadlock ordering, `Send/Sync` violation |
| **Panic/Error path** | `unwrap`/`expect` on fallible operations on critical paths, swallowed errors, `anyhow` where typed error is required, missing `?`, panic on bad input |
| **Resilience defect** | Retry without backoff, breaker that never opens, timeout that doesn't propagate, hedged request that doesn't cancel |
| **Dead/Unused** | Unused dependency, unreachable code, unused Cargo feature, dead public API, dangling reference in docs/code |
| **Contradiction** | README/docs claim ≠ code reality, ADR ≠ implementation, comment lies about behavior, env var documented but unused (or used but undocumented) |
| **Infra/Wiring** | docker-compose dep cycle, broken healthcheck, port collision, migration safety bug, pgbouncer mode mismatch, redis sentinel misconfig, nginx upstream typo, prometheus rule error, OTel exporter wrong URL, grafana broken panel |

Severity scale: 🔴 critical/security/money · 🟡 correctness or operations · 🟢 hygiene/minor · ℹ️ informational.

## 4. Method

Three phases. Phase A and C run in parallel via sub-agents (`dispatching-parallel-agents`). Phase B and D are synchronous orchestration steps.

### Phase A — 9 hunters in parallel

Each hunter has a single beat. Hunters never read each other's output (independent re-discovery is fine — Phase B dedupes). All hunters are told to **ignore `docs/audit/` entirely.**

Each hunter writes one file: `docs/audit/2026-05-18-hunt-<beat>.md`, one row per finding, strict format:

```
| ID | Sev | Title | File:Line | Evidence (≤3 lines, quoted) | Hypothesised impact |
```

No fix proposals in Phase A — those come from verifiers in Phase C.

**Hunter beats:**

| # | Beat | Primary scope | Looks for |
|---|------|---------------|-----------|
| H1 | Money math & types | `crates/transactions`, `crates/accounts`, `crates/shared_kernel`, `db/` | Decimal scale/rounding, balance arithmetic, money conservation, currency mix, sign/overflow, ledger entry invariants |
| H2 | Idempotency & outbox/inbox/saga | `crates/transactions`, `crates/notifications`, `db/`, `crates/shared_kernel` | Idempotency replay holes, outbox cooperative-lease bugs, inbox dedupe gaps, cross-shard saga reconciliation defects, replay attacks |
| H3 | Concurrency & async | All `crates/` | `Mutex`/`RwLock` across `.await`, races, cancellation safety, orphan `tokio::spawn`, channel misuse, deadlock orderings, `Send/Sync` violations |
| H4 | Auth & secrets | All `crates/`, `nginx/`, `haproxy/`, `Dockerfile`, `.env*` | JWT (alg/aud/iss/exp/rotation), AuthZ per route, session, secret leaks in logs/env/git, TLS config |
| H5 | Input validation & injection | All `crates/`, OpenAPI, route handlers | SQLi (string-built queries), XSS in any HTML/template, SSRF on outbound, command injection, unsafe deserialization, path traversal, IDOR via user-controlled keys |
| H6 | Error handling & resilience correctness | All `crates/` | `unwrap`/`expect` on fallible critical-path ops, swallowed errors, `anyhow` placement, missing `?`, panic-on-bad-input, retry without backoff/jitter, breaker that never trips, timeout not propagated, hedged-request leak |
| H7 | Dead/unused/contradictions | Everything (excluding excluded paths) | Unused deps, unreachable code, unused Cargo features, dead public APIs, dangling refs, doc↔code contradictions, stale comments, unused env vars |
| H8 | Infra data-plane | `db/`, `docker-compose.yml`, `redis/`, pgbouncer/Patroni configs, migrations | Compose dep cycles, broken healthchecks, env typos, port collisions, migration safety, pgbouncer mode mismatch, redis sentinel wiring, replica routing, pool sizing |
| H9 | Infra edge & observability | `nginx/`, `haproxy/`, `prometheus/`, `otel/`, `grafana/`, `k6/`, OpenAPI | Nginx upstream/timeout bugs, HAProxy stickiness, Prometheus rule syntax & label cardinality, OTel exporter URL/config, Grafana broken refs, k6 thresholds vs. SLO, API contract drift |

### Phase B — Aggregate & route (synchronous)

Orchestrator (Claude main session):

1. Reads all 9 hunter reports.
2. Deduplicates overlapping findings (e.g. H3 and H6 may both flag a panic-on-error path; record once, note both hunters).
3. Tags each finding with one of five **probe types** (see Phase C).
4. Writes `docs/audit/2026-05-18-hunt-verification-queue.md` — the master input for verifiers.

### Phase C — 5 verifiers in parallel

Each verifier picks up only the findings tagged for its probe type. Splitting by probe type (not by hunter) means each verifier specialises in one set of tools.

For each finding the verifier:
1. Defines the probe: a command/test that produces a binary signal (pass = bug refuted, fail = bug confirmed).
2. Runs the probe.
3. Records the verdict in its own per-verifier file `docs/audit/2026-05-18-hunt-verified-v{1..5}.md` (orchestrator merges these in Phase D):
   - **CONFIRMED** — probe output proves the bug; include a *recommended fix* and one authoritative reference (Rust API docs, OWASP, Postgres docs, RFC, etc).
   - **REFUTED** — hunter was wrong; explain why, quote the contradicting evidence.
   - **INCONCLUSIVE** — probe couldn't isolate the claim; describe what additional info is needed.

| # | Verifier | Tools | Picks up findings of type |
|---|----------|-------|----------------------------|
| V1 | Compile/Test/Type | `cargo check`, `cargo test`, `cargo clippy -D warnings`, minimal failing unit/integration tests | Logic bugs, type bugs, panic paths, claimed-unreachable code |
| V2 | Static / Dead-code | `cargo machete`, `cargo udeps` (nightly), `cargo deny`, ripgrep call-graphs, AST greps | Dead code/deps, unused features, dangling refs |
| V3 | Property/Invariant | `proptest`, `quickcheck`, loom (for concurrency), small fuzzers | Money invariants, races, idempotency, dedupe |
| V4 | Live runtime — owns docker stack | `docker compose up -d`, `curl`, `psql`, `redis-cli`, `nginx -t` against running container | Security exploits, AuthN/AuthZ bypass, infra runtime wiring, CORS/header verification |
| V5 | Config / Cross-ref | `docker compose config`, `promtool check rules`, parse YAML/TOML/JSON, grep docs↔code | Config drift, contradictions, API contract drift, README↔reality |

**V4 owns the docker stack.** Before starting work, V4 checks `docker compose ps`. If the stack is already up, V4 does not tear it down; it uses what's there. Otherwise V4 brings it up with `docker compose up -d`. V4 leaves the stack running so that other verifiers (rare, but possible — e.g. V3 may want a live DB) can hit it.

### Phase D — Final report

Single deliverable: `docs/audit/2026-05-18-hunt-verified.md`.

Structure:

1. Executive summary: total findings, CONFIRMED / REFUTED / INCONCLUSIVE counts by severity.
2. Confirmed findings, sorted by severity desc, then category. Each row: ID · category · severity · title · file:line · probe-cmd · probe-output (≤5 lines) · recommended fix · reference.
3. Refuted findings: intellectual-honesty section, mirrors the prior audit's "heatmap corrections" pattern. Each row: ID · why it was wrong · contradicting evidence.
4. Inconclusive findings: what would be needed to confirm/refute.
5. Cross-cutting themes: findings touched by ≥2 hunters (independent triangulation = highest defensibility).
6. Independence cross-check: which CONFIRMED findings overlap with the prior 2026-05-16 audit (computed at the END only, not consulted during the hunt itself).

## 5. Output artifacts

| File | Phase | Owner |
|------|-------|-------|
| `docs/audit/2026-05-18-hunt-h1-money.md` … `-h9-edge-obs.md` | A | Each hunter |
| `docs/audit/2026-05-18-hunt-verification-queue.md` | B | Orchestrator |
| `docs/audit/2026-05-18-hunt-verified.md` | C/D | Verifiers append; orchestrator finalises |
| `docs/superpowers/specs/2026-05-18-bug-hunt-design.md` | (this file) | Pre-hunt |

## 6. Independence guards

- Every hunter prompt contains: "Do not read `docs/audit/`. Do not consult the 2026-05-16 audit." Hunters work from a closed brief.
- The orchestrator does the cross-check with the prior audit at the END (Phase D §6). An independently-rediscovered finding from a closed brief is the strongest evidence that the prior finding is still real.
- Hunters do not read each other's outputs.
- Verifiers may read hunter outputs (their input) but must independently produce probe evidence. A finding cited by a hunter without independent probe confirmation is **not** marked CONFIRMED.

## 7. Risks & mitigations

| Risk | Mitigation |
|------|-----------|
| 9 sub-agents racing `cargo` builds → disk thrash | Only H7 will likely shell out to cargo; others mostly read source. Hunters told: prefer `Grep`/`Read` over `cargo`. |
| Docker stack state unknown / already running | V4 checks `docker compose ps` before any `up`/`down`. V4 never tears down a stack it didn't start. |
| Verifier output collisions in `…hunt-verified.md` | Verifiers append to *separate* per-verifier files (`…hunt-verified-v1.md` etc.); orchestrator merges in Phase D. |
| Wall-clock budget | Estimate: hunters ≈ 20-30 min each (parallel), verifiers ≈ 15-40 min each (parallel). Total ≈ 1–2h. Acceptable. |
| `outliers.json` (2.5 GB) accidentally grepped | Explicit "ignore `outliers.json`, `target/`, `Cargo.lock`" in every hunter prompt. |
| Empirical-verification rule (memory: `feedback_empirical_verification_at_checkpoints`) | Probe-that-would-FAIL-if-wrong is mandatory in verifier prompt; no diff-inspection-only confirmations. |

## 8. Definition of done

- 9 hunter reports written and committed.
- 1 verification queue written and committed.
- 1 master verified report written and committed, with every finding marked CONFIRMED / REFUTED / INCONCLUSIVE.
- Every CONFIRMED finding has a probe command + actual output + recommended fix + authoritative reference.
- A short Slack-ready summary at the top of the master report.

## 9. Out of scope (explicit)

- No code/config edits to fix findings. (User may follow up with a separate remediation plan; that is a different design.)
- No re-running of the existing 2026-05-16 audit's metrics/SLO measurement methodology.
- No load testing beyond what k6 already contains.

---

*Spec written 2026-05-18. To be committed before Phase A dispatch.*
