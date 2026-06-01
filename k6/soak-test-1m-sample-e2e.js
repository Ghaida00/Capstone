import http from "k6/http";
import { check, sleep, group } from "k6";
import { Rate, Counter, Trend } from "k6/metrics";
import { buildHandleSummary } from "./lib/summary.js";

// ─── Soak test: 1M/hour for hours, accept baseline + sampled e2e ──
// Sibling to soak-test-1m.js and load-test-1m-sample-e2e.js. Same
// arrival shape (278 rps ≈ 1M/hour) held for SOAK_HOURS, same
// payload mix, same replay rate. Adds the same sample-based end-to-
// end measurement: 5% of fresh accepted transactions are polled to
// terminal status and the per-request wall-clock from "request
// issued" to "completed/failed" is recorded into `transaction_e2e_ms`.
//
// Why sampling and not full e2e: full e2e at 278 rps adds
// ~1400-2800 status polls/sec, which over a multi-hour soak both
// blows past the nginx per-IP rate limit (NGINX_PER_IP_RATE=2000)
// and turns the run into a poll-storm test. 5% sampling adds only
// ~14 polls/sec; total client traffic stays ~620 rps.
//
// What the soak adds over the 13-minute e2e run: it answers "does
// end-to-end completion latency DRIFT over hours?" — a consumer
// that slowly falls behind, an outbox that grows unbounded, or a
// batch flush that degrades shows up as a rising `transaction_e2e_ms`
// p99 and a climbing timeout rate, neither visible in 13 minutes.

const transactionCreated = new Counter("transactions_created");
const transactionRead = new Counter("transactions_read");
const errorRate = new Rate("error_rate");
const idempotencyReplayAttempts = new Counter("idempotency_replay_attempts");

// End-to-end latency Trend (sampled). Trend so k6 reports
// p50/p95/p99. Replays are NOT recorded — they hit the
// idempotency cache and return ~instantly, which would skew the
// distribution optimistic.
const transactionE2eMs = new Trend("transaction_e2e_ms", true);
const transactionsE2eCompleted = new Counter("transactions_e2e_completed");
const transactionsE2eTimeout = new Counter("transactions_e2e_timeout");
// Timeout as a RATE (timeouts / e2e samples). Duration-independent,
// unlike a fixed count: over an 8h soak the sample count is ~100×
// the 13-minute run's, so a `count<N` gate would be meaningless.
// This gates the *fraction* of sampled txns that never reach a
// terminal state inside E2E_POLL_TIMEOUT_MS.
const transactionsE2eTimeoutRate = new Rate("transactions_e2e_timeout_rate");

// ─── Configuration ─────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const NUM_ACCOUNTS = 100000;
const BALANCE_POLL_POOL_SIZE = 100;

// 5% of fresh accepts → e2e poll. Override via env var if needed:
//   k6 run -e E2E_SAMPLE_RATE=0.10 k6/soak-test-1m-sample-e2e.js
const E2E_SAMPLE_RATE = parseFloat(__ENV.E2E_SAMPLE_RATE || "0.05");
const E2E_POLL_TIMEOUT_MS = 10000;
const E2E_POLL_INTERVAL_MS = 100;

// SOAK_HOURS sets the sustained hold (default 8h). Override:
//   k6 run -e SOAK_HOURS=4 k6/soak-test-1m-sample-e2e.js
// Non-positive / non-numeric input falls back to 8h.
const SOAK_HOURS = (() => {
  const h = parseFloat(__ENV.SOAK_HOURS || "8");
  return isFinite(h) && h > 0 ? h : 8;
})();
const WARMUP_SEC = 120; // 2m ramp 50 → 278 rps
const COOLDOWN_SEC = 120; // 2m ramp 278 → 0
const SOAK_SEC = Math.round(SOAK_HOURS * 3600);
const TOTAL_SEC = WARMUP_SEC + SOAK_SEC + COOLDOWN_SEC;

function randomAccount() {
  const i = Math.floor(Math.random() * NUM_ACCOUNTS) + 1;
  return `ACC_${String(i).padStart(7, "0")}`;
}

function balancePollAccount() {
  const i = Math.floor(Math.random() * BALANCE_POLL_POOL_SIZE) + 1;
  return `ACC_${String(i).padStart(7, "0")}`;
}

const REPLAY_BUFFER_SIZE = 32;
let replayBuffer = [];

function rememberRequest(req) {
  replayBuffer.push(req);
  if (replayBuffer.length > REPLAY_BUFFER_SIZE) {
    replayBuffer.shift();
  }
}

function pickReplayRequest() {
  if (replayBuffer.length === 0) return null;
  return replayBuffer[Math.floor(Math.random() * replayBuffer.length)];
}

// ─── Test Scenarios ────────────────────────────────────────

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)", "p(99.9)", "count"],

  tags: {
    environment: __ENV.K6_ENV || "local",
    run_id: __ENV.RUN_ID || String(Date.now()),
    git_sha: __ENV.GIT_SHA || "dev",
  },

  // Reuse TCP connections across iterations (keep-alive). This is
  // the k6 default — stated explicitly for reproducibility across
  // k6 versions and to avoid ephemeral-port / TIME_WAIT exhaustion
  // at 278 rps against loopback.
  noConnectionReuse: false,
  insecureSkipTLSVerify: false,

  scenarios: {
    soak_1m_per_hour: {
      // Identical arrival shape to soak-test-1m.js. 5% e2e sampling
      // adds only ~14 polls/sec, so no VU pool bump needed (each
      // sampled VU spends up to ~1s extra in pollStatus).
      executor: "ramping-arrival-rate",
      startRate: 50,
      timeUnit: "1s",
      preAllocatedVUs: 300,
      maxVUs: 1500,
      stages: [
        { duration: `${WARMUP_SEC}s`, target: 278 },
        { duration: `${SOAK_SEC}s`, target: 278 },
        { duration: `${COOLDOWN_SEC}s`, target: 0 },
      ],
      exec: "txWorkload",
      gracefulStop: "30s",
      tags: { scenario: "soak_1m_per_hour" },
    },

    balance_poll: {
      executor: "constant-vus",
      vus: 5,
      duration: `${TOTAL_SEC - 10}s`,
      startTime: "10s",
      exec: "balancePollWorkload",
      tags: { scenario: "balance_poll" },
    },
  },

  thresholds: {
    // Same accept-side SLOs as soak-test-1m. The aborting gate is
    // scoped to the transaction scenario so a balance-poll blip
    // can't abort the money-path run; the balance-poll gate reports
    // a breach in the summary but does NOT abort.
    "http_req_failed{scenario:soak_1m_per_hour}": [
      { threshold: "rate<0.05", abortOnFail: true, delayAbortEval: "2m" },
    ],
    "http_req_failed{scenario:balance_poll}": ["rate<0.05"],
    http_req_duration: ["p(95)<500", "p(99)<1500"],
    // `error_rate` is still collected (tagged per `endpoint`) for
    // Grafana slicing; no global threshold here — it would only
    // duplicate the `http_req_failed` gates above.

    "http_req_duration{name:GET /api/v2/accounts/:id/balance}": ["p(50)<3", "p(95)<10"],
    "http_req_duration{name:POST /api/v2/transactions}": ["p(50)<10", "p(95)<50", "p(99)<150"],
    "http_req_duration{name:GET /api/v2/transactions}": ["p(95)<50"],

    // End-to-end SLOs. Bound by consumer batch fill + flush cadence
    // (~200 txn batches at 278 rps fill every ~0.7 s). A fresh
    // transaction averages ~0.5–1 s typical, up to ~1.5 s worst.
    // p99<5000 ms allows headroom for batch-timeout flushes during
    // load lulls. Over a soak, the value of these is trend stability:
    // a creeping p99 means the consumer is falling behind.
    "transaction_e2e_ms": [
      "p(50)<1500",
      "p(95)<3000",
      "p(99)<5000",
    ],
    // Stuck transactions as a fraction of e2e samples. Anything
    // beyond E2E_POLL_TIMEOUT_MS (10 s) is genuinely wedged. A
    // duration-independent rate (vs. the load test's fixed count)
    // because the soak's sample count scales with SOAK_HOURS. If
    // this trips, the consumer or outbox is wedged, not just slow.
    "transactions_e2e_timeout_rate": ["rate<0.005"],
  },
};

// ─── Helpers ───────────────────────────────────────────────

const MAX_LOGGED_ERRORS = 5;
const LOG_VU_CEILING = 200;
let loggedErrors = 0;

function logFailure(context, res) {
  if (__VU > LOG_VU_CEILING) return;
  if (loggedErrors >= MAX_LOGGED_ERRORS) return;
  loggedErrors++;

  const bodyPreview = res.body
    ? String(res.body).substring(0, 200)
    : "(empty body)";
  console.warn(
    `⚠️  [${context}] status=${res.status} | ` +
      `error_code=${res.error_code} | ` +
      `body=${bodyPreview}`
  );
}

// Tag for setup-phase status polls.
const SETUP_POLL_PARAMS = {
  tags: { name: "GET /api/v2/transactions/status/:ref (setup poll)" },
};

// Tag for in-load e2e sample polls. Distinct from setup so each
// shows its own latency distribution in the k6 summary, and so
// http_req_duration threshold buckets don't conflate the two.
const E2E_POLL_PARAMS = {
  tags: { name: "GET /api/v2/transactions/status/:ref (e2e sample poll)" },
};

// Poll /status/{ref} until terminal. Returns "completed" / "failed"
// on success, or null on timeout. The `params` argument lets the
// caller tag the poll requests distinctly (setup vs in-load e2e).
function waitForCompletion(refId, params, timeoutMs, pollMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = http.get(
      `${BASE_URL}/api/v2/transactions/status/${refId}`,
      params,
    );
    if (res.status === 200) {
      try {
        const body = JSON.parse(res.body);
        const status = body && body.data && body.data.status;
        if (status === "completed" || status === "failed") {
          return status;
        }
      } catch (e) {
        /* fall through to retry */
      }
    }
    sleep(pollMs / 1000);
  }
  return null;
}

// ─── Setup ─────────────────────────────────────────────────
export function setup() {
  console.log(`🎯 Target: ${BASE_URL}`);
  console.log(`📊 Running: ${SOAK_HOURS}h soak @ 1M/hour + ${(E2E_SAMPLE_RATE * 100).toFixed(1)}% e2e sample + balance poll`);
  console.log(`⏱️  Total wall-clock: ~${(TOTAL_SEC / 3600).toFixed(2)}h (incl. 2m warmup + 2m cooldown)`);
  console.log(`👥 Using ${NUM_ACCOUNTS} pre-seeded accounts (ACC_0000001 – ACC_0100000)\n`);

  const health = http.get(`${BASE_URL}/health`);
  if (health.status !== 200) {
    throw new Error(
      `Health check failed (status ${health.status}). ` +
        `Make sure docker-compose is running.`
    );
  }
  console.log(`✅ Health check passed`);

  const balanceRes = http.get(`${BASE_URL}/api/v2/accounts/ACC_0000001/balance`);
  if (balanceRes.status !== 200) {
    throw new Error(
      `Account ACC_0000001 not found (status ${balanceRes.status}). ` +
        `Seed accounts first via db/init.sql or k6/seed-accounts.sql.`
    );
  }
  console.log(`✅ Accounts verified (ACC_0000001 exists)`);

  console.log(`\n🔍 End-to-end smoke test (create + wait for consumer)...`);
  const setupRefId = `setup-soak-sampled-${__ENV.HOSTNAME || "host"}-${Date.now()}`;
  const testPayload = JSON.stringify({
    from_account: "ACC_0000001",
    to_account: "ACC_0000002",
    amount: "1.00",
    currency: "IDR",
    reference_id: setupRefId,
    description: "k6 soak sampled-e2e setup diagnostic test",
  });

  const createRes = http.post(`${BASE_URL}/api/v2/transactions`, testPayload, {
    headers: { "Content-Type": "application/json" },
  });

  console.log(`   Create status: ${createRes.status}`);
  if (createRes.status < 200 || createRes.status >= 300) {
    throw new Error(
      `Transaction create returned ${createRes.status}. ` +
        `Body: ${String(createRes.body).substring(0, 300)}`
    );
  }

  const finalStatus = waitForCompletion(setupRefId, SETUP_POLL_PARAMS, 30000, 250);
  if (finalStatus === null) {
    throw new Error(
      `Consumer did not process setup transaction within 30s. ` +
        `Either RabbitMQ/consumer is wedged, or a prior run's ` +
        `backlog hasn't drained yet — wait 1–3 min and retry.`
    );
  }
  console.log(`✅ Consumer processed setup tx → status=${finalStatus}\n`);

  return {};
}

// ─── Main workload ─────────────────────────────────────────
export function txWorkload() {
  let referenceIdForE2e = null;
  let txStartTime = null;
  let isFreshAccepted = false;

  group("Create Transaction", () => {
    let payloadObj;
    let isReplay = false;
    if (Math.random() < 0.05 && replayBuffer.length > 0) {
      payloadObj = pickReplayRequest();
      isReplay = true;
      idempotencyReplayAttempts.add(1);
    } else {
      let fromAccount = randomAccount();
      let toAccount = randomAccount();
      while (toAccount === fromAccount) {
        toAccount = randomAccount();
      }
      const amount = (Math.random() * 1000 + 1).toFixed(2);
      const referenceId = `${__VU}-${__ITER}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      payloadObj = {
        from_account: fromAccount,
        to_account: toAccount,
        amount,
        currency: "IDR",
        reference_id: referenceId,
        description: `k6 soak sampled-e2e test`,
      };
    }

    const payload = JSON.stringify(payloadObj);
    const params = {
      headers: { "Content-Type": "application/json" },
      tags: { name: "POST /api/v2/transactions" },
    };

    // Capture t0 BEFORE the POST so any e2e sample we take below
    // includes the accept latency (honest tap-to-Successful clock).
    txStartTime = Date.now();
    const res = http.post(`${BASE_URL}/api/v2/transactions`, payload, params);
    const isAccepted = res.status >= 200 && res.status < 300;

    check(res, {
      "create status 2xx": (r) => r.status >= 200 && r.status < 300,
      "create has reference_id": (r) => {
        if (r.status < 200 || r.status >= 300) return false;
        try {
          const body = JSON.parse(r.body);
          return !!(body.data && body.data.reference_id);
        } catch (e) {
          return false;
        }
      },
      "not rate limited": (r) => r.status !== 429,
      "not overloaded": (r) => r.status !== 503,
    });

    errorRate.add(!isAccepted, { endpoint: "POST /api/v2/transactions" });

    if (isAccepted) {
      transactionCreated.add(1);
      if (!isReplay) {
        rememberRequest(payloadObj);
        referenceIdForE2e = payloadObj.reference_id;
        isFreshAccepted = true;
      }
    } else {
      logFailure("Create Transaction", res);
    }
  });

  // ── E2E sample (5% of fresh accepts) ──
  // Replays are excluded (cached response → would skew Trend toward
  // 0 ms). Failed accepts skip naturally because isFreshAccepted
  // stays false. The Math.random() gate lives outside the group so
  // un-sampled iters don't pay any group-tracking overhead.
  if (isFreshAccepted && referenceIdForE2e && Math.random() < E2E_SAMPLE_RATE) {
    group("E2E Sample", () => {
      const finalStatus = waitForCompletion(
        referenceIdForE2e,
        E2E_POLL_PARAMS,
        E2E_POLL_TIMEOUT_MS,
        E2E_POLL_INTERVAL_MS,
      );
      if (finalStatus === "completed" || finalStatus === "failed") {
        transactionE2eMs.add(Date.now() - txStartTime);
        transactionsE2eCompleted.add(1);
        transactionsE2eTimeoutRate.add(false);
      } else {
        // Timeout — don't pollute the Trend with a fixed-ceiling
        // value; count it and feed the rate gate so drift is caught.
        transactionsE2eTimeout.add(1);
        transactionsE2eTimeoutRate.add(true);
      }
    });
  }

  group("List Transactions", () => {
    let url = `${BASE_URL}/api/v2/transactions?limit=10`;
    if (Math.random() < 0.5) {
      const minutesBack = Math.floor(Math.random() * 60) + 1;
      const cursor = new Date(Date.now() - minutesBack * 60 * 1000).toISOString();
      url += `&before=${encodeURIComponent(cursor)}`;
    }

    const res = http.get(url, { tags: { name: "GET /api/v2/transactions" } });
    const ok = res.status === 200;

    check(res, {
      "list status 200": (r) => r.status === 200,
      "list returns array": (r) => {
        if (r.status !== 200) return false;
        try {
          const body = JSON.parse(r.body);
          return body.success && Array.isArray(body.data);
        } catch (e) {
          return false;
        }
      },
    });

    errorRate.add(!ok, { endpoint: "GET /api/v2/transactions" });

    if (ok) {
      transactionRead.add(1);
    } else {
      logFailure("List Transactions", res);
    }
  });
}

// ─── Balance-poll workload (separate scenario, low VU) ─────
export function balancePollWorkload() {
  const acc = balancePollAccount();
  const res = http.get(`${BASE_URL}/api/v2/accounts/${acc}/balance`, {
    tags: { name: "GET /api/v2/accounts/:id/balance" },
  });
  check(res, {
    "balance status 200": (r) => r.status === 200,
  });
  const balanceFailed = res.status !== 200;
  errorRate.add(balanceFailed, { endpoint: "GET /api/v2/accounts/:id/balance" });
  if (balanceFailed) {
    logFailure("Balance Poll", res);
  }
  sleep(0.1);
}

// ─── Teardown ──────────────────────────────────────────────
export function teardown() {
  const expectedIters = Math.round(278 * SOAK_SEC);
  console.log(`\n📈 Soak test complete!`);
  console.log(`   Check Grafana dashboard: http://localhost:3001`);
  console.log(`   E2E sample Trend: 'transaction_e2e_ms' in k6 summary`);
  console.log(`   Sample size: ~${Math.round(expectedIters * E2E_SAMPLE_RATE)} of ~${expectedIters} iters`);
  console.log(`   Watch for DRIFT: a rising e2e p99 over the run means the consumer is falling behind.`);
}

// Writes <run>.summary.json + .txt to k6/output/ on every run and
// re-emits the report to stdout. See k6/lib/summary.js.
export const handleSummary = buildHandleSummary("soak-test-1m-sample-e2e");
