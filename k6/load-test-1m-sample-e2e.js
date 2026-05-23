import http from "k6/http";
import { check, sleep, group } from "k6";
import { Rate, Counter, Trend } from "k6/metrics";

// ─── Load test: 1M/hour, accept-only baseline + sampled e2e ──
// Sibling to load-test-1m.js. Same arrival shape (~278 rps for
// 13 min sustained), same payload mix, same replay rate. Adds a
// sample-based end-to-end measurement: 5% of fresh accepted
// transactions are also polled to terminal status, and the
// per-request wall-clock from "request issued" to
// "completed/failed" is recorded into `transaction_e2e_ms`.
//
// Why sampling and not full e2e (the now-removed load-test-e2e.js):
//
//   - Full e2e at 278 rps added ~1400-2800 status polls/sec.
//     That blew through the production-safe nginx per-IP rate
//     limit (NGINX_PER_IP_RATE=2000 in .env) and dominated total
//     read load on /status. The "what's our e2e under load?"
//     question got polluted with "...with the system also under
//     a self-inflicted poll storm".
//   - 5% sampling adds only ~14 polls/sec — total client traffic
//     stays at ~620 rps, comfortably under the per-IP ceiling.
//   - 5% × 217k iters over 13 min ≈ 10,850 samples. Plenty for
//     stable p50/p95/p99 without saturating the system from one
//     k6 host.
//
// The accept-side SLOs are identical to load-test-1m so this
// script also serves as a stricter superset: regressions on
// accept latency AND end-to-end completion are both caught.

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

// ─── Configuration ─────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const NUM_ACCOUNTS = 100000;
const BALANCE_POLL_POOL_SIZE = 100;

// 5% of fresh accepts → e2e poll. Override via env var if needed:
//   k6 run -e E2E_SAMPLE_RATE=0.10 k6/load-test-1m-sample-e2e.js
const E2E_SAMPLE_RATE = parseFloat(__ENV.E2E_SAMPLE_RATE || "0.05");
const E2E_POLL_TIMEOUT_MS = 10000;
const E2E_POLL_INTERVAL_MS = 100;

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

  noConnectionReuse: false,
  insecureSkipTLSVerify: false,

  scenarios: {
    sustained_1m_per_hour: {
      // Identical to load-test-1m.js. 5% e2e sampling adds only
      // ~14 polls/sec, so no VU pool bump needed (each sampled VU
      // spends ~1 extra second in pollStatus → ~14 concurrent
      // VUs additional × 300 preAllocated is plenty).
      executor: "ramping-arrival-rate",
      startRate: 50,
      timeUnit: "1s",
      preAllocatedVUs: 300,
      maxVUs: 1500,
      stages: [
        { duration: "1m",  target: 278 },
        { duration: "13m", target: 278 },
        { duration: "55s", target: 0 },
      ],
      exec: "txWorkload",
      gracefulStop: "30s",
      tags: { scenario: "sustained_1m_per_hour" },
    },

    balance_poll: {
      executor: "constant-vus",
      vus: 5,
      duration: "14m50s",
      startTime: "10s",
      exec: "balancePollWorkload",
      tags: { scenario: "balance_poll" },
    },
  },

  thresholds: {
    // Same accept-side SLOs as load-test-1m.
    http_req_failed: [
      { threshold: "rate<0.05", abortOnFail: true, delayAbortEval: "2m" },
    ],
    http_req_duration: ["p(95)<500", "p(99)<1500"],
    error_rate: ["rate<0.05"],

    "http_req_duration{name:GET /api/v2/accounts/:id/balance}": ["p(50)<3", "p(95)<10"],
    "http_req_duration{name:POST /api/v2/transactions}": ["p(50)<10", "p(95)<50", "p(99)<150"],
    "http_req_duration{name:GET /api/v2/transactions}": ["p(95)<50"],

    // End-to-end SLOs. Bound by consumer batch fill + flush
    // cadence (~200 txn batches at 278 rps fill every ~0.7 s).
    // A fresh transaction averages ~0.35 s queue wait + ~0.1 s
    // DB write + ~0.05 s accept ≈ 0.5–1 s typical, up to ~1.5 s
    // worst. p99<5000 ms allows headroom for batch-timeout
    // flushes during load lulls.
    "transaction_e2e_ms": [
      "p(50)<1500",
      "p(95)<3000",
      "p(99)<5000",
    ],
    // Stuck transactions: anything beyond E2E_POLL_TIMEOUT_MS
    // (10 s) is genuinely wedged. 5% × 217k iters = ~10,850
    // samples; 0.5% timeout = ~55. If this trips, the consumer
    // or outbox is wedged, not just slow.
    "transactions_e2e_timeout": ["count<55"],
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
  console.log(`📊 Running: sustained 1M/hour + ${(E2E_SAMPLE_RATE * 100).toFixed(1)}% e2e sample + balance poll`);
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
  const setupRefId = `setup-sampled-${__ENV.HOSTNAME || "host"}-${Date.now()}`;
  const testPayload = JSON.stringify({
    from_account: "ACC_0000001",
    to_account: "ACC_0000002",
    amount: "1.00",
    currency: "IDR",
    reference_id: setupRefId,
    description: "k6 sampled-e2e setup diagnostic test",
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
        description: `k6 sampled-e2e load test`,
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
      "create status 2xx": () => isAccepted,
      "create has reference_id": () => {
        if (!isAccepted) return false;
        try {
          const body = JSON.parse(res.body);
          return !!(body.data && body.data.reference_id);
        } catch (e) {
          return false;
        }
      },
      "not rate limited": () => res.status !== 429,
      "not overloaded": () => res.status !== 503,
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
      } else {
        // Timeout — don't pollute the Trend with a fixed-ceiling
        // value; count separately so the threshold catches drift.
        transactionsE2eTimeout.add(1);
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
      "list status 200": () => ok,
      "list returns array": () => {
        if (!ok) return false;
        try {
          const body = JSON.parse(res.body);
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
    "balance status 200": () => res.status === 200,
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
  console.log(`\n📈 Load test complete!`);
  console.log(`   Check Grafana dashboard: http://localhost:3001`);
  console.log(`   E2E sample Trend: 'transaction_e2e_ms' in k6 summary`);
  console.log(`   Sample size: ~${Math.round(217000 * E2E_SAMPLE_RATE)} of ~217k iters`);
}
