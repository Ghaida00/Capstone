import http from "k6/http";
import { check, sleep, group } from "k6";
import { Rate, Counter, Trend } from "k6/metrics";

// ─── Load test: 1M *completed* per hour (end-to-end) ────────
// Companion to load-test-1m.js. Same arrival shape (~278 rps for
// 13 min sustained), same payload mix, same replay rate. The
// difference: every fresh POST also polls /status/{ref} until the
// transaction reaches a terminal state, and the per-request
// wall-clock from "request issued" to "completed/failed" is
// recorded into `transaction_e2e_ms`.
//
// Why a separate file:
//
//   - load-test-1m.js measures **accept latency** only — the time
//     from POST to 202. That's the right number for "does the API
//     feel responsive?" SLOs (p95 < 50 ms), but it hides the
//     batched async write path: queue → consumer batch → DB flush
//     → outbox publish. End-to-end is typically 0.8–1.5 s per
//     transaction.
//   - This file measures the **honest tap-to-Successful** time.
//     It's a stronger claim: not just "1M accepted per hour" but
//     "1M *completed* per hour, with bounded end-to-end latency".
//
// Why this also stresses the system harder than load-test-1m:
//
//   - Every iteration now does POST + ~5–10 GETs on /status (one
//     every 100 ms until terminal). At 278 POST/s that's an extra
//     ~1400–2800 read/s on the status endpoint. The status
//     handler is Redis-cached, so most polls land sub-ms, but the
//     fan-out is more realistic — real payment clients DO poll.
//   - Each VU is busy ~1.5 s per iter (vs ~50 ms in load-test-1m).
//     Little's law: 278 rps × 1.5 s = ~420 concurrent VUs. We
//     pre-allocate 600 and cap maxVUs at 2000 for safety under
//     end-to-end latency spikes.

const transactionCreated = new Counter("transactions_created");
const transactionRead = new Counter("transactions_read");
const errorRate = new Rate("error_rate");
const idempotencyReplayAttempts = new Counter("idempotency_replay_attempts");

// End-to-end latency: from POST issuance to terminal status visible
// on /status/{ref}. Trend so we get p50/p95/p99 reported. Replays
// are EXCLUDED from this Trend — they hit the idempotency cache and
// return near-instantly, which would skew the distribution optimistic.
const transactionE2eMs = new Trend("transaction_e2e_ms", true);
const transactionsE2eCompleted = new Counter("transactions_e2e_completed");
const transactionsE2eTimeout = new Counter("transactions_e2e_timeout");

// ─── Configuration ─────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const NUM_ACCOUNTS = 100000;
const BALANCE_POLL_POOL_SIZE = 100;

// Per-iter e2e budget. 10 s gives plenty of headroom over the
// observed ~1.5 s typical end-to-end while still failing fast on
// genuinely stuck transactions. Timed-out polls increment
// `transactions_e2e_timeout` and are NOT added to the Trend.
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
    sustained_1m_per_hour_e2e: {
      // Same arrival shape as load-test-1m.js, but each iter is
      // ~30× longer wall-clock because of e2e polling. The
      // ramping-arrival-rate executor pins the rate regardless of
      // iter duration — k6 spins up VUs from the pool to meet it.
      executor: "ramping-arrival-rate",
      startRate: 50,
      timeUnit: "1s",
      // Little's law: 278 rps × ~1.5 s/iter ≈ 420 concurrent VUs.
      // Pre-allocate 600 so the warmup doesn't stall on VU spawn;
      // cap maxVUs at 2000 to ride out latency tails without
      // hitting the default 1500 ceiling.
      preAllocatedVUs: 600,
      maxVUs: 2000,
      stages: [
        { duration: "1m",  target: 278 },
        { duration: "13m", target: 278 },
        { duration: "1m",  target: 0 },
      ],
      exec: "txWorkload",
      // 30 s isn't enough when iters can take 10 s — bump to 60 s
      // so the cooldown phase actually drains in-flight e2e polls
      // instead of orphaning them as interrupted iterations.
      gracefulStop: "60s",
      tags: { scenario: "sustained_1m_per_hour_e2e" },
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
    // Same accept-side SLOs as load-test-1m, repeated here so a
    // regression on POST latency is caught by this run too.
    http_req_failed: [
      { threshold: "rate<0.05", abortOnFail: true, delayAbortEval: "2m" },
    ],
    http_req_duration: ["p(95)<500", "p(99)<1500"],
    error_rate: ["rate<0.05"],

    "http_req_duration{name:GET /api/v2/accounts/:id/balance}": ["p(50)<3", "p(95)<10"],
    "http_req_duration{name:POST /api/v2/transactions}": ["p(50)<10", "p(95)<50", "p(99)<150"],
    "http_req_duration{name:GET /api/v2/transactions}": ["p(95)<50"],

    // End-to-end SLOs. The hot path is bounded by batch fill +
    // flush cadence (consumer batch size ~200; at 278 rps batches
    // fill every ~0.7 s). A fresh transaction arrives somewhere
    // in the cycle — average ~0.35 s queue wait + ~0.1 s DB write
    // + accept ~0.05 s ≈ 0.5–1 s typical, up to ~1.5 s worst.
    // p99 < 5 s allows headroom for batch-timeout flushes during
    // load lulls.
    "transaction_e2e_ms": [
      "p(50)<1500",
      "p(95)<3000",
      "p(99)<5000",
    ],
    // Anything beyond 10 s is genuinely stuck. Bound the timeout
    // count to <0.5% of expected accepted volume (~218k over
    // 13 min × 0.005 ≈ 1090). If this trips, the consumer or
    // outbox is wedged, not just slow.
    "transactions_e2e_timeout": ["count<1090"],
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

// Tag for setup-phase status polls. Kept separate from the e2e
// poll tag so the threshold buckets stay clean.
const SETUP_POLL_PARAMS = {
  tags: { name: "GET /api/v2/transactions/status/:ref (setup poll)" },
};

// Tag for in-load e2e status polls. Distinct from setup so each
// shows its own latency distribution in the k6 summary, and so
// http_req_duration threshold buckets don't conflate the two.
const E2E_POLL_PARAMS = {
  tags: { name: "GET /api/v2/transactions/status/:ref (e2e poll)" },
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
  console.log(`📊 Running: sustained 1M/hour END-TO-END + balance poll`);
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
  const setupRefId = `setup-e2e-${__ENV.HOSTNAME || "host"}-${Date.now()}`;
  const testPayload = JSON.stringify({
    from_account: "ACC_0000001",
    to_account: "ACC_0000002",
    amount: "1.00",
    currency: "IDR",
    reference_id: setupRefId,
    description: "k6 e2e setup diagnostic test",
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

// ─── Main workload (end-to-end per iter) ───────────────────
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
        description: `k6 e2e load test`,
      };
    }

    const payload = JSON.stringify(payloadObj);
    const params = {
      headers: { "Content-Type": "application/json" },
      tags: { name: "POST /api/v2/transactions" },
    };

    // Capture t0 BEFORE the POST so the recorded e2e includes the
    // accept latency — the honest "request → done" wall-clock the
    // user's phone would see, not just the post-202 polling delay.
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

  // ── End-to-end completion track ──
  // Only for fresh accepted transactions. Replays return cached
  // accepted-payload immediately and would skew the Trend toward
  // 0 ms; their e2e doesn't represent fresh-write latency. Failed
  // accepts (4xx/5xx) skip naturally because referenceIdForE2e
  // is null.
  if (isFreshAccepted && referenceIdForE2e) {
    group("Wait for Completion", () => {
      const finalStatus = waitForCompletion(
        referenceIdForE2e,
        E2E_POLL_PARAMS,
        E2E_POLL_TIMEOUT_MS,
        E2E_POLL_INTERVAL_MS,
      );
      if (finalStatus === "completed" || finalStatus === "failed") {
        const e2eMs = Date.now() - txStartTime;
        transactionE2eMs.add(e2eMs);
        transactionsE2eCompleted.add(1);
      } else {
        // Timeout — consumer/outbox is stuck for this ref. Don't
        // pollute the Trend with a fixed-ceiling value; count it
        // separately so the threshold catches drift.
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
  console.log(`   Dashboard: Peakload Capstone — Performance Dashboard`);
  console.log(`   End-to-end Trend: 'transaction_e2e_ms' in k6 summary above`);
  console.log(`   Timeouts (consumer drift signal): 'transactions_e2e_timeout'`);
}
