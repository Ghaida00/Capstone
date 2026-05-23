import http from "k6/http";
import { check, sleep, group } from "k6";
import { Rate, Counter } from "k6/metrics";

// ─── Custom Metrics ────────────────────────────────────────
// Per-endpoint latency lives on the built-in `http_req_duration`
// metric, sliced by `tags.name`. A separate `transaction_latency`
// Trend mixed POST and GET into one distribution, making its p95
// meaningless — dropped in favour of the per-endpoint thresholds.
const transactionCreated = new Counter("transactions_created");
const transactionRead = new Counter("transactions_read");
const errorRate = new Rate("error_rate");
const idempotencyReplayAttempts = new Counter("idempotency_replay_attempts");

// ─── Configuration ─────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";

// Must match the accounts seeded by db/init.sql (ACC_0000001 – ACC_0100000)
const NUM_ACCOUNTS = 100000;

// Pool fits inside the `v1:balance:` 10 s Redis TTL
// (BALANCE_CACHE_TTL_SECS in crates/accounts/src/application/mod.rs)
// so steady-state polls hit warm.
const BALANCE_POLL_POOL_SIZE = 100;

// Random account from the 100k pool. Generated on-the-fly so we never
// allocate a 100k array per VU.
function randomAccount() {
  const i = Math.floor(Math.random() * NUM_ACCOUNTS) + 1;
  return `ACC_${String(i).padStart(7, "0")}`;
}

// Random account from the balance-poll pool.
function balancePollAccount() {
  const i = Math.floor(Math.random() * BALANCE_POLL_POOL_SIZE) + 1;
  return `ACC_${String(i).padStart(7, "0")}`;
}

// Per-VU buffer of recently-issued requests. Storing the FULL
// payload (not just the reference_id) is what makes the replay
// fire the server's Replay branch: idempotency_key embeds
// `shard_for_account(from_account)` and the cached entry is keyed
// on `(idempotency_key, request_hash)`. Replaying with a fresh
// from_account or amount produces a different shard / hash and
// would land as either a fresh reservation or a HashConflict 400.
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
  // Default summary stats include only avg/min/med/max/p(90)/p(95).
  // Add p(99) + p(99.9) explicitly so the per-run stdout shows the
  // tail percentiles the thresholds gate on (otherwise you must
  // reach for Grafana even to see whether the p(99)<150 threshold
  // was tight or loose).
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)", "p(99.9)", "count"],

  // Global tags get attached to every sample so Grafana/k6 Cloud
  // can filter runs by environment + commit. Run-local default to
  // ms-since-epoch so back-to-back local runs do not overlap in
  // the same series.
  tags: {
    environment: __ENV.K6_ENV || "local",
    run_id: __ENV.RUN_ID || String(Date.now()),
    git_sha: __ENV.GIT_SHA || "dev",
  },

  // Don't reuse TCP connections across iterations — this is the
  // k6 default but stated explicitly here for reproducibility
  // across k6 versions and against external readers.
  noConnectionReuse: false,
  insecureSkipTLSVerify: false,

  scenarios: {
    sustained_1m_per_hour: {
      // Ramping arrival rate: a 1-minute warmup avoids the cold-
      // pool / cold-cache thundering herd that briefly spikes p99
      // at t=0 with constant-arrival-rate. The 1-minute cooldown
      // exposes whether p99 recovers cleanly after load stops —
      // a leak / queue-buildup bug shows up here as p99 staying
      // high after the load drops to zero.
      //
      // Note: the progress bar may show a non-zero rate (~3 iter/s)
      // at t=14:59.x because target=0 is reached at the END of the
      // last stage, not before. That is a k6 progress-display
      // quirk for ramping-arrival-rate, not a real "scenario did
      // not complete" condition — `iterations` counts all
      // dispatched, all complete, no orphans.
      executor: "ramping-arrival-rate",
      startRate: 50,
      timeUnit: "1s",
      preAllocatedVUs: 300,
      maxVUs: 1500,
      stages: [
        { duration: "1m",  target: 278 },   // warmup: 50 → 278 rps
        { duration: "13m", target: 278 },   // sustained at 1M/hour
        { duration: "55s",  target: 0 },     // cooldown: 278 → 0
      ],
      exec: "txWorkload",
      gracefulStop: "30s",
      tags: { scenario: "sustained_1m_per_hour" },
    },

    // 5 VUs polling GET /balance. Starts after a 10s warmup so
    // cold pool / replica-health-probe blips at t=0 don't poison
    // the check-rate threshold. Runs 14m50s to stay inside the
    // 15m envelope.
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
    // `abortOnFail` kills the run if the error rate breaches the
    // SLO — no point burning 15 minutes against a wedged server
    // once the gate has tripped. `delayAbortEval: "2m"` rides
    // through the warmup window (cold pools, cache misses) where
    // a 1-minute moving rate can spike harmlessly.
    http_req_failed: [
      { threshold: "rate<0.05", abortOnFail: true, delayAbortEval: "2m" },
    ],
    http_req_duration: ["p(95)<500", "p(99)<1500"],
    error_rate: ["rate<0.05"],

    // Per-endpoint targets in ms. Floors set by loopback HTTP
    // round-trip (~1ms) + handler work, not by server CPU. Sub-ms
    // p(50) is unachievable here regardless of optimization.
    "http_req_duration{name:GET /api/v2/accounts/:id/balance}": ["p(50)<3", "p(95)<10"],
    "http_req_duration{name:POST /api/v2/transactions}": ["p(50)<10", "p(95)<50", "p(99)<150"],
    "http_req_duration{name:GET /api/v2/transactions}": ["p(95)<50"],
  },
};

// ─── Helpers ───────────────────────────────────────────────

// Per-VU error log budget. Logging is silently dropped above
// 200 VU (spike / late-stress) to avoid drowning the k6 stdout
// in 10k+ lines per run.
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

// Poll /api/v2/transactions/status/{ref} until the consumer marks
// the row completed/failed. Used by setup to verify end-to-end works
// before the load scenarios start.
//
// Server-side fix means the endpoint returns 200 + `status: "pending"`
// during the brief accept→flush window (instead of the previous 404).
// No `responseCallback` workaround needed — every setup poll is now
// a true 200, so k6's default failure classification is correct.
const SETUP_POLL_PARAMS = {
  tags: { name: "GET /api/v2/transactions/status/:ref (setup poll)" },
};

function waitForCompletion(refId, timeoutMs = 5000, pollMs = 200) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = http.get(
      `${BASE_URL}/api/v2/transactions/status/${refId}`,
      SETUP_POLL_PARAMS
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

// ─── Setup: Verify connectivity & end-to-end pipeline ──────
// Any unrecoverable problem here throws — k6 aborts before
// scenarios fire, so we don't burn 15 minutes of iterations
// against a wedged pipeline.
export function setup() {
  console.log(`🎯 Target: ${BASE_URL}`);
  console.log(`📊 Running: sustained 1M/hour + balance poll`);
  console.log(`👥 Using ${NUM_ACCOUNTS} pre-seeded accounts (ACC_0000001 – ACC_0100000)\n`);

  // 1. Health check
  const health = http.get(`${BASE_URL}/health`);
  if (health.status !== 200) {
    throw new Error(
      `Health check failed (status ${health.status}). ` +
        `Make sure docker-compose is running.`
    );
  }
  console.log(`✅ Health check passed`);

  // 2. Verify accounts exist (no state mutation)
  const balanceRes = http.get(`${BASE_URL}/api/v2/accounts/ACC_0000001/balance`);
  if (balanceRes.status !== 200) {
    throw new Error(
      `Account ACC_0000001 not found (status ${balanceRes.status}). ` +
        `Seed accounts first via db/init.sql or k6/seed-accounts.sql.`
    );
  }
  console.log(`✅ Accounts verified (ACC_0000001 exists)`);

  // 3. End-to-end smoke test: create + poll status until consumer
  // marks the row processed. Confirms the entire path (HTTP → queue
  // → consumer → DB) is alive before the load scenarios fire.
  console.log(`\n🔍 End-to-end smoke test (create + wait for consumer)...`);
  const setupRefId = `setup-${__ENV.HOSTNAME || "host"}-${Date.now()}`;
  const testPayload = JSON.stringify({
    from_account: "ACC_0000001",
    to_account: "ACC_0000002",
    amount: "1.00",
    currency: "IDR",
    reference_id: setupRefId,
    description: "k6 setup diagnostic test",
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

  // 30 s is sized to absorb residual drain from a previous run —
  // the cross-shard processor + idempotency publisher can keep
  // backends busy for ~1–3 minutes after a 15-minute load finishes.
  // A back-to-back run started during that window queues this
  // setup tx behind that backlog; 30 s gives the consumer one full
  // tick of headroom over the usual cold-start ~5 s. Past 30 s the
  // pipeline is genuinely wedged and aborting is the right move.
  const finalStatus = waitForCompletion(setupRefId, 30000, 250);
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
  group("Create Transaction", () => {
    // ~5% of iterations replay a full earlier request — simulates
    // a real client retrying after a network glitch (same payload,
    // same reference_id). Server takes the Replay branch.
    // The other 95% are fresh random traffic.
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
      // toFixed(2) avoids JS float JSON artefacts; server validates scale<=2.
      const amount = (Math.random() * 1000 + 1).toFixed(2);
      const referenceId = `${__VU}-${__ITER}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      payloadObj = {
        from_account: fromAccount,
        to_account: toAccount,
        amount,
        currency: "IDR",
        reference_id: referenceId,
        description: `k6 load test`,
      };
    }

    const payload = JSON.stringify(payloadObj);
    const params = {
      headers: { "Content-Type": "application/json" },
      tags: { name: "POST /api/v2/transactions" },
    };

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
      // Counter mirrors the server's `transactions_created_total`,
      // which is incremented once per accepted POST inside the HTTP
      // handler — including the replay branch. So the two rates
      // should match 1:1 (no queue→consumer lag). If you want a
      // counter that mirrors `transactions_processed_total`
      // instead, exclude replays here.
      transactionCreated.add(1);
      if (!isReplay) {
        rememberRequest(payloadObj);
      }
    } else {
      logFailure("Create Transaction", res);
    }
  });

  group("List Transactions", () => {
    // Vary the cursor in half the calls so we don't all hit the
    // same `txn_list:10:latest` cache key. Realistic clients page
    // back through history with a `before` cursor.
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

  // No sleep: open-model executor paces requests itself; iteration-end sleep would only inflate the VU pool. (balancePollWorkload's sleep is correct — that scenario is closed-model.)
}

// ─── Balance-poll workload (separate scenario, low VU) ─────
// Polls GET /api/v2/accounts/:id/balance over BALANCE_POLL_POOL_SIZE
// accounts. Tagged `name:GET /api/v2/accounts/:id/balance` for the
// per-endpoint threshold.
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

// ─── Teardown: Print summary ───────────────────────────────
export function teardown() {
  console.log(`\n📈 Load test complete!`);
  console.log(`   Check Grafana dashboard: http://localhost:3001`);
  console.log(`   Dashboard: Peakload Capstone — Performance Dashboard`);
}
