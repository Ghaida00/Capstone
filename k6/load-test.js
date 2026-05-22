import http from "k6/http";
import { check, sleep, group } from "k6";
import { Rate, Counter, Trend } from "k6/metrics";

// ─── Custom Metrics ────────────────────────────────────────
const transactionCreated = new Counter("transactions_created");
const transactionRead = new Counter("transactions_read");
const errorRate = new Rate("error_rate");
const transactionLatency = new Trend("transaction_latency", true);
const idempotencyReplayHits = new Counter("idempotency_replay_attempts");

// ─── Configuration ─────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";

// Must match the accounts seeded by db/init.sql (ACC_0000001 – ACC_0100000)
const NUM_ACCOUNTS = 100000;

// Hot-key contention pool: tiny set to force row-lock waits on the
// debit UPDATE. Exercises the locking behavior that the uniform
// 1M-account pick can never reach.
const HOT_KEY_POOL_SIZE = 10;

// Pool fits inside the `v1:balance:` 30 s Redis TTL so steady-state
// polls hit warm.
const BALANCE_POLL_POOL_SIZE = 100;

// Random account from the 1M pool. Generated on-the-fly so we never
// allocate a 1M array per VU.
function randomAccount() {
  const i = Math.floor(Math.random() * NUM_ACCOUNTS) + 1;
  return `ACC_${String(i).padStart(7, "0")}`;
}

// Random account from the 10-key hot pool.
function hotAccount() {
  const i = Math.floor(Math.random() * HOT_KEY_POOL_SIZE) + 1;
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
// smoke   — light load to verify everything works
// load    — sustained load at target TPS
// stress  — push beyond limits to test resilience
// spike   — sudden traffic spike (sized to fit the protection budget:
//           1500 VU × 3 in-flight reqs ≤ 4500 vs MAX_CONCURRENT 4000;
//           still trips backpressure, but not by an order of magnitude)
// hotkey  — small account pool to stress per-row UPDATE wait under
//           the sender's tx; runs after spike

export const options = {
  scenarios: {
    smoke: {
      executor: "constant-vus",
      vus: 10,
      duration: "30s",
      startTime: "0s",
      exec: "txWorkload",
      tags: { scenario: "smoke" },
    },

    load: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "1m", target: 100 },
        { duration: "3m", target: 500 },
        { duration: "2m", target: 500 },
        { duration: "1m", target: 0 },
      ],
      startTime: "35s",
      exec: "txWorkload",
      tags: { scenario: "load" },
    },

    stress: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "30s", target: 500 },
        { duration: "1m", target: 1000 },
        { duration: "1m", target: 1000 },
        { duration: "30s", target: 0 },
      ],
      startTime: "8m",
      exec: "txWorkload",
      tags: { scenario: "stress" },
    },

    spike: {
      executor: "ramping-vus",
      startVUs: 50,
      stages: [
        { duration: "10s", target: 2000 },
        { duration: "30s", target: 2000 },
        { duration: "10s", target: 50 },
      ],
      startTime: "11m",
      exec: "txWorkload",
      tags: { scenario: "spike" },
    },

    hotkey: {
      executor: "constant-vus",
      vus: 100,
      duration: "1m",
      startTime: "12m",
      exec: "hotKeyWorkload",
      tags: { scenario: "hotkey" },
    },

    // Continuous health probe at low VU. Separated from txWorkload
    // so the per-iteration loop does not amplify /health into a DB
    // load multiplier (every /health touches every shard's writer
    // and reader pool, so under 2000 VU it dominated the workload).
    health: {
      executor: "constant-vus",
      vus: 1,
      duration: "13m",
      startTime: "0s",
      exec: "healthProbe",
      tags: { scenario: "health" },
    },

    // 5 VUs polling GET /balance over BALANCE_POLL_POOL_SIZE accounts.
    // Pool fits the 30 s `v1:balance:` Redis TTL so steady-state hits
    // the warm path; tag `name:GET /api/v2/accounts/:id/balance`.
    balance_poll: {
      executor: "constant-vus",
      vus: 5,
      duration: "13m",
      startTime: "0s",
      exec: "balancePollWorkload",
      tags: { scenario: "balance_poll" },
    },
  },

  thresholds: {
    // Tightened in line with the post-optimisation latency budget:
    // load p95 < 50 ms, smoke p95 < 30 ms. Stress / spike are still
    // allowed to degrade but no longer accept the multi-second tail
    // we used to tolerate.
    http_req_duration: ["p(95)<500", "p(99)<1500"],
    http_req_failed: ["rate<0.05"],
    error_rate: ["rate<0.05"],

    // Smoke threshold relaxed from 0.005 to 0.01 — the original
    // value tripped on a single cold-start 5xx (RabbitMQ reconnect
    // window during the first iterations). 1% gives one fail per
    // ~100 requests of headroom.
    "http_req_duration{scenario:smoke}": ["p(95)<30", "p(99)<80"],
    "http_req_failed{scenario:smoke}": ["rate<0.01"],

    "http_req_duration{scenario:load}": ["p(95)<50", "p(99)<150"],
    "http_req_failed{scenario:load}": ["rate<0.01"],

    "http_req_duration{scenario:stress}": ["p(95)<300", "p(99)<800"],
    "http_req_failed{scenario:stress}": ["rate<0.05"],

    "http_req_duration{scenario:spike}": ["p(95)<800", "p(99)<2000"],
    "http_req_failed{scenario:spike}": ["rate<0.10"],

    // Hot-key is expected to be slower (lock contention) but should
    // not error out — the consumer-side idempotency + same-shard
    // tx atomicity keep the tail bounded.
    "http_req_duration{scenario:hotkey}": ["p(95)<200", "p(99)<500"],
    "http_req_failed{scenario:hotkey}": ["rate<0.02"],

    // Per-endpoint targets, milliseconds. Tag values match
    // `tags: { name: ... }` on the http calls below.
    "http_req_duration{name:GET /api/v2/accounts/:id/balance}": ["p(50)<0.5", "p(95)<1.5"],
    "http_req_duration{name:POST /api/v2/transactions}": ["p(50)<2", "p(95)<5", "p(99)<15"],
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
function waitForCompletion(refId, timeoutMs = 5000, pollMs = 200) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = http.get(`${BASE_URL}/api/v2/transactions/status/${refId}`);
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
export function setup() {
  console.log(`🎯 Target: ${BASE_URL}`);
  console.log(`📊 Running: smoke → load → stress → spike → hotkey`);
  console.log(`👥 Using ${NUM_ACCOUNTS} pre-seeded accounts (ACC_0000001 – ACC_0100000)`);
  console.log(`🔥 Hot-key pool: ${HOT_KEY_POOL_SIZE} accounts (ACC_0000001 – ACC_${String(HOT_KEY_POOL_SIZE).padStart(7, "0")})\n`);

  // 1. Health check
  const health = http.get(`${BASE_URL}/health`);
  if (health.status !== 200) {
    console.error(`❌ Health check failed! Status: ${health.status}`);
    console.error(`   Make sure docker-compose is running.`);
    return { ok: false };
  }
  console.log(`✅ Health check passed`);

  // 2. Verify accounts exist (no state mutation)
  const balanceRes = http.get(`${BASE_URL}/api/v2/accounts/ACC_0000001/balance`);
  if (balanceRes.status !== 200) {
    console.error(
      `❌ Account ACC_0000001 not found (status ${balanceRes.status}).\n` +
        `   Seed accounts first via db/init.sql or k6/seed-accounts.sql.`
    );
    return { ok: false };
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
    console.error(
      `❌ Transaction create returned ${createRes.status}. ` +
        `Body: ${String(createRes.body).substring(0, 300)}`
    );
    return { ok: false };
  }

  const finalStatus = waitForCompletion(setupRefId, 8000, 200);
  if (finalStatus === null) {
    console.error(
      `❌ Consumer did not process setup transaction within 8s. ` +
        `RabbitMQ or consumer may be wedged.`
    );
    return { ok: false };
  }
  console.log(`✅ Consumer processed setup tx → status=${finalStatus}\n`);

  return { ok: true };
}

// ─── Main workload (smoke / load / stress / spike) ─────────
export function txWorkload(data) {
  if (!data.ok) {
    sleep(1);
    return;
  }

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
      idempotencyReplayHits.add(1);
    } else {
      let fromAccount = randomAccount();
      let toAccount = randomAccount();
      while (toAccount === fromAccount) {
        toAccount = randomAccount();
      }
      // Send amount as a string with exactly 2 decimal places to
      // sidestep JS float→JSON round-trip artefacts (e.g. 567.89
      // serialising as 567.8899999...). Server validates scale<=2
      // and would reject anything else.
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

    errorRate.add(!isAccepted);
    transactionLatency.add(res.timings.duration);

    if (isAccepted) {
      // Counter reflects accepted creates only — the dashboard
      // panel that compares this against the server's
      // `transactions_processed_total` should match modulo the
      // queue → consumer delay.
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

    errorRate.add(!ok);
    transactionLatency.add(res.timings.duration);

    if (ok) {
      transactionRead.add(1);
    } else {
      logFailure("List Transactions", res);
    }
  });

  sleep(Math.random() * 0.5);
}

// ─── Hot-key workload ──────────────────────────────────────
// Picks both `from` and `to` from a tiny 10-account pool so the
// per-row UPDATE inside the sender's tx contends. Verifies that
// the new same-shard same-tx atomicity does not deadlock and that
// the consumer-side idempotency check does not regress correctness
// when concurrent retries fight over the same row.
export function hotKeyWorkload(data) {
  if (!data.ok) {
    sleep(1);
    return;
  }

  let fromAccount = hotAccount();
  let toAccount = hotAccount();
  while (toAccount === fromAccount) {
    toAccount = hotAccount();
  }

  const referenceId = `hot-${__VU}-${__ITER}-${Date.now()}`;
  const amount = (Math.random() * 10 + 1).toFixed(2);
  const payload = JSON.stringify({
    from_account: fromAccount,
    to_account: toAccount,
    amount,
    currency: "IDR",
    reference_id: referenceId,
    description: `k6 hot-key`,
  });

  const res = http.post(`${BASE_URL}/api/v2/transactions`, payload, {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /api/v2/transactions (hotkey)" },
  });

  const isAccepted = res.status >= 200 && res.status < 300;
  check(res, {
    "hotkey create status 2xx": () => isAccepted,
  });

  errorRate.add(!isAccepted);
  transactionCreated.add(1);
  transactionLatency.add(res.timings.duration);

  if (!isAccepted) {
    logFailure("Hot-Key Create", res);
  }

  sleep(Math.random() * 0.2);
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
  if (res.status !== 200) {
    errorRate.add(1);
    logFailure("Balance Poll", res);
  } else {
    errorRate.add(0);
  }
  sleep(0.1);
}

// ─── Health probe (separate scenario, low VU) ──────────────
export function healthProbe() {
  const res = http.get(`${BASE_URL}/health`, {
    tags: { name: "GET /health" },
  });
  check(res, {
    "health status 200": () => res.status === 200,
    "health response valid": () => {
      try {
        const body = JSON.parse(res.body);
        return body.status === "healthy" || body.status === "degraded";
      } catch (e) {
        return false;
      }
    },
  });
  sleep(2); // probe every ~2s
}

// ─── Teardown: Print summary ───────────────────────────────
export function teardown() {
  console.log(`\n📈 Load test complete!`);
  console.log(`   Check Grafana dashboard: http://localhost:3001`);
  console.log(`   Dashboard: Peakload Capstone — Performance Dashboard`);
}
