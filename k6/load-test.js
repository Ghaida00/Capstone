import http from "k6/http";
import { check, sleep, group } from "k6";
import { Rate, Counter, Trend } from "k6/metrics";

// ─── Custom Metrics ────────────────────────────────────────
const transactionCreated = new Counter("transactions_created");
const transactionRead = new Counter("transactions_read");
const errorRate = new Rate("error_rate");
const transactionLatency = new Trend("transaction_latency", true);

// ─── Configuration ─────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";

// Must match the accounts seeded by db/init.sql (ACC_0000001 – ACC_1000000)
const NUM_ACCOUNTS = 1000000;

// Generate account number on-the-fly to avoid allocating a 1M array per VU
function randomAccount() {
  const i = Math.floor(Math.random() * NUM_ACCOUNTS) + 1;
  return `ACC_${String(i).padStart(7, "0")}`;
}

// ─── Test Scenarios ────────────────────────────────────────
// Scenario 1: Smoke Test        — light load to verify everything works
// Scenario 2: Load Test         — sustained load at target TPS
// Scenario 3: Stress Test       — push beyond limits to test resilience
// Scenario 4: Spike Test        — sudden traffic spike

export const options = {
  scenarios: {
    // ── Scenario 1: Smoke (10 VUs for 30s) ──────────────────
    smoke: {
      executor: "constant-vus",
      vus: 10,
      duration: "30s",
      startTime: "0s",
      tags: { scenario: "smoke" },
    },

    // ── Scenario 2: Load (ramp to 500 VUs over 5 min) ───────
    load: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "1m", target: 100 }, // ramp up to 100
        { duration: "3m", target: 500 }, // ramp up to 500
        { duration: "2m", target: 500 }, // sustained
        { duration: "1m", target: 0 }, // ramp down
      ],
      startTime: "35s",
      tags: { scenario: "load" },
    },

    // ── Scenario 3: Stress (burst to 1000 VUs) ──────────────
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
      tags: { scenario: "stress" },
    },

    // ── Scenario 4: Spike (sudden burst) ────────────────────
    spike: {
      executor: "ramping-vus",
      startVUs: 50,
      stages: [
        { duration: "10s", target: 2000 }, // instant spike
        { duration: "30s", target: 2000 }, // hold spike
        { duration: "10s", target: 50 }, // drop back
      ],
      startTime: "11m",
      tags: { scenario: "spike" },
    },
  },

  thresholds: {
    // ── Overall thresholds (covers ALL scenarios) ────────────
    // Intentionally lenient since stress & spike push limits.
    http_req_duration: ["p(95)<2000", "p(99)<5000"],
    http_req_failed: ["rate<0.30"],
    error_rate: ["rate<0.30"],

    // ── Per-scenario thresholds (strict where it matters) ───
    // Smoke: must be fast, near-zero errors
    "http_req_duration{scenario:smoke}": ["p(95)<200"],
    "http_req_failed{scenario:smoke}": ["rate<0.01"],

    // Load: production-realistic expectations
    "http_req_duration{scenario:load}": ["p(95)<500", "p(99)<1000"],
    "http_req_failed{scenario:load}": ["rate<0.05"],

    // Stress: expect some degradation but system shouldn't collapse
    "http_req_duration{scenario:stress}": ["p(95)<3000"],
    "http_req_failed{scenario:stress}": ["rate<0.20"],

    // Spike: even more lenient — we're testing recovery
    "http_req_duration{scenario:spike}": ["p(95)<5000"],
    "http_req_failed{scenario:spike}": ["rate<0.40"],
  },
};

// ─── Helpers ───────────────────────────────────────────────

const MAX_LOGGED_ERRORS = 5;
let loggedErrors = 0;

function logFailure(context, res) {
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

// ─── Setup: Verify connectivity & account seeding ──────────
export function setup() {
  console.log(`🎯 Target: ${BASE_URL}`);
  console.log(`📊 Running: smoke → load → stress → spike`);
  console.log(`👥 Using ${NUM_ACCOUNTS} pre-seeded accounts (ACC_0000001 – ACC_1000000)\n`);

  // 1. Health check
  const health = http.get(`${BASE_URL}/health`);
  if (health.status !== 200) {
    console.error(`❌ Health check failed! Status: ${health.status}`);
    console.error(`   Make sure docker-compose is running.`);
    return { ok: false };
  }
  console.log(`✅ Health check passed`);

  // 2. Verify accounts exist by checking the first one's balance
  const balanceRes = http.get(
    `${BASE_URL}/api/v2/accounts/ACC_0000001/balance`
  );
  if (balanceRes.status !== 200) {
    console.error(
      `❌ Account ACC_0000001 not found (status ${balanceRes.status}).\n` +
        `   Run the seed script first:\n` +
        `     docker exec -i <pg-container> psql -U $POSTGRES_USER -d $POSTGRES_DB < k6/seed-accounts.sql\n` +
        `   Or for each shard via pg-haproxy:\n` +
        `     PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -p 5000 -U $POSTGRES_USER -d $POSTGRES_DB < k6/seed-accounts.sql\n` +
        `     PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -p 5001 -U $POSTGRES_USER -d $POSTGRES_DB < k6/seed-accounts.sql`
    );
    return { ok: false };
  }
  console.log(`✅ Accounts verified (ACC_0000001 exists)`);

  // 3. Smoke-test a single transaction with known-good accounts
  console.log(`\n🔍 Testing single transaction create...`);
  const testPayload = JSON.stringify({
    from_account: "ACC_0000001",
    to_account: "ACC_0000002",
    amount: 100.0,
    currency: "IDR",
    description: "k6 setup diagnostic test",
  });

  const createRes = http.post(
    `${BASE_URL}/api/v2/transactions`,
    testPayload,
    { headers: { "Content-Type": "application/json" } }
  );

  console.log(`   Status: ${createRes.status}`);
  console.log(`   Body:   ${String(createRes.body).substring(0, 300)}`);

  if (createRes.status >= 200 && createRes.status < 300) {
    console.log(`✅ Transaction create works (status ${createRes.status})\n`);
  } else {
    console.error(
      `❌ Transaction create returned ${createRes.status} — ` +
        `the test will likely fail. Check the error above.`
    );
    return { ok: false };
  }

  return { ok: true };
}

// ─── Main Test Function ────────────────────────────────────
export default function (data) {
  if (!data.ok) {
    console.error("Setup failed — skipping iteration");
    sleep(1);
    return;
  }

  group("Health Check", () => {
    const res = http.get(`${BASE_URL}/health`);
    check(res, {
      "health status 200": (r) => r.status === 200,
      "health response valid": (r) => {
        try {
          const body = JSON.parse(r.body);
          return body.status === "healthy" || body.status === "degraded";
        } catch (e) {
          return false;
        }
      },
    });
  });

  group("Create Transaction", () => {
    // Pick two different random accounts from the seeded pool
    let fromAccount = randomAccount();
    let toAccount = randomAccount();
    while (toAccount === fromAccount) {
      toAccount = randomAccount();
    }

    const payload = JSON.stringify({
      from_account: fromAccount,
      to_account: toAccount,
      amount: parseFloat((Math.random() * 10000 + 1).toFixed(2)),
      currency: "IDR",
      description: `k6 load test transaction ${Date.now()}`,
    });

    const params = {
      headers: { "Content-Type": "application/json" },
      tags: { name: "POST /api/v2/transactions" },
    };

    const res = http.post(
      `${BASE_URL}/api/v2/transactions`,
      payload,
      params
    );

    const isAccepted = res.status >= 200 && res.status < 300;

    check(res, {
      "create status 2xx": (r) => isAccepted,
      "create has reference_id": (r) => {
        if (!isAccepted) return false;
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

    errorRate.add(!isAccepted);
    transactionCreated.add(1);
    transactionLatency.add(res.timings.duration);

    if (!isAccepted) {
      logFailure("Create Transaction", res);
    }
  });

  group("List Transactions", () => {
    const params = {
      tags: { name: "GET /api/v2/transactions" },
    };

    const res = http.get(
      `${BASE_URL}/api/v2/transactions?limit=10&offset=0`,
      params
    );

    check(res, {
      "list status 200": (r) => r.status === 200,
      "list returns array": (r) => {
        if (r.status === 200) {
          try {
            const body = JSON.parse(r.body);
            return body.success && Array.isArray(body.data);
          } catch (e) {
            return false;
          }
        }
        return false;
      },
    });

    transactionRead.add(1);
  });

  // Small pause between iterations to simulate realistic traffic
  sleep(Math.random() * 0.5);
}

// ─── Teardown: Print summary ───────────────────────────────
export function teardown(data) {
  console.log(`\n📈 Load test complete!`);
  console.log(`   Check Grafana dashboard: http://localhost:3001`);
  console.log(`   Dashboard: Peakload Capstone — Performance Dashboard`);
}
