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
        { duration: "1m", target: 100 },   // ramp up to 100
        { duration: "3m", target: 500 },   // ramp up to 500
        { duration: "2m", target: 500 },   // sustained
        { duration: "1m", target: 0 },     // ramp down
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
        { duration: "10s", target: 2000 },  // instant spike
        { duration: "30s", target: 2000 },  // hold spike
        { duration: "10s", target: 50 },    // drop back
      ],
      startTime: "11m",
      tags: { scenario: "spike" },
    },
  },

  thresholds: {
    // Overall thresholds
    http_req_duration: ["p(95)<500", "p(99)<1000"],      // 95% under 500ms
    http_req_failed: ["rate<0.05"],                       // Error rate < 5%
    error_rate: ["rate<0.1"],                              // Custom error rate < 10%

    // Per-scenario thresholds
    "http_req_duration{scenario:smoke}": ["p(95)<200"],
    "http_req_duration{scenario:load}": ["p(95)<500"],
  },
};

// ─── Main Test Function ────────────────────────────────────
export default function () {
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
    const payload = JSON.stringify({
      from_account: `ACC${Math.floor(Math.random() * 10000)
        .toString()
        .padStart(5, "0")}`,
      to_account: `ACC${Math.floor(Math.random() * 10000)
        .toString()
        .padStart(5, "0")}`,
      amount: parseFloat((Math.random() * 10000 + 1).toFixed(2)),
      currency: "IDR",
      description: `k6 load test transaction ${Date.now()}`,
    });

    const params = {
      headers: { "Content-Type": "application/json" },
    };

    const res = http.post(
      `${BASE_URL}/api/v1/transactions`,
      payload,
      params
    );

    const success = check(res, {
      "create status 202": (r) => r.status === 202,
      "create has reference_id": (r) => {
        if (r.status === 202) {
          try {
            const body = JSON.parse(r.body);
            return !!(body.data && body.data.reference_id);
          } catch (e) {
            return false;
          }
        }
        return false;
      },
      "not rate limited": (r) => r.status !== 429,
      "not overloaded": (r) => r.status !== 503,
    });

    errorRate.add(!success);
    transactionCreated.add(1);
    transactionLatency.add(res.timings.duration);
  });

  group("List Transactions", () => {
    const res = http.get(
      `${BASE_URL}/api/v1/transactions?limit=10&offset=0`
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

// ─── Setup: Verify connectivity ────────────────────────────
export function setup() {
  console.log(`🎯 Target: ${BASE_URL}`);
  console.log(`📊 Running: smoke → load → stress → spike`);

  const health = http.get(`${BASE_URL}/health`);
  if (health.status !== 200) {
    console.error(`❌ Health check failed! Status: ${health.status}`);
    console.error(`   Make sure docker-compose is running.`);
  } else {
    console.log(`✅ Health check passed`);
  }
}

// ─── Teardown: Print summary ───────────────────────────────
export function teardown(data) {
  console.log(`\n📈 Load test complete!`);
  console.log(`   Check Grafana dashboard: http://localhost:3001`);
  console.log(`   Dashboard: Peakload Capstone — Performance Dashboard`);
}
