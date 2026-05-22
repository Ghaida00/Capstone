// sync-commit-bench.js
//
// Purpose-built load harness for the synchronous_commit A/B
// experiment. It differs from load-test-1m.js on purpose:
//   * One scenario only — a constant-arrival-rate POST stream.
//     No List GET, no balance poll, no health probe: the
//     measurement target is the write path, so reads would
//     only add noise.
//   * rate + duration come from env vars (RATE, DURATION) so
//     the same script drives every config in the matrix.
//   * No idempotency replays — every request is a fresh
//     transfer, so every message the consumer processes is
//     real debit/credit work, not a cheap ON CONFLICT skip.
//
// The throughput figure is NOT taken from this script. k6 only
// sees the HTTP create side, which returns 202 before the
// consumer runs. The real number is the consumer's
// `transactions_processed_total`, read from Prometheus after
// the run.

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const RATE = parseInt(__ENV.RATE || "350", 10);
const DURATION = __ENV.DURATION || "5m";

// Matches the accounts seeded by db/init.sql (ACC_0000001 – ACC_0100000).
const NUM_ACCOUNTS = 100000;

const accepted = new Counter("bench_accepted");
const errorRate = new Rate("bench_errors");

function randomAccount() {
  const i = Math.floor(Math.random() * NUM_ACCOUNTS) + 1;
  return `ACC_${String(i).padStart(7, "0")}`;
}

export const options = {
  scenarios: {
    sync_commit_bench: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      // Headroom: at a few hundred req/s with a ~10-50 ms 202
      // the steady VU count is well under 100; maxVUs covers a
      // slow create path without capping the arrival rate.
      preAllocatedVUs: 200,
      maxVUs: 1000,
      exec: "txWorkload",
    },
  },
  // Informational only — pass/fail is judged from the
  // consumer-side Prometheus metric, not from k6.
  thresholds: {
    http_req_failed: ["rate<0.10"],
  },
};

// Poll the status endpoint until the consumer marks the row
// terminal. setup() uses it to prove the pipeline is alive
// before the load starts.
function waitForCompletion(refId, timeoutMs, pollMs) {
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
        /* retry */
      }
    }
    sleep(pollMs / 1000);
  }
  return null;
}

// Throws on any failure so k6 aborts the run immediately —
// a wedged stack must not burn the full DURATION doing nothing.
export function setup() {
  console.log(`sync-commit-bench → ${BASE_URL} | rate=${RATE}/s duration=${DURATION}`);

  const health = http.get(`${BASE_URL}/health`);
  if (health.status !== 200) {
    throw new Error(`health check failed: status ${health.status}`);
  }

  const balance = http.get(`${BASE_URL}/api/v2/accounts/ACC_0000001/balance`);
  if (balance.status !== 200) {
    throw new Error(`account ACC_0000001 missing: status ${balance.status}`);
  }

  const refId = `bench-setup-${Date.now()}`;
  const createRes = http.post(
    `${BASE_URL}/api/v2/transactions`,
    JSON.stringify({
      from_account: "ACC_0000001",
      to_account: "ACC_0000002",
      amount: "1.00",
      currency: "IDR",
      reference_id: refId,
      description: "sync-commit bench setup",
    }),
    { headers: { "Content-Type": "application/json" } }
  );
  if (createRes.status < 200 || createRes.status >= 300) {
    throw new Error(`setup create failed: status ${createRes.status}`);
  }

  const status = waitForCompletion(refId, 8000, 200);
  if (status === null) {
    throw new Error("consumer did not process setup tx within 8s");
  }
  console.log(`pipeline alive — setup tx → ${status}`);
  return { ok: true };
}

export function txWorkload(data) {
  if (!data || !data.ok) {
    return;
  }

  let fromAccount = randomAccount();
  let toAccount = randomAccount();
  while (toAccount === fromAccount) {
    toAccount = randomAccount();
  }

  const payload = JSON.stringify({
    from_account: fromAccount,
    to_account: toAccount,
    amount: (Math.random() * 1000 + 1).toFixed(2),
    currency: "IDR",
    reference_id: `bench-${__VU}-${__ITER}-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2, 8)}`,
    description: "sync-commit bench",
  });

  const res = http.post(`${BASE_URL}/api/v2/transactions`, payload, {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /api/v2/transactions" },
  });

  const ok = res.status >= 200 && res.status < 300;
  check(res, { "create 2xx": () => ok });
  errorRate.add(!ok);
  if (ok) {
    accepted.add(1);
  }
}
