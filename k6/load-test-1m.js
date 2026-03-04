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

// ─── 1 Million Transactions Per Hour Target ────────────────
// 1,000,000 transactions / 60 minutes / 60 seconds = ~277.77 transactions per second (TPS).
// We will use the "constant-arrival-rate" executor to force k6 to start exactly 278 iterations per second.

export const options = {
    scenarios: {
        sustained_1m_per_hour: {
            executor: "constant-arrival-rate",
            // Target rate: 278 iterations per second.
            rate: 278,
            timeUnit: "1s",

            // We'll run this for 15 minutes by default to prove stability. 
            // You can change this to "1h" if you want to run the full hour test.
            duration: "15m",

            // Pre-allocate VUs to handle the steady load
            preAllocatedVUs: 300,

            // If the system slows down, k6 is allowed to spin up to 1500 VUs to maintain the 278 TPS rate.
            maxVUs: 1500,

            tags: { scenario: "sustained_1m_per_hour" },
        },
    },

    thresholds: {
        // Strict thresholds for a sustained production-like load.
        // We expect the system to hum along perfectly without rate limiting or circuit breaking.
        http_req_duration: ["p(95)<200", "p(99)<500"], // 95% under 200ms, 99% under 500ms
        http_req_failed: ["rate<0.01"],                // Less than 1% total errors
        error_rate: ["rate<0.01"],                     // Custom error tracking < 1%
    },
};

// ─── Setup: Verify connectivity ────────────────────────────
export function setup() {
    console.log(`🎯 Target: ${BASE_URL}`);
    console.log(`📊 Goal: 1 Million Transactions Per Hour (~278 TPS)`);
    console.log(`⏱️  Duration: 15 minutes (can scale up to 1h)`);

    const health = http.get(`${BASE_URL}/health`);
    if (health.status !== 200) {
        console.error(`❌ Health check failed! Status: ${health.status}`);
        console.error(`   Make sure docker-compose is running.`);
    } else {
        console.log(`✅ Health check passed`);
    }
}

// ─── Main Test Function ────────────────────────────────────
export default function () {
    // In a constant-arrival-rate scenario, each iteration is exactly 1 user journey.
    // We'll focus strictly on the core business flow: Creating a transaction.

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
            description: `k6 1M/hr load test ${Date.now()}`,
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

        // Only increment created if we actually succeeded
        if (success) {
            transactionCreated.add(1);
        }

        transactionLatency.add(res.timings.duration);
    });

    // Optional: Read a transaction (simulating 10% of users checking their list)
    if (Math.random() < 0.1) {
        group("List Transactions", () => {
            const res = http.get(
                `${BASE_URL}/api/v1/transactions?limit=10&offset=0`
            );

            check(res, {
                "list status 200": (r) => r.status === 200,
            });

            transactionRead.add(1);
        });
    }
}

// ─── Teardown: Print summary ───────────────────────────────
export function teardown(data) {
    console.log(`\n📈 1 Million/hr Load test complete!`);
    console.log(`   Check Grafana dashboard: http://localhost:3001`);
    console.log(`   Dashboard: GN Backend — Performance Dashboard`);
}
