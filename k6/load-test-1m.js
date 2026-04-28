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
    console.log(`📊 Goal: 1 Million Transactions Per Hour (~278 TPS)`);
    console.log(`⏱️  Duration: 15 minutes (can scale up to 1h)`);
    console.log(`👥 Using ${NUM_ACCOUNTS} pre-seeded accounts (ACC_0000001 – ACC_1000000)\n`);

    // 1. Health check
    const health = http.get(`${BASE_URL}/health`);
    if (health.status !== 200) {
        console.error(`❌ Health check failed! Status: ${health.status}`);
        console.error(`   Make sure docker-compose is running.`);
        return { ok: false };
    }
    console.log(`✅ Health check passed`);

    // 2. Verify accounts exist
    const balanceRes = http.get(
        `${BASE_URL}/api/v2/accounts/ACC_0000001/balance`
    );
    if (balanceRes.status !== 200) {
        console.error(
            `❌ Account ACC_0000001 not found (status ${balanceRes.status}).\n` +
                `   Run the seed script first on each shard:\n` +
                `     PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -p 5000 -U $POSTGRES_USER -d $POSTGRES_DB < k6/seed-accounts.sql\n` +
                `     PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -p 5001 -U $POSTGRES_USER -d $POSTGRES_DB < k6/seed-accounts.sql`
        );
        return { ok: false };
    }
    console.log(`✅ Accounts verified (ACC_0000001 exists)`);

    // 3. Smoke-test a single transaction
    console.log(`\n🔍 Testing single transaction create...`);
    const testPayload = JSON.stringify({
        from_account: "ACC_0000001",
        to_account: "ACC_0000002",
        amount: 100.0,
        currency: "IDR",
        description: "k6 1M/hr setup diagnostic test",
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
            `❌ Transaction create returned ${createRes.status} — the test will likely fail.`
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

    // In a constant-arrival-rate scenario, each iteration is exactly 1 user journey.
    // We'll focus strictly on the core business flow: Creating a transaction.

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
            description: `k6 1M/hr load test ${Date.now()}`,
        });

        const params = {
            headers: { "Content-Type": "application/json" },
        };

        const res = http.post(
            `${BASE_URL}/api/v2/transactions`,
            payload,
            params
        );

        const isAccepted = res.status >= 200 && res.status < 300;

        const success = check(res, {
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

        // Only increment created if we actually succeeded
        if (isAccepted) {
            transactionCreated.add(1);
        }

        transactionLatency.add(res.timings.duration);

        if (!isAccepted) {
            logFailure("Create Transaction", res);
        }
    });

    // Optional: Read a transaction (simulating 10% of users checking their list)
    if (Math.random() < 0.1) {
        group("List Transactions", () => {
            const res = http.get(
                `${BASE_URL}/api/v2/transactions?limit=10&offset=0`
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
    console.log(`   Dashboard: Peakload Capstone — Performance Dashboard`);
}
