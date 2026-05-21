// k6/setup-probe.js — minimal smoke probe for the
// /api/v2/transactions write path. NOT part of CI; run manually
// with `k6 run k6/setup-probe.js` to confirm an environment can
// accept one POST and resolve its status before kicking off a
// full load run.
//
// Behaviour: one POST + poll GET /status until processed, then
// exit. Logs the elapsed times and the final status.
//
// Mirrors the setup() block in load-test.js so a green probe
// here predicts a green setup() during the real run.
import http from "k6/http";
import { sleep } from "k6";

export const options = {
  vus: 1,
  iterations: 1,
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";

export default function () {
  const refId = `probe-${Date.now()}`;
  const payload = JSON.stringify({
    from_account: "ACC_0000001",
    to_account: "ACC_0000002",
    amount: "1.00",
    currency: "IDR",
    reference_id: refId,
    description: "setup probe",
  });

  const t0 = Date.now();
  const createRes = http.post(`${BASE_URL}/api/v2/transactions`, payload, {
    headers: { "Content-Type": "application/json" },
  });
  const tCreate = Date.now() - t0;
  console.log(`POST  status=${createRes.status} elapsed=${tCreate}ms`);
  if (createRes.status < 200 || createRes.status >= 300) {
    console.error(`POST body: ${String(createRes.body).substring(0, 300)}`);
    return;
  }

  // Poll GET /status every 200ms for up to 8s, like load-test.js.
  const tPollStart = Date.now();
  const deadline = tPollStart + 8000;
  let polls = 0;
  let finalStatus = null;
  while (Date.now() < deadline) {
    polls++;
    const r = http.get(`${BASE_URL}/api/v2/transactions/status/${refId}`);
    if (r.status === 200) {
      const body = r.json();
      const status = body && body.data && body.data.status;
      if (status && status !== "processing" && status !== "accepted") {
        finalStatus = status;
        break;
      }
    } else if (r.status !== 404) {
      console.log(`  poll ${polls}: GET status=${r.status} body=${String(r.body).substring(0,150)}`);
    }
    sleep(0.2);
  }
  const tPoll = Date.now() - tPollStart;
  if (finalStatus === null) {
    console.error(`WEDGE: status never resolved. polls=${polls} elapsed=${tPoll}ms refId=${refId}`);
  } else {
    console.log(`OK status=${finalStatus} after polls=${polls} elapsed=${tPoll}ms refId=${refId}`);
  }
}
