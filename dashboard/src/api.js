// HTTP client for the Peakload backend. Every function takes a
// `deps` object whose only required member is `fetch` — so tests
// can pass a stub without monkey-patching globals.
//
// In the browser, the App wires `deps = { fetch: window.fetch.bind(window) }`.

(function () {
  async function readBody(res) {
    try {
      return await res.json();
    } catch {
      return null;
    }
  }

  function errFromBody(res, body) {
    const msg =
      (body && body.error && body.error.message) ||
      (body && body.message) ||
      `HTTP ${res.status}`;
    const err = new Error(msg);
    err.status = res.status;
    err.body = body;
    return err;
  }

  async function sendTxn(deps, payload) {
    const res = await deps.fetch('/api/v2/transactions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const body = await readBody(res);
    if (!res.ok) throw errFromBody(res, body);
    return body;
  }

  async function getBalance(deps, accountId) {
    const res = await deps.fetch(
      `/api/v2/accounts/${encodeURIComponent(accountId)}/balance`,
    );
    const body = await readBody(res);
    if (!res.ok) throw errFromBody(res, body);
    return body;
  }

  async function listRecent(deps, limit = 10) {
    const res = await deps.fetch(`/api/v2/transactions?limit=${limit}`);
    const body = await readBody(res);
    if (!res.ok) throw errFromBody(res, body);
    return body;
  }

  async function getStatus(deps, refId) {
    const res = await deps.fetch(
      `/api/v2/transactions/status/${encodeURIComponent(refId)}`,
    );
    const body = await readBody(res);
    if (!res.ok) throw errFromBody(res, body);
    return body;
  }

  async function getHealth(deps) {
    const res = await deps.fetch('/health');
    const body = await readBody(res);
    if (!res.ok) throw errFromBody(res, body);
    return body;
  }

  async function getMetrics(deps) {
    const res = await deps.fetch('/metrics');
    if (!res.ok) throw new Error(`metrics HTTP ${res.status}`);
    return await res.text();
  }

  async function pollStatus(deps, refId, opts) {
    const intervalMs = (opts && opts.intervalMs) || 250;
    const timeoutMs = (opts && opts.timeoutMs) || 5000;
    const sleepFn = (opts && opts.sleep) || ((ms) => new Promise((r) => setTimeout(r, ms)));
    const now = (opts && opts.now) || (() => Date.now());

    const deadline = now() + timeoutMs;
    while (now() < deadline) {
      try {
        const body = await getStatus(deps, refId);
        const status = body && body.data && body.data.status;
        if (status === 'completed' || status === 'failed') {
          return body.data;
        }
        // status === "pending" → keep polling
      } catch (e) {
        // 404 or transient — keep polling until deadline.
        // Mirrors the k6 setup-poll behavior at
        // k6/load-test-1m.js:188-209.
      }
      await sleepFn(intervalMs);
    }
    const err = new Error(`pollStatus timeout after ${timeoutMs}ms for ref ${refId}`);
    err.code = 'POLL_TIMEOUT';
    throw err;
  }

  const api = {
    sendTxn, getBalance, listRecent, getStatus, getHealth, getMetrics, pollStatus,
  };
  if (typeof module !== 'undefined') module.exports = api;
  if (typeof window !== 'undefined') window.PL = Object.assign(window.PL || {}, api);
})();
