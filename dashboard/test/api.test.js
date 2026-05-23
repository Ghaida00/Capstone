const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  sendTxn, getBalance, listRecent, getStatus, pollStatus, getHealth, getMetrics,
} = require('../src/api.js');

function fakeResponse({ status = 200, body }) {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  };
}

test('sendTxn: posts to /api/v2/transactions with JSON body', async () => {
  let captured = null;
  const fetch = async (url, init) => {
    captured = { url, init };
    return fakeResponse({
      status: 202,
      body: { success: true, data: { reference_id: 'r1', status: 'pending', message: 'queued' } },
    });
  };
  const out = await sendTxn({ fetch }, {
    from_account: 'ACC_1', to_account: 'ACC_2', amount: '10.00',
    currency: 'IDR', reference_id: 'r1',
  });
  assert.equal(captured.url, '/api/v2/transactions');
  assert.equal(captured.init.method, 'POST');
  assert.equal(captured.init.headers['Content-Type'], 'application/json');
  const parsed = JSON.parse(captured.init.body);
  assert.equal(parsed.from_account, 'ACC_1');
  assert.equal(out.data.reference_id, 'r1');
});

test('sendTxn: 4xx surfaces API error message, not generic fetch failed', async () => {
  const fetch = async () => fakeResponse({
    status: 400,
    body: { success: false, error: { code: 'bad_request', message: 'amount must be positive' } },
  });
  await assert.rejects(
    () => sendTxn({ fetch }, { from_account: 'X', to_account: 'Y', amount: '0', currency: 'IDR' }),
    (err) => err.message === 'amount must be positive' && err.status === 400,
  );
});

test('sendTxn: 5xx with no body falls back to HTTP status', async () => {
  const fetch = async () => ({
    ok: false,
    status: 503,
    json: async () => { throw new Error('not json'); },
  });
  await assert.rejects(
    () => sendTxn({ fetch }, {}),
    (err) => err.message === 'HTTP 503' && err.status === 503,
  );
});

test('getBalance: URL-encodes account id', async () => {
  let captured;
  const fetch = async (url) => {
    captured = url;
    return fakeResponse({ status: 200, body: { success: true, data: { balance: '100' } } });
  };
  await getBalance({ fetch }, 'ACC_0000001');
  assert.equal(captured, '/api/v2/accounts/ACC_0000001/balance');
});

test('listRecent: defaults limit=10, hits the list endpoint', async () => {
  let captured;
  const fetch = async (url) => {
    captured = url;
    return fakeResponse({ status: 200, body: { success: true, data: [] } });
  };
  await listRecent({ fetch });
  assert.equal(captured, '/api/v2/transactions?limit=10');
});

test('getStatus: hits /status/:ref', async () => {
  let captured;
  const fetch = async (url) => {
    captured = url;
    return fakeResponse({ status: 200, body: { success: true, data: { reference_id: 'abc', status: 'pending' } } });
  };
  await getStatus({ fetch }, 'abc');
  assert.equal(captured, '/api/v2/transactions/status/abc');
});

test('getMetrics: returns raw text', async () => {
  const fetch = async () => ({
    ok: true,
    status: 200,
    text: async () => '# HELP foo bar\nfoo 1\n',
  });
  const out = await getMetrics({ fetch });
  assert.match(out, /foo 1/);
});

test('getHealth: returns parsed JSON', async () => {
  const fetch = async () => fakeResponse({
    status: 200,
    body: { status: 'healthy', services: { database_write: true } },
  });
  const out = await getHealth({ fetch });
  assert.equal(out.status, 'healthy');
});

test('pollStatus: resolves on completed', async () => {
  let n = 0;
  const fetch = async () => {
    n++;
    return fakeResponse({
      status: 200,
      body: { success: true, data: { reference_id: 'x', status: n < 3 ? 'pending' : 'completed' } },
    });
  };
  const out = await pollStatus({ fetch }, 'x', { intervalMs: 1, timeoutMs: 1000 });
  assert.equal(out.status, 'completed');
});

test('pollStatus: keeps polling on 200+pending', async () => {
  let n = 0;
  const fetch = async () => {
    n++;
    return fakeResponse({
      status: 200,
      body: { success: true, data: { reference_id: 'x', status: n < 5 ? 'pending' : 'failed' } },
    });
  };
  const out = await pollStatus({ fetch }, 'x', { intervalMs: 1, timeoutMs: 1000 });
  assert.equal(out.status, 'failed');
  assert.ok(n >= 5);
});

test('pollStatus: keeps polling on 404 (race window) and resolves later', async () => {
  let n = 0;
  const fetch = async () => {
    n++;
    if (n < 3) return fakeResponse({ status: 404, body: { error: { message: 'not yet' } } });
    return fakeResponse({ status: 200, body: { success: true, data: { reference_id: 'x', status: 'completed' } } });
  };
  const out = await pollStatus({ fetch }, 'x', { intervalMs: 1, timeoutMs: 1000 });
  assert.equal(out.status, 'completed');
});

test('pollStatus: rejects with POLL_TIMEOUT', async () => {
  const fetch = async () => fakeResponse({
    status: 200,
    body: { success: true, data: { reference_id: 'x', status: 'pending' } },
  });
  await assert.rejects(
    () => pollStatus({ fetch }, 'x', { intervalMs: 1, timeoutMs: 5 }),
    (err) => err.code === 'POLL_TIMEOUT',
  );
});
