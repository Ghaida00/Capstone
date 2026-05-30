const { test } = require('node:test');
const assert = require('node:assert/strict');
const { runBurst, genRefId, HARD_CAP } = require('../src/burst.js');

function makeFakeFetcher(behavior = () => ({ ok: true })) {
  const state = { calls: 0, inflight: 0, peakInflight: 0 };
  async function fetcher(payload) {
    // Capture call ordinal BEFORE the await — reading state.calls
    // afterwards is racy under concurrency.
    const callIdx = state.calls;
    state.calls++;
    state.inflight++;
    state.peakInflight = Math.max(state.peakInflight, state.inflight);
    try {
      await new Promise(r => setTimeout(r, 1));
      const res = behavior(payload, callIdx);
      if (res && res.throw) throw res.throw;
      return res;
    } finally {
      state.inflight--;
    }
  }
  return { fetcher, state };
}

test('runBurst: dispatches exactly n requests', async () => {
  const { fetcher, state } = makeFakeFetcher();
  const out = await runBurst({
    n: 10, concurrency: 3, fetcher,
    payloadGen: i => ({ idx: i }),
  });
  assert.equal(state.calls, 10);
  assert.equal(out.ok, 10);
  assert.equal(out.failed, 0);
});

test('runBurst: respects concurrency cap (peak inflight <= concurrency)', async () => {
  const { fetcher, state } = makeFakeFetcher();
  await runBurst({
    n: 20, concurrency: 5, fetcher,
    payloadGen: i => ({ idx: i }),
  });
  assert.ok(state.peakInflight <= 5, `peakInflight=${state.peakInflight} exceeded cap 5`);
});

test('runBurst: failures are counted, not propagated', async () => {
  const { fetcher } = makeFakeFetcher((_p, i) =>
    i % 2 === 0 ? { ok: true } : { throw: new Error('boom') },
  );
  const out = await runBurst({
    n: 10, concurrency: 3, fetcher,
    payloadGen: i => ({ idx: i }),
  });
  assert.equal(out.ok, 5);
  assert.equal(out.failed, 5);
});

test('runBurst: synchronous throws are also counted as failed', async () => {
  const fetcher = () => { throw new Error('sync boom'); };
  const out = await runBurst({
    n: 4, concurrency: 2, fetcher,
    payloadGen: i => ({ idx: i }),
  });
  assert.equal(out.ok, 0);
  assert.equal(out.failed, 4);
});

test('runBurst: clamps n to HARD_CAP', async () => {
  const { fetcher, state } = makeFakeFetcher();
  const out = await runBurst({
    n: 500, concurrency: 10, fetcher,
    payloadGen: i => ({ idx: i }),
  });
  assert.equal(state.calls, HARD_CAP);
  assert.equal(out.ok + out.failed, HARD_CAP);
});

test('runBurst: maxLatencyMs reflects slowest single call', async () => {
  const fetcher = async (p) => {
    await new Promise(r => setTimeout(r, p.idx === 3 ? 50 : 1));
  };
  const out = await runBurst({
    n: 6, concurrency: 3, fetcher,
    payloadGen: i => ({ idx: i }),
  });
  assert.ok(out.maxLatencyMs >= 45, `expected >=45ms got ${out.maxLatencyMs}`);
});

test('runBurst: n=0 returns zeros without calling fetcher', async () => {
  const { fetcher, state } = makeFakeFetcher();
  const out = await runBurst({
    n: 0, concurrency: 5, fetcher,
    payloadGen: i => ({ idx: i }),
  });
  assert.equal(state.calls, 0);
  assert.deepEqual({ ok: out.ok, failed: out.failed }, { ok: 0, failed: 0 });
});

test('genRefId: produces distinct ids', () => {
  const seen = new Set();
  for (let i = 0; i < 100; i++) seen.add(genRefId());
  assert.equal(seen.size, 100);
});

test('genRefId: prefix is honored', () => {
  assert.match(genRefId('manual'), /^manual-/);
});
