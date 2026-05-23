const { test } = require('node:test');
const assert = require('node:assert/strict');
const { rollingPush } = require('../src/metrics.js');

// parseProm / rateOf / histogramQuantile were removed when the
// dashboard switched from client-side /metrics text-parsing to
// PromQL queries through the /prom/ nginx proxy. Aggregation
// happens server-side in Prometheus now — see app.jsx PROM_QUERIES.

test('rollingPush: under capacity grows', () => {
  assert.deepEqual(rollingPush([1, 2], 3, 5), [1, 2, 3]);
});

test('rollingPush: at capacity evicts oldest', () => {
  assert.deepEqual(rollingPush([1, 2, 3], 4, 3), [2, 3, 4]);
});

test('rollingPush: does not mutate input', () => {
  const a = [1, 2, 3];
  rollingPush(a, 4, 3);
  assert.deepEqual(a, [1, 2, 3]);
});
