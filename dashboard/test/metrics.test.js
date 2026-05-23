const { test } = require('node:test');
const assert = require('node:assert/strict');
const { parseProm, rateOf, histogramQuantile, rollingPush } = require('../src/metrics.js');

test('parseProm: simple counter line', () => {
  const text = '# HELP foo description\n# TYPE foo counter\nfoo 42\n';
  assert.deepEqual(parseProm(text), { foo: 42 });
});

test('parseProm: labels with quoted commas are preserved as a single key', () => {
  const text = 'foo{a="1",b="2"} 7\n';
  assert.deepEqual(parseProm(text), { 'foo{a="1",b="2"}': 7 });
});

test('parseProm: histogram emits _bucket / _sum / _count as distinct keys', () => {
  const text = [
    'http_request_duration_seconds_bucket{le="0.05"} 1000',
    'http_request_duration_seconds_bucket{le="+Inf"} 1200',
    'http_request_duration_seconds_sum 25.5',
    'http_request_duration_seconds_count 1200',
  ].join('\n');
  const got = parseProm(text);
  assert.equal(got['http_request_duration_seconds_bucket{le="0.05"}'], 1000);
  assert.equal(got['http_request_duration_seconds_bucket{le="+Inf"}'], 1200);
  assert.equal(got['http_request_duration_seconds_sum'], 25.5);
  assert.equal(got['http_request_duration_seconds_count'], 1200);
});

test('parseProm: blank lines and comments ignored', () => {
  const text = '\n# HELP a x\n# TYPE a counter\n\na 5\n# more\n';
  assert.deepEqual(parseProm(text), { a: 5 });
});

test('rateOf: simple delta over seconds', () => {
  assert.equal(rateOf(100, 150, 2), 25);
});

test('rateOf: counter reset returns 0, not negative', () => {
  assert.equal(rateOf(500, 10, 1), 0);
});

test('rateOf: zero or negative dt returns 0', () => {
  assert.equal(rateOf(10, 20, 0), 0);
  assert.equal(rateOf(10, 20, -1), 0);
});

test('histogramQuantile: returns smallest le where cumulative >= q*total', () => {
  const buckets = { '0.005': 10, '0.01': 50, '0.05': 95, '0.5': 99, '+Inf': 100 };
  // p95 = first le with cum >= 95 → 0.05
  assert.equal(histogramQuantile(buckets, 0.95), 0.05);
});

test('histogramQuantile: 0.99 lands in 0.5 bucket', () => {
  const buckets = { '0.005': 10, '0.01': 50, '0.05': 95, '0.5': 99, '+Inf': 100 };
  assert.equal(histogramQuantile(buckets, 0.99), 0.5);
});

test('histogramQuantile: empty buckets returns 0', () => {
  assert.equal(histogramQuantile({}, 0.95), 0);
});

test('histogramQuantile: total zero returns 0', () => {
  assert.equal(histogramQuantile({ '0.005': 0, '+Inf': 0 }, 0.95), 0);
});

test('histogramQuantile: tail-only case returns Infinity', () => {
  // All counts land in +Inf — bucket boundaries set too low for the data.
  const buckets = { '0.005': 0, '0.01': 0, '+Inf': 100 };
  assert.equal(histogramQuantile(buckets, 0.95), Infinity);
});

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
