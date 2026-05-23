const { test } = require('node:test');
const assert = require('node:assert/strict');

test('node --test harness is alive', () => {
  assert.equal(1 + 1, 2);
});
