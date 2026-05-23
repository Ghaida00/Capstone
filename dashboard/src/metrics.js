// Dual-target module: attaches to window.PL in the browser,
// exports via module.exports in Node. Lets `node --test` exercise
// the parser + math without a build chain, while a plain
// <script src="metrics.js"> still loads it in the dashboard.

(function () {
  function parseProm(text) {
    const out = {};
    for (const raw of text.split('\n')) {
      const line = raw.trim();
      if (!line || line.startsWith('#')) continue;

      // Split metric-name-and-labels from value. Labels are inside
      // {...}, which can contain spaces inside quoted values — so a
      // naive lastIndexOf(' ') would slice inside a label. Find the
      // boundary after the closing brace; if there's no brace, the
      // first whitespace is fine.
      let split;
      if (line.includes('{')) {
        const close = line.indexOf('}');
        if (close === -1) continue;
        split = close + 1;
      } else {
        split = line.indexOf(' ');
        if (split === -1) continue;
      }

      const key = line.slice(0, split).trim();
      const rest = line.slice(split).trim();
      const valStr = rest.split(/\s+/)[0]; // value, ignore optional timestamp
      const val = parseFloat(valStr);
      if (Number.isNaN(val)) continue;

      out[key] = val;
    }
    return out;
  }

  function rateOf(prev, curr, dtSec) {
    if (dtSec <= 0) return 0;
    if (curr < prev) return 0; // counter reset / app restart
    return (curr - prev) / dtSec;
  }

  function histogramQuantile(buckets, q) {
    // buckets: { "0.005": cum, "0.01": cum, ..., "+Inf": cum }
    // already cumulative per Prometheus text-format convention.
    const entries = Object.entries(buckets)
      .map(([le, count]) => [le === '+Inf' ? Infinity : parseFloat(le), count])
      .sort((a, b) => a[0] - b[0]);
    if (entries.length === 0) return 0;
    const total = entries[entries.length - 1][1];
    if (total === 0) return 0;
    const target = q * total;
    for (const [le, count] of entries) {
      if (count >= target) return le;
    }
    return entries[entries.length - 1][0];
  }

  function rollingPush(arr, value, size) {
    const out = arr.length >= size ? arr.slice(1) : arr.slice();
    out.push(value);
    return out;
  }

  const api = { parseProm, rateOf, histogramQuantile, rollingPush };
  if (typeof module !== 'undefined') module.exports = api;
  if (typeof window !== 'undefined') window.PL = Object.assign(window.PL || {}, api);
})();
