// Dual-target module: attaches to window.PL in the browser,
// exports via module.exports in Node. Lets `node --test` exercise
// the helpers without a build chain, while a plain
// <script src="metrics.js"> still loads it in the dashboard.
//
// History: this file used to host parseProm + rateOf +
// histogramQuantile for client-side aggregation off the app's
// /metrics endpoint. That approach was broken under the project's
// 2-replica nginx least_conn deployment — consecutive polls landed
// on different replicas with different counter state, producing
// phantom rates (alternating 0/335 rps). The dashboard now queries
// Prometheus aggregations via /prom/ instead (PL.queryProm in
// api.js), so only the rolling buffer helper remains here.

(function () {
  function rollingPush(arr, value, size) {
    const out = arr.length >= size ? arr.slice(1) : arr.slice();
    out.push(value);
    return out;
  }

  const api = { rollingPush };
  if (typeof module !== 'undefined') module.exports = api;
  if (typeof window !== 'undefined') window.PL = Object.assign(window.PL || {}, api);
})();
