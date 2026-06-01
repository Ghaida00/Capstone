// Shared end-of-test summary writer for the k6 suites.
//
// `buildHandleSummary(testName)` returns a `handleSummary` function
// that writes two artifacts per run into `K6_OUT_DIR` (default
// `k6/output`):
//
//   <testName>-<runId>.summary.json   full k6 summary export (JSON)
//   <testName>-<runId>.summary.txt    compact human-readable report
//
// The same human report is also returned for `stdout`, replacing
// k6's default end-of-test summary.
//
// Two deliberate constraints:
//   - The output directory must already exist. k6 does NOT create
//     missing directories for `handleSummary` outputs — it logs an
//     error and still exits 0, so a missing path silently yields no
//     file. `k6/output/` is kept in the repo via a committed
//     `.gitkeep` so the write target is always present on checkout.
//   - The report is hand-rolled rather than imported from
//     `jslib.k6.io`, so an offline run (e.g. a multi-hour local
//     soak) can never fail at init on a CDN fetch.
//
// `runId` is the `RUN_ID` env var when set (a CI run can pin a
// stable filename), otherwise a filesystem-safe UTC timestamp.

const OUT_DIR = (__ENV.K6_OUT_DIR || "k6/output").replace(/\/+$/, "");

function resolveRunId() {
  if (__ENV.RUN_ID) return __ENV.RUN_ID;
  // 2026-06-01T11-21-45-123Z — ':' and '.' are illegal in Windows
  // filenames, so flatten them to '-'.
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function fmt(n) {
  if (n === undefined || n === null || Number.isNaN(n)) return "—";
  if (Math.abs(n) >= 100) return n.toFixed(0);
  if (Math.abs(n) >= 1) return n.toFixed(2);
  return n.toFixed(4);
}

// The value keys worth printing, per metric type. Trends show the
// latency shape; rates show the pass/fail split; counters show the
// total and the per-second rate. Unknown types fall back to every
// value the metric carries.
function valueKeysFor(metric) {
  switch (metric.type) {
    case "trend":
      return ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"];
    case "rate":
      return ["rate", "passes", "fails"];
    case "counter":
      return ["count", "rate"];
    case "gauge":
      return ["value"];
    default:
      return Object.keys(metric.values || {});
  }
}

function metricLine(name, metric) {
  const values = metric.values || {};
  const parts = valueKeysFor(metric)
    .filter((k) => values[k] !== undefined)
    .map((k) => `${k}=${fmt(values[k])}`);
  return `  ${name}\n    ${parts.join("  ")}`;
}

function buildReport(testName, runId, data) {
  const metrics = data.metrics || {};
  const names = Object.keys(metrics).sort();
  const lines = [];

  lines.push("══════════════════════════════════════════════════════════");
  lines.push(`  k6 summary — ${testName}`);
  lines.push(`  run_id = ${runId}`);
  lines.push("══════════════════════════════════════════════════════════");

  // ── Thresholds ──
  lines.push("");
  lines.push("Thresholds:");
  let anyThreshold = false;
  let anyFail = false;
  names.forEach((name) => {
    const thresholds = metrics[name].thresholds;
    if (!thresholds) return;
    Object.keys(thresholds).forEach((expr) => {
      anyThreshold = true;
      const ok = thresholds[expr].ok;
      if (!ok) anyFail = true;
      lines.push(`  [${ok ? "PASS" : "FAIL"}] ${name}: ${expr}`);
    });
  });
  if (!anyThreshold) lines.push("  (none defined)");

  // ── Per-metric values ──
  lines.push("");
  lines.push("Metrics:");
  names.forEach((name) => lines.push(metricLine(name, metrics[name])));

  lines.push("");
  lines.push(
    `Result: ${anyFail ? "❌ THRESHOLD(S) FAILED" : "✅ all thresholds passed"}`
  );
  lines.push("");
  return lines.join("\n");
}

// Returns a `handleSummary` for the named test. Export the result
// as `handleSummary` from the script module.
export function buildHandleSummary(testName) {
  return function handleSummary(data) {
    const runId = resolveRunId();
    const base = `${OUT_DIR}/${testName}-${runId}.summary`;
    const report = buildReport(testName, runId, data);
    const out = {};
    out["stdout"] = "\n" + report;
    out[`${base}.json`] = JSON.stringify(data, null, 2);
    out[`${base}.txt`] = report;
    return out;
  };
}
