// Shared end-of-test summary writer for the k6 suites.
//
// `buildHandleSummary(testName)` returns a `handleSummary` function
// that writes two artifacts per run into `K6_OUT_DIR` (default
// `k6/output`):
//
//   <testName>-<runId>.summary.json   full k6 summary export (JSON)
//   <testName>-<runId>.summary.txt    k6's native text summary (no colors)
//
// stdout keeps k6's native end-of-test summary unchanged — defining
// `handleSummary` would normally suppress it, so we regenerate it
// here with the vendored `textSummary` (k6/lib/k6-summary.js) and
// return it under the `stdout` key. Colors follow the NO_COLOR
// convention (https://no-color.org): set NO_COLOR to disable them
// when piping the output to a file.
//
// One deliberate constraint: the output directory must already
// exist. k6 does NOT create missing directories for `handleSummary`
// outputs — it logs an error and still exits 0, so a missing path
// silently yields no file. `k6/output/` is kept in the repo via a
// committed `.gitkeep` so the write target is always present.
//
// `runId` is the `RUN_ID` env var when set (a CI run can pin a
// stable filename), otherwise a filesystem-safe UTC timestamp.

import { textSummary } from "./k6-summary.js";

const OUT_DIR = (__ENV.K6_OUT_DIR || "k6/output").replace(/\/+$/, "");

function resolveRunId() {
  if (__ENV.RUN_ID) return __ENV.RUN_ID;
  // 2026-06-01T11-21-45-123Z — ':' and '.' are illegal in Windows
  // filenames, so flatten them to '-'.
  return new Date().toISOString().replace(/[:.]/g, "-");
}

// Returns a `handleSummary` for the named test. Export the result
// as `handleSummary` from the script module.
export function buildHandleSummary(testName) {
  return function handleSummary(data) {
    const runId = resolveRunId();
    const base = `${OUT_DIR}/${testName}-${runId}.summary`;
    return {
      // Native k6 summary, colored as usual unless NO_COLOR is set.
      stdout: textSummary(data, { indent: " ", enableColors: !__ENV.NO_COLOR }),
      // Same report, plain text (no ANSI) for a clean retained file.
      [`${base}.txt`]: textSummary(data, { indent: " ", enableColors: false }),
      // Full machine-parseable export for trend tooling / Grafana.
      [`${base}.json`]: JSON.stringify(data, null, 2),
    };
  };
}
