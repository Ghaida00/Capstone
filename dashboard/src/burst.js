// Concurrent POST orchestrator with a hard cap (spec §"Burst N").
// Uses a worker-pool pattern: K workers pull from a shared index
// counter until it reaches n. Simpler than batch-chunking and
// smooths out per-request latency variance.

(function () {
  const HARD_CAP = 100;

  async function runBurst(opts) {
    const concurrency = Math.max(1, opts.concurrency || 20);
    const n = Math.min(HARD_CAP, Math.max(0, opts.n | 0));
    const fetcher = opts.fetcher;
    const payloadGen = opts.payloadGen;

    const start = Date.now();
    let ok = 0;
    let failed = 0;
    let maxLatencyMs = 0;
    let next = 0;

    async function worker() {
      while (true) {
        const idx = next++;
        if (idx >= n) return;
        const t0 = Date.now();
        try {
          await fetcher(payloadGen(idx));
          ok++;
        } catch (_err) {
          failed++;
        }
        const dt = Date.now() - t0;
        if (dt > maxLatencyMs) maxLatencyMs = dt;
      }
    }

    const workers = Array.from(
      { length: Math.min(concurrency, n) },
      () => worker(),
    );
    await Promise.all(workers);

    return { ok, failed, maxLatencyMs, totalMs: Date.now() - start };
  }

  function genRefId(prefix = 'burst') {
    return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  }

  const api = { runBurst, genRefId, HARD_CAP };
  if (typeof module !== 'undefined') module.exports = api;
  if (typeof window !== 'undefined') window.PL = Object.assign(window.PL || {}, api);
})();
