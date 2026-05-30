/* Peakload dashboard — App: state, polling loops, action handlers.
   Reads kernels from window.PL and components from window globals.

   Whole file wrapped in an IIFE because Babel-standalone injects each
   transpiled text/babel script as an inline <script> in the document.
   Top-level `const` in classic scripts shares a lexical environment
   across scripts, so a second `const { useState, ... } = React;` here
   would collide with the same destructure at the top of components.jsx
   ("Identifier 'useState' has already been declared"). */

(function () {

const { useState, useEffect, useRef, useCallback } = React;
const PL = window.PL;

const METRICS_TICK_MS = 1000;
const HEALTH_TICK_MS  = 5000;
const RPS_WINDOW      = 40;
const LAT_WINDOW      = 32;
const SPARK_WINDOW    = 24;
const LOG_MAX         = 60;
const SUMMARY_EVERY_N_TICKS = 8;
const STATUS_POLL_INTERVAL_MS = 250;
const STATUS_POLL_TIMEOUT_MS  = 5000;
const BURST_CONCURRENCY = 20;

// Exclude the dashboard's own /health and /prom polls from the
// observed request totals. Slim regex; we don't filter /metrics
// since the dashboard doesn't hit it anymore (PromQL replaces it).
const EXCLUDE_PATHS_PROMQL = 'http_response_status_code=~"5..",url_path!~"/(health|metrics|dashboard.*).*"';

const deps = { fetch: window.fetch.bind(window) };

/* ---------- Pure helpers ---------- */

// All metrics now come from Prometheus PromQL queries against the
// /prom/ nginx proxy. Prometheus aggregates the per-replica counters
// across both app instances server-side (sum()), eliminating the
// "alternating reads under nginx least_conn" problem that made
// client-side aggregation off /metrics produce phantom rates.
//
// Rate windows: 15s for counters (3 scrapes at 5s scrape_interval),
// 60s for histograms (more samples = stabler quantile estimates).
const PROM_QUERIES = {
  acceptedRps:   'sum(rate(transactions_created_total[15s]))',
  processedRps: 'sum(rate(transactions_processed_total[15s]))',
  errorsRps:    `sum(rate(http_requests_total{${EXCLUDE_PATHS_PROMQL}}[15s]))`,
  p95Ms:        'histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[1m]))) * 1000',
  p99Ms:        'histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[1m]))) * 1000',
  acceptedTotal: 'sum(transactions_created_total)',
};

// Map /health JSON → 6 + N rows (one per service + one per replica).
// B2 fix: when health is null (fetch failed), the caller is expected
// to keep the previously rendered rows and just flag the cluster as
// degraded in the header — see the health-poll catch branch. This
// function only handles the happy path now.
function healthToRows(health) {
  const s = health.services || {};
  const rows = [
    { id: 'app',    name: 'app',      meta: 'axum · :3000', glyph: 'A',  status: health.status === 'healthy' ? 'ok' : 'warn' },
    { id: 'nginx',  name: 'nginx',    meta: 'edge · :8080', glyph: 'NG', status: 'ok' }, // if we got a response, nginx is up
    { id: 'dbw',    name: 'db-write', meta: 'pgbouncer',    glyph: 'DW', status: s.database_write ? 'ok' : 'err' },
    { id: 'dbr',    name: 'db-read',  meta: 'pgbouncer',    glyph: 'DR', status: s.database_read  ? 'ok' : 'err' },
    { id: 'redis',  name: 'redis',    meta: 'cache',        glyph: 'RD', status: s.redis    ? 'ok' : 'err' },
    { id: 'rabbit', name: 'rabbitmq', meta: 'queue',        glyph: 'RB', status: s.rabbitmq ? 'ok' : 'err' },
  ];
  for (const r of (s.replicas || [])) {
    rows.push({
      id: `shard-${r.shard}-replicas`,
      name: `shard-${r.shard}`,
      meta: `${r.healthy}/${r.total} replicas`,
      glyph: `S${r.shard}`,
      status: r.healthy === r.total ? 'ok' : (r.healthy === 0 ? 'err' : 'warn'),
    });
  }
  return rows;
}

/* ---------- App ---------- */

function App() {
  const toast = useToast();

  const [rps, setRps]           = useState([]);
  const [lat, setLat]           = useState([]);
  const [accSpark, setAccSpark] = useState([]);
  const [procSpark, setProcSpark] = useState([]);
  const [p95Spark, setP95Spark] = useState([]);
  const [p99Spark, setP99Spark] = useState([]);
  const [errSpark, setErrSpark] = useState([]);
  const [acceptedRps, setAcceptedRps]   = useState(0);
  const [processedRps, setProcessedRps] = useState(0);
  const [p95, setP95] = useState(0);
  const [p99, setP99] = useState(0);
  const [errRps, setErrRps] = useState(0);
  const [acceptedTotal, setAcceptedTotal] = useState(0);
  const [logEntries, setLogEntries] = useState([]);
  const [components, setComponents] = useState([]);
  const [healthStatus, setHealthStatus] = useState('healthy');
  const [sending, setSending] = useState(false);
  const [bursting, setBursting] = useState(false);

  const tickRef = useRef(0);
  const prevHealthRef = useRef(null);

  const addLog = useCallback((entry) => {
    // B1 fix: stamp a unique id per entry and clear `fresh` only for
    // that id later. Previous version cleared every entry's fresh flag
    // on every timer fire, so a burst of logs killed each other's
    // highlight animations.
    const id = Math.random().toString(36).slice(2);
    setLogEntries(prev => [{
      id, t: fmtTime(), fresh: true, ...entry,
    }, ...prev].slice(0, LOG_MAX));
    setTimeout(() => {
      setLogEntries(prev => prev.map(e => e.id === id ? { ...e, fresh: false } : e));
    }, 400);
  }, []);

  /* ----- Metrics poll (PromQL via /prom/ proxy) ----- */

  useEffect(() => {
    let cancelled = false;

    async function tick() {
      try {
        // Fire all 6 queries in parallel; Prometheus handles each one
        // in microseconds and the round-trip is the only cost.
        const [aRps, pRps, eRps, p95v, p99v, accTotal] = await Promise.all([
          PL.queryProm(deps, PROM_QUERIES.acceptedRps),
          PL.queryProm(deps, PROM_QUERIES.processedRps),
          PL.queryProm(deps, PROM_QUERIES.errorsRps),
          PL.queryProm(deps, PROM_QUERIES.p95Ms),
          PL.queryProm(deps, PROM_QUERIES.p99Ms),
          PL.queryProm(deps, PROM_QUERIES.acceptedTotal),
        ]);
        if (cancelled) return;

        // queryProm returns null when a series has no data yet (e.g.
        // p95 right after restart with zero observations). Coerce to
        // 0 for display.
        const accepted   = aRps    ?? 0;
        const processed  = pRps    ?? 0;
        const errors     = eRps    ?? 0;
        const p95v_      = p95v    ?? 0;
        const p99v_      = p99v    ?? 0;
        const totalAcc   = Math.round(accTotal ?? 0);

        setAcceptedRps(accepted);
        setProcessedRps(processed);
        setErrRps(errors);
        setP95(p95v_);
        setP99(p99v_);
        setAcceptedTotal(totalAcc);

        setRps(arr => PL.rollingPush(arr, Math.round(accepted), RPS_WINDOW));
        setLat(arr => PL.rollingPush(arr, Math.round(p95v_), LAT_WINDOW));
        setAccSpark(arr => PL.rollingPush(arr, Math.round(accepted), SPARK_WINDOW));
        setProcSpark(arr => PL.rollingPush(arr, Math.round(processed), SPARK_WINDOW));
        setP95Spark(arr => PL.rollingPush(arr, Math.round(p95v_), SPARK_WINDOW));
        setP99Spark(arr => PL.rollingPush(arr, Math.round(p99v_), SPARK_WINDOW));
        setErrSpark(arr => PL.rollingPush(arr, Math.round(errors * 100) / 100, SPARK_WINDOW));

        tickRef.current++;
        if (tickRef.current % SUMMARY_EVERY_N_TICKS === 0) {
          addLog({
            tag: 'info',
            msg: <>k6 sustained <span className="num">{Math.round(accepted)} rps</span> · p95 <span className="num">{Math.round(p95v_)} ms</span> · {Math.round(errors)} 5xx/s</>,
          });
        }
      } catch (e) {
        // Prometheus down or query parse error. Only log every 10th
        // failure to avoid drowning the activity stream.
        if (cancelled) return;
        if (tickRef.current % 10 === 0) {
          addLog({ tag: 'warn', msg: <>Prometheus query failed: {String(e.message)}</> });
        }
        tickRef.current++;
      }
    }

    tick();
    const id = setInterval(tick, METRICS_TICK_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, [addLog]);

  /* ----- Health poll ----- */

  useEffect(() => {
    let cancelled = false;

    async function tick() {
      try {
        const h = await PL.getHealth(deps);
        if (cancelled) return;
        setComponents(healthToRows(h));
        setHealthStatus(h.status);

        const prev = prevHealthRef.current;
        if (prev && prev !== h.status) {
          if (h.status === 'degraded') {
            addLog({ tag: 'warn', msg: <>health transition: <span className="accent">degraded</span></> });
          } else {
            addLog({ tag: 'info', msg: <>health transition: <span className="accent">healthy</span></> });
          }
        }
        prevHealthRef.current = h.status;
      } catch (e) {
        // B2 + B3 fix: don't erase the component rows on a transient
        // /health blip — just flip the header pill. Audience sees
        // "cluster degraded" without the card disappearing. Also log
        // the transition the FIRST time we go degraded, so the
        // activity stream has a paper trail before the eventual
        // "health transition: healthy" log on recovery.
        if (cancelled) return;
        setHealthStatus('degraded');
        const prev = prevHealthRef.current;
        if (prev && prev !== 'degraded') {
          addLog({ tag: 'warn', msg: <>/health unreachable — cluster marked <span className="accent">degraded</span></> });
        }
        prevHealthRef.current = 'degraded';
      }
    }

    tick();
    const id = setInterval(tick, HEALTH_TICK_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, [addLog]);

  /* ----- Action handlers ----- */

  const onSend = useCallback(async ({ from, to, amt }) => {
    if (sending) return;
    setSending(true);
    const refId = PL.genRefId('manual');
    // tStart is the wall-clock origin for *both* timers: request → pending
    // (accept time) AND request → done (full end-to-end). The previous
    // version measured the post-202 poll duration as "completed in X ms",
    // which hid the accept latency from the displayed end-to-end number.
    const tStart = Date.now();
    try {
      const res = await PL.sendTxn(deps, {
        from_account: from, to_account: to, amount: amt,
        currency: 'IDR', reference_id: refId, description: 'dashboard manual',
      });
      const tPendingAt = Date.now();
      const acceptMs = tPendingAt - tStart;
      addLog({
        tag: 'info',
        msg: <>POST /transactions → <span className="accent">pending</span> in <span className="num">{acceptMs}ms</span> · ref=<span className="mono">{refId}</span></>,
      });
      try {
        const final = await PL.pollStatus(deps, refId, {
          intervalMs: STATUS_POLL_INTERVAL_MS, timeoutMs: STATUS_POLL_TIMEOUT_MS,
        });
        const tDoneAt = Date.now();
        const e2eMs = tDoneAt - tStart;          // request → done (the honest end-to-end)
        const asyncMs = tDoneAt - tPendingAt;    // pending → done (queue + batch + DB)
        addLog({
          tag: final.status === 'completed' ? 'ok' : 'warn',
          msg: <>ref=<span className="mono">{refId}</span> → <span className="accent">{final.status}</span> · end-to-end <span className="num">{e2eMs}ms</span> (accept {acceptMs}ms + async {asyncMs}ms)</>,
        });
        toast(`${final.status} in ${e2eMs}ms`, { detail: `accept ${acceptMs}ms · async ${asyncMs}ms` });
      } catch (timeout) {
        addLog({ tag: 'warn', msg: <>ref=<span className="mono">{refId}</span> status-poll timeout (accept was {acceptMs}ms)</> });
      }
    } catch (e) {
      addLog({ tag: 'warn', msg: <>POST /transactions failed: {String(e.message)}</> });
      toast('Send failed', { detail: String(e.message).slice(0, 60) });
    } finally {
      setSending(false);
    }
  }, [sending, addLog, toast]);

  const onBurst = useCallback(async ({ n }) => {
    if (bursting || !n) return;
    setBursting(true);
    // refTimes records the wall-clock moment each POST was actually
    // issued (worker pool != linear). Pairing with the completion
    // timestamp from pollStatus gives a true per-request end-to-end
    // number — the previous "total 805ms" only measured accept-only,
    // matching what k6 reports and hiding the queue + batch + DB
    // latency every burst transaction also pays.
    const refs = [];
    const refTimes = new Map();
    try {
      const out = await PL.runBurst({
        n,
        concurrency: BURST_CONCURRENCY,
        fetcher: (payload) => {
          refTimes.set(payload.reference_id, Date.now());
          return PL.sendTxn(deps, payload);
        },
        payloadGen: (i) => {
          const refId = PL.genRefId(`burst-${i}`);
          refs.push(refId);
          return {
            from_account: 'ACC_0000001',
            to_account: 'ACC_0000042',
            amount: '1.00',
            currency: 'IDR',
            reference_id: refId,
            description: 'dashboard burst',
          };
        },
      });

      // Phase 1: accept-only summary (mirrors what k6 / http_req_duration
      // measure — fast, comparable across tools).
      addLog({
        tag: out.failed === 0 ? 'ok' : 'warn',
        msg: <>BURST × {out.ok + out.failed} · {out.failed === 0 ? <span className="accent">100% accepted</span> : <span className="accent">{out.ok} accepted / {out.failed} failed</span>} in <span className="num">{out.totalMs}ms</span> (max single accept <span className="num">{out.maxLatencyMs}ms</span>) · polling end-to-end…</>,
      });

      // Phase 2: end-to-end per ref. Promise.all is fine here — each
      // pollStatus is GET-only and we already capped the burst at 100,
      // so the inflight read load is bounded. Total wall-clock ~= the
      // slowest end-to-end (the worker pool naturally batches polls).
      if (out.ok > 0 && refs.length > 0) {
        const polled = await Promise.all(refs.map(async (ref) => {
          const postedAt = refTimes.get(ref);
          if (postedAt == null) return null;
          try {
            const final = await PL.pollStatus(deps, ref, {
              intervalMs: STATUS_POLL_INTERVAL_MS,
              timeoutMs: STATUS_POLL_TIMEOUT_MS,
            });
            return { ref, status: final.status, e2eMs: Date.now() - postedAt };
          } catch (_) {
            return { ref, status: 'timeout', e2eMs: null };
          }
        }));
        const completed = polled.filter(p => p && p.status === 'completed');
        const stuck = polled.filter(p => p && p.status !== 'completed');
        const e2es = completed.map(p => p.e2eMs);
        const maxE2E = e2es.length ? Math.max(...e2es) : 0;
        const minE2E = e2es.length ? Math.min(...e2es) : 0;
        const avgE2E = e2es.length ? Math.round(e2es.reduce((s, v) => s + v, 0) / e2es.length) : 0;
        addLog({
          tag: stuck.length === 0 ? 'ok' : 'warn',
          msg: <>BURST × {out.ok} end-to-end: <span className="accent">{completed.length} completed</span>{stuck.length > 0 && <>, {stuck.length} stuck</>} · range <span className="num">{minE2E}–{maxE2E}ms</span> · avg <span className="num">{avgE2E}ms</span></>,
        });
        toast(`Burst e2e: max ${maxE2E}ms`, { detail: `${completed.length}/${out.ok} completed · avg ${avgE2E}ms` });
      }
    } catch (e) {
      addLog({ tag: 'warn', msg: <>Burst failed: {String(e.message)}</> });
    } finally {
      setBursting(false);
    }
  }, [bursting, addLog, toast]);

  const onBalance = useCallback(async ({ acc }) => {
    const t0 = Date.now();
    try {
      const res = await PL.getBalance(deps, acc);
      const ms = Date.now() - t0;
      const bal = res.data && res.data.balance;
      addLog({
        tag: 'info',
        msg: <>GET /accounts/{acc}/balance <span className="accent">200</span> {ms}ms · bal=<span className="num">{bal}</span></>,
      });
      toast(`Balance ${bal}`, { detail: `${ms}ms` });
    } catch (e) {
      addLog({ tag: 'warn', msg: <>Balance {acc} failed: {String(e.message)}</> });
    }
  }, [addLog, toast]);

  const onListRecent = useCallback(async () => {
    const t0 = Date.now();
    try {
      const res = await PL.listRecent(deps, 10);
      const ms = Date.now() - t0;
      const count = (res.data || []).length;
      addLog({
        tag: 'info',
        msg: <>GET /transactions?limit=10 <span className="accent">200</span> {ms}ms · <span className="num">{count}</span> records</>,
      });
      toast(`Listed ${count} recent`, { detail: `${ms}ms` });
    } catch (e) {
      addLog({ tag: 'warn', msg: <>List failed: {String(e.message)}</> });
    }
  }, [addLog, toast]);

  /* ----- Render ----- */

  const queue = [
    { name: 'p95',     v: `${Math.round(p95)}ms`,  pct: clamp((p95 / 500) * 100, 4, 100),  warn: p95 > 500 },
    { name: 'p99',     v: `${Math.round(p99)}ms`,  pct: clamp((p99 / 1500) * 100, 4, 100), warn: p99 > 1500 },
    { name: 'err/s',   v: errRps.toFixed(2),       pct: clamp(errRps * 100, 4, 100),       warn: errRps > 0.5 },
    { name: 'acc/s',   v: Math.round(acceptedRps), pct: clamp((acceptedRps / 300) * 100, 4, 100),  warn: false },
    { name: 'proc/s',  v: Math.round(processedRps),pct: clamp((processedRps / 300) * 100, 4, 100), warn: false },
  ];

  return (
    <div className="app">
      <header className="header">
        <div className="brand">
          <div className="brand-mark">P</div>
          <div className="brand-text">
            <span className="brand-name">Peakload Capstone</span>
            <span className="brand-sub">ops · cluster prod-1</span>
          </div>
          <div className="divider-v" style={{margin: "0 4px"}}/>
          <Pill tone={healthStatus === 'healthy' ? 'mint' : 'amber'} dot={healthStatus === 'healthy' ? '' : 'amber'}>
            {healthStatus === 'healthy' ? 'cluster healthy' : 'cluster degraded'}
          </Pill>
        </div>
        <div className="header-right">
          <Pill>{acceptedTotal.toLocaleString()} accepted total</Pill>
        </div>
      </header>

      <div className="status-banner">
        <div className="sb-left">
          <div className="sb-icon"><Icon name="check" size={16} stroke={2.4}/></div>
          <div>
            <div className="sb-title">{healthStatus === 'healthy' ? 'All systems nominal' : 'System degraded'}</div>
            <div className="sb-sub">tick {METRICS_TICK_MS}ms · window {RPS_WINDOW}s</div>
          </div>
        </div>
        <div className="sb-stats">
          <div className="sb-stat"><span className="l">accepted /s</span><span className="v">{Math.round(acceptedRps)}</span></div>
          <div className="sb-stat"><span className="l">processed /s</span><span className="v">{Math.round(processedRps)}</span></div>
          <div className="sb-stat"><span className="l">p95</span><span className="v">{Math.round(p95)}ms</span></div>
          <div className="sb-stat"><span className="l">5xx /s</span><span className="v">{errRps.toFixed(2)}</span></div>
        </div>
      </div>

      <div className="kpi-row">
        <KpiCard label="Accepted /s" value={Math.round(acceptedRps)} unit="rps" sparkData={accSpark} sparkColor="var(--mint)" sparkFill="rgba(16,185,129,0.10)"/>
        <KpiCard label="Processed /s" value={Math.round(processedRps)} unit="rps" sparkData={procSpark} sparkColor="var(--teal)" sparkFill="rgba(20,184,166,0.10)"/>
        <KpiCard label="Latency p95" value={Math.round(p95)} unit="ms" sparkData={p95Spark} sparkColor="var(--ink)" sparkFill="rgba(24,24,26,0.06)"/>
        <KpiCard label="Latency p99" value={Math.round(p99)} unit="ms" sparkData={p99Spark} sparkColor="var(--amber)" sparkFill="rgba(240,180,41,0.12)"/>
        <KpiCard label="Errors /s" value={errRps.toFixed(2)} unit="rps" sparkData={errSpark} sparkColor="var(--red)" sparkFill="rgba(194,72,63,0.10)"/>
      </div>

      <div className="main-grid">
        <div className="card">
          <div className="card-head">
            <div className="card-title">
              <Icon name="pulse" size={14} color="var(--mint)"/>
              Live throughput & latency
            </div>
            <span className="card-sub">refresh · {METRICS_TICK_MS/1000}s</span>
          </div>
          <div className="card-body">
            <ChartsPanel rps={rps} lat={lat} queue={queue} tickMs={METRICS_TICK_MS}/>
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <div className="card-title">
              <Icon name="bolt" size={14} color="var(--amber)"/>
              Manual actions
            </div>
            <span className="card-sub">demo</span>
          </div>
          <div className="card-body">
            <ActionsPanel
              onSend={onSend}
              onBurst={onBurst}
              onBalance={onBalance}
              onListRecent={onListRecent}
              sending={sending}
              bursting={bursting}
            />
          </div>
        </div>
      </div>

      <div className="bottom-grid">
        <div className="card">
          <div className="card-head">
            <div className="card-title">
              <Icon name="list" size={14}/>
              Activity stream
            </div>
            <span className="card-sub">newest first · {logEntries.length} events</span>
          </div>
          <div className="card-body">
            <ActivityLog entries={logEntries}/>
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <div className="card-title">
              <Icon name="server" size={14}/>
              Component health
            </div>
            <span className="card-sub">{components.filter(c => c.status === "ok").length}/{components.length} ok</span>
          </div>
          <div className="card-body">
            <HealthList items={components}/>
          </div>
        </div>
      </div>
    </div>
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <ToastProvider>
    <App/>
  </ToastProvider>
);

})();
