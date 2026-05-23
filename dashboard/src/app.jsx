/* Peakload dashboard — App: state, polling loops, action handlers.
   Reads kernels from window.PL and components from window globals. */

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
const DASHBOARD_PATHS_RX = /^\/(metrics|health|dashboard)/;

const deps = { fetch: window.fetch.bind(window) };

/* ---------- Pure helpers ---------- */

// Sum http_requests_total{...} entries where the status_code label
// starts with "5". Returns the cumulative counter total (not rate).
function sum5xxFromMetrics(metrics) {
  let total = 0;
  for (const [k, v] of Object.entries(metrics)) {
    if (!k.startsWith('http_requests_total{')) continue;
    const m = k.match(/http_response_status_code="(\d+)"/);
    if (!m) continue;
    if (!m[1].startsWith('5')) continue;
    // Exclude the dashboard's own paths so we don't count ourselves.
    const path = (k.match(/url_path="([^"]*)"/) || [])[1] || '';
    if (DASHBOARD_PATHS_RX.test(path)) continue;
    total += v;
  }
  return total;
}

// Build the bucket map for http_request_duration_seconds (excluding
// the dashboard's own request paths). Returns { "0.005": cum, ..., "+Inf": cum }.
function buildLatencyBuckets(metrics) {
  const byLe = {};
  for (const [k, v] of Object.entries(metrics)) {
    if (!k.startsWith('http_request_duration_seconds_bucket{')) continue;
    const path = (k.match(/url_path="([^"]*)"/) || [])[1] || '';
    if (DASHBOARD_PATHS_RX.test(path)) continue;
    const le = (k.match(/le="([^"]+)"/) || [])[1];
    if (!le) continue;
    byLe[le] = (byLe[le] || 0) + v;
  }
  return byLe;
}

// Map /health JSON → 6 + N rows (one per service + one per replica).
function healthToRows(health) {
  if (!health) {
    return [{ id: 'app', name: 'app', meta: 'unreachable', glyph: 'A', status: 'err' }];
  }
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

  const prevMetricsRef = useRef({ counters: null, t: null });
  const tickRef = useRef(0);
  const prevHealthRef = useRef(null);

  const addLog = useCallback((entry) => {
    setLogEntries(prev => {
      const next = [{
        id: Math.random().toString(36).slice(2),
        t: fmtTime(),
        fresh: true,
        ...entry,
      }, ...prev].slice(0, LOG_MAX);
      return next;
    });
    setTimeout(() => {
      setLogEntries(prev => prev.map(e => ({ ...e, fresh: false })));
    }, 400);
  }, []);

  /* ----- Metrics poll ----- */

  useEffect(() => {
    let cancelled = false;

    async function tick() {
      try {
        const text = await PL.getMetrics(deps);
        if (cancelled) return;
        const metrics = PL.parseProm(text);
        const now = Date.now();

        const created = metrics['transactions_created_total'] || 0;
        const processed = metrics['transactions_processed_total'] || 0;
        const errors5xx = sum5xxFromMetrics(metrics);
        const buckets = buildLatencyBuckets(metrics);
        const p95v = PL.histogramQuantile(buckets, 0.95) * 1000; // → ms
        const p99v = PL.histogramQuantile(buckets, 0.99) * 1000;

        const prev = prevMetricsRef.current;
        if (prev.counters !== null) {
          const dtSec = (now - prev.t) / 1000;
          const aRps = PL.rateOf(prev.counters.created,   created,   dtSec);
          const pRps = PL.rateOf(prev.counters.processed, processed, dtSec);
          const eRps = PL.rateOf(prev.counters.errors5xx, errors5xx, dtSec);

          setAcceptedRps(aRps);
          setProcessedRps(pRps);
          setErrRps(eRps);
          setP95(p95v);
          setP99(p99v);
          setAcceptedTotal(created);

          setRps(arr => PL.rollingPush(arr, Math.round(aRps), RPS_WINDOW));
          setLat(arr => PL.rollingPush(arr, Math.round(p95v), LAT_WINDOW));
          setAccSpark(arr => PL.rollingPush(arr, Math.round(aRps), SPARK_WINDOW));
          setProcSpark(arr => PL.rollingPush(arr, Math.round(pRps), SPARK_WINDOW));
          setP95Spark(arr => PL.rollingPush(arr, Math.round(p95v), SPARK_WINDOW));
          setP99Spark(arr => PL.rollingPush(arr, Math.round(p99v), SPARK_WINDOW));
          setErrSpark(arr => PL.rollingPush(arr, Math.round(eRps * 100) / 100, SPARK_WINDOW));

          tickRef.current++;
          if (tickRef.current % SUMMARY_EVERY_N_TICKS === 0) {
            addLog({
              tag: 'info',
              msg: <>k6 sustained <span className="num">{Math.round(aRps)} rps</span> · p95 <span className="num">{Math.round(p95v)} ms</span> · {Math.round(eRps)} 5xx/s</>,
            });
          }
        }

        prevMetricsRef.current = { counters: { created, processed, errors5xx }, t: now };
      } catch (e) {
        if (tickRef.current % 10 === 0) {
          addLog({ tag: 'warn', msg: <>/metrics fetch failed: {String(e.message)}</> });
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
        setComponents(healthToRows(null));
        setHealthStatus('degraded');
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
    const t0 = Date.now();
    try {
      const res = await PL.sendTxn(deps, {
        from_account: from, to_account: to, amount: amt,
        currency: 'IDR', reference_id: refId, description: 'dashboard manual',
      });
      const acceptMs = Date.now() - t0;
      addLog({
        tag: 'ok',
        msg: <>POST /transactions <span className="accent">{res.data.status === 'pending' ? '202' : '200'}</span> {acceptMs}ms · ref=<span className="mono">{refId}</span></>,
      });
      const tPoll0 = Date.now();
      try {
        const final = await PL.pollStatus(deps, refId, {
          intervalMs: STATUS_POLL_INTERVAL_MS, timeoutMs: STATUS_POLL_TIMEOUT_MS,
        });
        const totalMs = Date.now() - tPoll0;
        addLog({
          tag: final.status === 'completed' ? 'ok' : 'warn',
          msg: <>ref=<span className="mono">{refId}</span> → <span className="accent">{final.status}</span> in <span className="num">{totalMs}ms</span></>,
        });
        toast(`Transaction ${final.status}`, { detail: `ref=${refId.slice(0,12)}… · ${totalMs}ms` });
      } catch (timeout) {
        addLog({ tag: 'warn', msg: <>ref=<span className="mono">{refId}</span> status-poll timeout</> });
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
    try {
      const out = await PL.runBurst({
        n,
        concurrency: BURST_CONCURRENCY,
        fetcher: (payload) => PL.sendTxn(deps, payload),
        payloadGen: (i) => ({
          from_account: 'ACC_0000001',
          to_account: 'ACC_0000042',
          amount: '1.00',
          currency: 'IDR',
          reference_id: PL.genRefId(`burst-${i}`),
          description: 'dashboard burst',
        }),
      });
      addLog({
        tag: out.failed === 0 ? 'ok' : 'warn',
        msg: <>BURST × {out.ok + out.failed} · {out.failed === 0 ? <span className="accent">100% ok</span> : <span className="accent">{out.ok} ok / {out.failed} failed</span>} · max <span className="num">{out.maxLatencyMs}ms</span> · total <span className="num">{out.totalMs}ms</span></>,
      });
      toast(`Burst ${out.ok}/${out.ok + out.failed} ok`, { detail: `max ${out.maxLatencyMs}ms` });
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
