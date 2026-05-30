/* Peakload dashboard — UI primitives + cards.
   Babel-transpiled in-browser. Attaches components to window so
   app.jsx (loaded next) can use them without ES module imports. */

const { useState, useEffect, useRef, useMemo, useCallback } = React;

/* ===== Icons (verbatim from reference) ===== */

function Icon({ name, size = 14, stroke = 1.6, color = "currentColor" }) {
  const paths = {
    bolt:   <path d="M13 2L4 14h6l-1 8 9-12h-6l1-8z"/>,
    send:   <path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z"/>,
    search: <><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></>,
    list:   <><path d="M8 6h13M8 12h13M8 18h13"/><circle cx="3.5" cy="6" r="1"/><circle cx="3.5" cy="12" r="1"/><circle cx="3.5" cy="18" r="1"/></>,
    flame:  <path d="M12 2c2 4 5 6 5 11a5 5 0 1 1-10 0c0-2 1-3 2-4-.5 3 1 4 2 4 0-3-1-6 1-11z"/>,
    check:  <path d="M5 13l4 4L19 7"/>,
    arrowU: <path d="M12 19V5M5 12l7-7 7 7"/>,
    arrowD: <path d="M12 5v14M5 12l7 7 7-7"/>,
    pulse:  <path d="M3 12h4l3-8 4 16 3-8h4"/>,
    server: <><rect x="3" y="4" width="18" height="6" rx="1.5"/><rect x="3" y="14" width="18" height="6" rx="1.5"/><circle cx="7" cy="7" r="0.6" fill="currentColor"/><circle cx="7" cy="17" r="0.6" fill="currentColor"/></>,
    bell:   <><path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9"/><path d="M10 21a2 2 0 0 0 4 0"/></>,
    settings: <><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3h0a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5h0a1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8v0a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"/></>,
  };
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none"
      stroke={color} strokeWidth={stroke} strokeLinecap="round" strokeLinejoin="round">
      {paths[name]}
    </svg>
  );
}

/* ===== Sparkline ===== */

function Sparkline({ data, color = "var(--mint)", fill = "rgba(16, 185, 129, 0.10)", height = 28 }) {
  if (!data || data.length === 0) return null;
  const w = 120, h = height;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = Math.max(1, max - min);
  const stepX = w / (data.length - 1 || 1);
  const pts = data.map((v, i) => {
    const x = i * stepX;
    const y = h - 2 - ((v - min) / range) * (h - 4);
    return [x, y];
  });
  const linePath = pts.map(([x, y], i) => (i === 0 ? `M${x},${y}` : `L${x},${y}`)).join(" ");
  const fillPath = `${linePath} L${w},${h} L0,${h} Z`;
  const last = pts[pts.length - 1];
  return (
    <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" style={{ width: "100%", height: h, display: "block" }}>
      <path d={fillPath} fill={fill} />
      <path d={linePath} stroke={color} strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
      <circle cx={last[0]} cy={last[1]} r="2.2" fill={color}/>
    </svg>
  );
}

/* ===== Bar histogram ===== */

function BarHist({ buckets, color = "mint", max, latestIndex }) {
  const bestMax = max ?? Math.max(...buckets, 1);
  return (
    <div className="bars">
      {buckets.map((v, i) => {
        const pct = Math.min(100, (v / bestMax) * 100);
        return (
          <div
            key={i}
            className={`bar ${color} ${i === latestIndex ? "latest" : ""}`}
            style={{ height: `${Math.max(2, pct)}%` }}
            title={String(v)}
          />
        );
      })}
    </div>
  );
}

/* ===== Pill ===== */

function Pill({ children, tone = "default", dot }) {
  const cls = "pill" + (tone !== "default" ? " " + tone : "");
  return (
    <span className={cls}>
      {dot && <span className={`pill-dot ${dot}`} />}
      {children}
    </span>
  );
}

/* ===== Toasts ===== */

const ToastContext = React.createContext(null);

function ToastProvider({ children }) {
  const [toasts, setToasts] = useState([]);
  const push = useCallback((msg, opts = {}) => {
    const id = Math.random().toString(36).slice(2);
    setToasts(t => [...t, { id, msg, ...opts }]);
    setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), opts.duration || 2600);
  }, []);
  return (
    <ToastContext.Provider value={push}>
      {children}
      <div className="toast-stack">
        {toasts.map(t => (
          <div key={t.id} className="toast">
            <span className="t-icon"><Icon name="check" size={11} stroke={2.4}/></span>
            <span>{t.msg}</span>
            {t.detail && <span className="t-mono">{t.detail}</span>}
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

const useToast = () => React.useContext(ToastContext);

/* ===== Utility helpers ===== */

function fmtTime(d = new Date()) {
  return d.toTimeString().slice(0, 8);
}
function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }
function pad(n, w = 2) { return String(n).padStart(w, "0"); }

/* ===== KPI card ===== */

function KpiCard({ label, value, unit, delta, deltaTone, sparkData, sparkColor, sparkFill }) {
  return (
    <div className="kpi">
      <div className="kpi-head">
        <span className="kpi-label">{label}</span>
        {delta != null && (
          <span className={`kpi-delta ${deltaTone || "flat"}`}>
            {deltaTone === "up" && <Icon name="arrowU" size={9} stroke={2.6}/>}
            {deltaTone === "down" && <Icon name="arrowD" size={9} stroke={2.6}/>}
            {delta}
          </span>
        )}
      </div>
      <div className="kpi-value">
        {value}<span className="kpi-unit">{unit}</span>
      </div>
      <div className="kpi-spark">
        <Sparkline data={sparkData} color={sparkColor} fill={sparkFill}/>
      </div>
    </div>
  );
}

/* ===== Actions panel ===== */

function ActionsPanel({ onSend, onBurst, onBalance, onListRecent, sending, bursting }) {
  const [from, setFrom]     = useState("ACC_0000001");
  const [to, setTo]         = useState("ACC_0000042");
  const [amt, setAmt]       = useState("125.00");
  const [burstN, setBurstN] = useState("20");
  const [balAcc, setBalAcc] = useState("ACC_0000001");

  // B4 fix: wrap each section in a <form> so Enter submits the
  // primary action. Button defaults to type="submit" inside a form;
  // secondary buttons declare type="button" so they don't accidentally
  // submit.
  const submitSend = (e) => { e.preventDefault(); if (!sending) onSend({ from, to, amt }); };
  const submitBurst = (e) => { e.preventDefault(); if (!bursting) onBurst({ n: parseInt(burstN, 10) || 0 }); };
  const submitBalance = (e) => { e.preventDefault(); onBalance({ acc: balAcc }); };

  return (
    <div className="actions">
      <form className="actions-section" onSubmit={submitSend}>
        <div className="actions-section-title">Send transaction</div>
        <div className="field-row">
          <div className="field">
            <span className="field-label">from</span>
            <input className="input" value={from} onChange={e => setFrom(e.target.value)} />
          </div>
          <div className="field">
            <span className="field-label">to</span>
            <input className="input" value={to} onChange={e => setTo(e.target.value)} />
          </div>
        </div>
        <div className="field">
          <span className="field-label">amount</span>
          <input className="input" value={amt} onChange={e => setAmt(e.target.value)} />
        </div>
        <button type="submit" className="btn btn-mint" disabled={sending}>
          <Icon name="send" size={13}/> {sending ? "Sending…" : "Send transaction"}
        </button>
      </form>

      <form className="actions-section" onSubmit={submitBurst}>
        <div className="actions-section-title">Burst</div>
        <div className="field">
          <span className="field-label">count (1–100)</span>
          {/* B5 fix: numeric input with hard min/max so the browser
              enforces the same range runBurst's HARD_CAP enforces. */}
          <input className="input" type="number" min="1" max="100" step="1"
                 value={burstN} onChange={e => setBurstN(e.target.value)} />
        </div>
        <button type="submit" className="btn btn-amber" disabled={bursting}>
          <Icon name="flame" size={13}/> {bursting ? "Bursting…" : `Fire burst (×${burstN || 0})`}
        </button>
      </form>

      <form className="actions-section" onSubmit={submitBalance}>
        <div className="actions-section-title">Read</div>
        <div className="field">
          <span className="field-label">account</span>
          <input className="input" value={balAcc} onChange={e => setBalAcc(e.target.value)} />
        </div>
        <div className="btn-row">
          <button type="submit" className="btn btn-ghost">
            <Icon name="search" size={13}/> Balance
          </button>
          <button type="button" className="btn btn-ghost" onClick={() => onListRecent()}>
            <Icon name="list" size={13}/> List recent
          </button>
        </div>
      </form>
    </div>
  );
}

/* ===== Charts panel ===== */

function ChartsPanel({ rps, lat, queue, tickMs }) {
  const latestRpsIdx = rps.length - 1;
  const latestLatIdx = lat.length - 1;
  const rpsNow = rps[latestRpsIdx] || 0;
  const latP95 = lat[latestLatIdx] || 0;

  return (
    <div className="charts-wrap">
      <div className="chart-block">
        <div className="chart-meta">
          <div className="chart-meta-left">
            <span className="chart-name">Throughput · /s</span>
            <span className="chart-big">{Math.round(rpsNow)}<span className="unit">rps</span></span>
          </div>
          <div className="chart-legend">
            <span className="legend-item"><span className="legend-swatch" style={{background:"var(--mint)"}}/>accepted</span>
            <span className="muted mono">window {rps.length}s</span>
          </div>
        </div>
        <BarHist buckets={rps} color="mint" latestIndex={latestRpsIdx}/>
        <div className="chart-axis">
          <span>-{rps.length}s</span><span>now</span>
        </div>
      </div>

      <div className="chart-block">
        <div className="chart-meta">
          <div className="chart-meta-left">
            <span className="chart-name">Latency p95 · ms</span>
            <span className="chart-big">{Math.round(latP95)}<span className="unit">ms</span></span>
          </div>
          <div className="chart-legend">
            <span className="legend-item"><span className="legend-swatch" style={{background:"var(--teal)"}}/>p95</span>
            <span className="muted mono">window {lat.length}s</span>
          </div>
        </div>
        <BarHist buckets={lat} color="teal" latestIndex={latestLatIdx}/>
        <div className="chart-axis">
          <span>-{lat.length}s</span><span>now</span>
        </div>
      </div>

      {queue && queue.length > 0 && (
        <div>
          <div className="chart-meta" style={{marginBottom: 8}}>
            <div className="chart-meta-left">
              <span className="chart-name">Activity gauges</span>
            </div>
            <span className="muted mono" style={{fontSize: 11}}>updated {Math.round(tickMs/1000)}s ago</span>
          </div>
          <div className="queue-grid">
            {queue.map(q => (
              <div key={q.name} className={`queue-cell ${q.warn ? "warn" : ""}`}>
                <div className="qname">{q.name}</div>
                <div className="qval">{q.v}</div>
                <div className="qbar" style={{ width: `${clamp(q.pct, 4, 100)}%` }} />
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

/* ===== Activity log ===== */

function ActivityLog({ entries }) {
  return (
    <div className="log">
      {entries.map((e, i) => (
        <div key={e.id || i} className={`log-row ${e.fresh ? "fresh" : ""}`}>
          <span className="log-time">{e.t}</span>
          <span className="log-icon">
            {e.tag === "ok" && <Icon name="check" size={12} stroke={2.4} color="var(--mint)"/>}
            {e.tag === "info" && <Icon name="pulse" size={12} stroke={2} color="var(--indigo)"/>}
            {e.tag === "warn" && <Icon name="bolt" size={12} stroke={2} color="var(--amber)"/>}
          </span>
          <span className="log-msg">{e.msg}</span>
          <span className={`log-tag ${e.tag}`}>{e.tag}</span>
        </div>
      ))}
    </div>
  );
}

/* ===== Health list ===== */

function HealthList({ items }) {
  return (
    <div className="health-list">
      {items.map(it => (
        <div key={it.id} className="health-row">
          <div className="health-glyph">{it.glyph}</div>
          <div className="health-name">
            <span className="n">{it.name}</span>
            <span className="meta">{it.meta}</span>
          </div>
          <div className="health-status">
            <span className={`status-dot ${it.status}`}/>
            <span>{it.status === "ok" ? "ok" : it.status === "warn" ? "warn" : "err"}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

/* ===== Export to window ===== */

Object.assign(window, {
  Icon, Sparkline, BarHist, Pill, ToastProvider, useToast,
  fmtTime, clamp, pad,
  KpiCard, ActionsPanel, ChartsPanel, ActivityLog, HealthList,
});
