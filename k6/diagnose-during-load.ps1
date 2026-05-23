# Captures PG connection + lock state every 5s during a k6 run.
# Pair with `k6 run ./k6/load-test-1m.js` in another terminal so
# the snapshots correlate with k6 timestamps.
#
# Output:
#   k6/diagnose-out/diagnose-<shard>-<timestamp>.log
#
# The script writes per-shard files so the parser can tell which
# shard saturated first.

param(
    [int]$IntervalSeconds = 5,
    [string]$OutputDir = "k6\diagnose-out"
)

$ErrorActionPreference = "Continue"

if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$shard0Log = Join-Path $OutputDir "diagnose-shard0-$timestamp.log"
$shard1Log = Join-Path $OutputDir "diagnose-shard1-$timestamp.log"

Write-Host "Probing every $IntervalSeconds s. Output:"
Write-Host "  $shard0Log"
Write-Host "  $shard1Log"
Write-Host "Stop with Ctrl-C."
Write-Host ""

function Probe-Shard {
    param([string]$Container, [string]$Logfile)

    $now = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $Logfile -Value "=== $now ==="

    # Connection-state breakdown. The 117 cap (max_connections=120 minus
    # superuser_reserved=3) is the line we are watching.
    $activity = docker exec $Container psql -U postgres -d peakload_db -tA -c @"
SELECT 'totals:' || ' total=' || count(*) ||
       ' active=' || count(*) FILTER (WHERE state='active') ||
       ' idle=' || count(*) FILTER (WHERE state='idle') ||
       ' idle_tx=' || count(*) FILTER (WHERE state='idle in transaction') ||
       ' clients=' || count(*) FILTER (WHERE backend_type='client backend')
FROM pg_stat_activity;
"@ 2>&1
    Add-Content -Path $Logfile -Value $activity

    # Top wait events. Anything in `Lock` / `LWLock` / `IO` / `BufferPin`
    # is the contention signal we expect to see if the hypothesis holds.
    Add-Content -Path $Logfile -Value "--- top wait events ---"
    $waits = docker exec $Container psql -U postgres -d peakload_db -tA -c @"
SELECT wait_event_type, wait_event, count(*)
FROM pg_stat_activity
WHERE state = 'active' AND wait_event IS NOT NULL
GROUP BY wait_event_type, wait_event
ORDER BY count(*) DESC
LIMIT 5;
"@ 2>&1
    Add-Content -Path $Logfile -Value $waits

    # Blocked locks. NOT granted = a transaction is waiting on another
    # transaction's lock. This is the row-lock contention signal.
    Add-Content -Path $Logfile -Value "--- blocked locks (not granted) ---"
    $locks = docker exec $Container psql -U postgres -d peakload_db -tA -c @"
SELECT pid || '|' || locktype || '|' || COALESCE(relation::regclass::text, '-') ||
       '|' || mode || '|granted=' || granted
FROM pg_locks
WHERE NOT granted
LIMIT 10;
"@ 2>&1
    Add-Content -Path $Logfile -Value $locks

    # Longest-running queries. Anything close to statement_timeout=2s
    # is the about-to-fail candidate.
    Add-Content -Path $Logfile -Value "--- longest active queries ---"
    $longest = docker exec $Container psql -U postgres -d peakload_db -tA -c @"
SELECT extract(epoch FROM (now() - query_start))::int || 's|' ||
       state || '|' || COALESCE(wait_event_type, '-') || '|' ||
       substring(regexp_replace(query, E'[\n\r\t ]+', ' ', 'g') from 1 for 120)
FROM pg_stat_activity
WHERE state = 'active' AND query_start IS NOT NULL
ORDER BY query_start
LIMIT 5;
"@ 2>&1
    Add-Content -Path $Logfile -Value $longest
    Add-Content -Path $Logfile -Value ""
}

while ($true) {
    try {
        Probe-Shard -Container "peakload-pg-shard0-node-a" -Logfile $shard0Log
        Probe-Shard -Container "peakload-pg-shard1-node-a" -Logfile $shard1Log
        Write-Host -NoNewline "."
    } catch {
        Write-Host "probe error: $_"
    }
    Start-Sleep -Seconds $IntervalSeconds
}
