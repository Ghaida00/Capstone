# Replica-focused probe. Captures the data we couldn't see with the
# original primary-only probe:
#   * primary's view of the replica via pg_stat_replication
#       (write_lag / flush_lag / replay_lag, LSN gap)
#   * replica's view of itself via pg_stat_wal_receiver
#       (last-message age, receive vs replay LSN)
#   * container CPU + memory for each PG node and app instance
#       (catches the case where the replica is CPU-pinned, which
#        is invisible from PG counters)
#   * app-side read pool health-flag state by scraping /metrics
#
# Pair with `k6 run ./k6/load-test-1m.js` in another terminal.
# The output goes to k6/diagnose-out/replication-<ts>.log; cross-
# reference timestamps with k6's WARN lines and the slow-statement
# log entries in PG to align cause and symptom.

param(
    [int]$IntervalSeconds = 5,
    [string]$OutputDir = "k6\diagnose-out"
)

$ErrorActionPreference = "Continue"

if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logFile = Join-Path $OutputDir "replication-$timestamp.log"

Write-Host "Replica-side probe every $IntervalSeconds s. Output:"
Write-Host "  $logFile"
Write-Host "Stop with Ctrl-C."
Write-Host ""

# Each shard has two nodes (a/b); roles flip on failover so query
# both and let pg_is_in_recovery() report which is which. Patroni
# keeps role state in etcd, but a direct PG query is enough for
# diagnostic snapshots.
$Shards = @(
    @{ shard = 0; nodes = @("peakload-pg-shard0-node-a", "peakload-pg-shard0-node-b") },
    @{ shard = 1; nodes = @("peakload-pg-shard1-node-a", "peakload-pg-shard1-node-b") }
)

function Get-NodeRole {
    param([string]$Container)

    $r = docker exec $Container psql -U postgres -d peakload_db -tA -c "SELECT pg_is_in_recovery();" 2>&1
    if ($LASTEXITCODE -ne 0) { return "unknown" }
    if ($r.Trim() -eq "f") { return "primary" }
    if ($r.Trim() -eq "t") { return "replica" }
    return "unknown"
}

function Probe-Primary {
    param([string]$Container, [int]$Shard)

    # pg_stat_replication: how the primary sees the replica.
    # write_lag/flush_lag/replay_lag are the wall-clock time the
    # replica is behind on each stage. bytes_behind quantifies it
    # in WAL bytes (useful when lag is small but cumulative).
    Add-Content -Path $logFile -Value "    --- pg_stat_replication (primary view) ---"
    $rep = docker exec $Container psql -U postgres -d peakload_db -tA -c @"
SELECT application_name,
       state,
       sync_state,
       COALESCE(EXTRACT(epoch FROM write_lag)::numeric(10,3)::text, '-')  AS write_lag_s,
       COALESCE(EXTRACT(epoch FROM flush_lag)::numeric(10,3)::text, '-')  AS flush_lag_s,
       COALESCE(EXTRACT(epoch FROM replay_lag)::numeric(10,3)::text, '-') AS replay_lag_s,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS bytes_behind
FROM pg_stat_replication;
"@ 2>&1
    Add-Content -Path $logFile -Value $rep

    # WAL generation rate snapshot — pair successive snapshots to
    # compute MB/s of WAL the primary is producing. A surge here
    # while replica is lagging tells us WAL volume outpaced
    # replica's receive bandwidth.
    Add-Content -Path $logFile -Value "    --- WAL generation ---"
    $wal = docker exec $Container psql -U postgres -d peakload_db -tA -c @"
SELECT 'current_lsn:' || pg_current_wal_lsn() || ' bytes_since_reset:' ||
       pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::text;
"@ 2>&1
    Add-Content -Path $logFile -Value $wal
}

function Probe-Replica {
    param([string]$Container, [int]$Shard)

    # pg_stat_wal_receiver: replica's view of the WAL stream.
    # msg_age_s tells us when the last heartbeat from primary
    # arrived — a value climbing past `wal_receiver_timeout`
    # explains the "Replica marked unhealthy" event we saw at
    # 19:34:03.
    Add-Content -Path $logFile -Value "    --- pg_stat_wal_receiver (replica view) ---"
    $recv = docker exec $Container psql -U postgres -d peakload_db -tA -c @"
SELECT 'status:' || status ||
       ' last_msg_send_time:' || COALESCE(last_msg_send_time::text, '-') ||
       ' last_msg_receipt_time:' || COALESCE(last_msg_receipt_time::text, '-') ||
       ' msg_age_s:' || COALESCE(EXTRACT(epoch FROM (now() - last_msg_receipt_time))::numeric(10,2)::text, '-') ||
       ' replay_behind_bytes:' || COALESCE(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::text, '-')
FROM pg_stat_wal_receiver;
"@ 2>&1
    Add-Content -Path $logFile -Value $recv

    # Replica's own activity. Heavy autovacuum on the replica side
    # (yes, replica also runs autovacuum) or long-running snapshot
    # queries can starve the WAL applier of CPU.
    Add-Content -Path $logFile -Value "    --- replica longest active queries ---"
    $act = docker exec $Container psql -U postgres -d peakload_db -tA -c @"
SELECT EXTRACT(epoch FROM (now() - query_start))::int || 's|' ||
       state || '|' || COALESCE(wait_event_type, '-') || '|' ||
       substring(regexp_replace(query, E'[\n\r\t ]+', ' ', 'g') from 1 for 100)
FROM pg_stat_activity
WHERE state = 'active' AND query_start IS NOT NULL
ORDER BY query_start
LIMIT 5;
"@ 2>&1
    Add-Content -Path $logFile -Value $act
}

function Probe-Containers {
    # docker stats is a one-shot reading; --no-stream stops it from
    # streaming. We filter for PG + app + pgbouncer containers
    # because the rest (etcd, prometheus, grafana) aren't on the
    # critical path of write throughput.
    Add-Content -Path $logFile -Value "  --- container resources ---"
    $stats = docker stats --no-stream --format "{{.Name}}|cpu={{.CPUPerc}}|mem={{.MemPerc}}|mem_usage={{.MemUsage}}" `
        peakload-pg-shard0-node-a peakload-pg-shard0-node-b `
        peakload-pg-shard1-node-a peakload-pg-shard1-node-b `
        peakload-app-1 peakload-app-2 `
        peakload-pgbouncer-shard0 peakload-pgbouncer-shard1 2>&1
    Add-Content -Path $logFile -Value $stats
}

function Probe-AppHealth {
    # The "Replica marked unhealthy" event is logged but not
    # surfaced as a counter on /metrics. We can however check the
    # current read-pool health gauges if exposed; if not, this
    # block is a no-op and the app log itself is the source of
    # truth (grep for "marked unhealthy" / "recovered").
    Add-Content -Path $logFile -Value "  --- app read-pool health metrics ---"
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing -TimeoutSec 2
        $health = ($resp.Content -split "`n") | Where-Object { $_ -match "^db_(replica|read_pool)" -or $_ -match "^pg_replication" } | Select-Object -First 10
        if ($health) {
            Add-Content -Path $logFile -Value $health
        } else {
            Add-Content -Path $logFile -Value "(no db_replica_* / db_read_pool_* metrics exposed)"
        }
    } catch {
        Add-Content -Path $logFile -Value "(metrics endpoint unreachable: $($_.Exception.Message))"
    }
}

while ($true) {
    $now = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logFile -Value "=== $now ==="

    foreach ($s in $Shards) {
        Add-Content -Path $logFile -Value "[shard $($s.shard)]"
        foreach ($node in $s.nodes) {
            $role = Get-NodeRole -Container $node
            Add-Content -Path $logFile -Value "  $node ($role)"
            if ($role -eq "primary") {
                Probe-Primary -Container $node -Shard $s.shard
            } elseif ($role -eq "replica") {
                Probe-Replica -Container $node -Shard $s.shard
            } else {
                Add-Content -Path $logFile -Value "    (role probe failed)"
            }
        }
    }

    Probe-Containers
    Probe-AppHealth

    Add-Content -Path $logFile -Value ""
    Write-Host -NoNewline "."

    Start-Sleep -Seconds $IntervalSeconds
}
