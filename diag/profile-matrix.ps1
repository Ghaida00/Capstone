# Profile matrix runner — fast regression gate for A/B/C/D (Windows).
#
# Usage:
#   .\diag\profile-matrix.ps1                      # gate all profiles (~45-70 min)
#   .\diag\profile-matrix.ps1 -Profiles B        # one profile, post-reboot check
#   .\diag\profile-matrix.ps1 -Profiles B -Tier full   # 15m cert run for demo profile
#   .\diag\profile-matrix.ps1 -Profiles A,B -SkipColdBoot  # swap .env only (same topology)
#
# Tiers:
#   smoke  — health + setup-probe only (~3 min/profile)
#   gate   — K6_GATE=1 load-test-1m-sample-e2e (~8 min/profile incl. bring-up)
#   full   — full 15m load-test-1m-sample-e2e (certification before pameran)
#
# Outputs: diag/matrix-<stamp>/profile-<X>-<tier>.log + summary.csv

param(
    [string[]]$Profiles = @("A", "B", "C", "D"),
    [ValidateSet("smoke", "gate", "full")]
    [string]$Tier = "gate",
    [switch]$SkipColdBoot,
    [string]$DemoProfile = "B",
    [int]$HealthWaitSec = 300
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $RepoRoot

$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$OutDir = Join-Path $RepoRoot "diag\matrix-$Stamp"
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

$ProfileEnv = @{
    A = ".env.profile-a.example"
    B = ".env.profile-b.example"
    C = ".env.profile-c.example"
    D = ".env.profile-d.example"
}

# Shard topology — only need `down -v` when crossing groups (unless -SkipColdBoot).
$TopologyGroup = @{
    A = "2shard"
    B = "1shard"
    C = "2shard"
    D = "3shard"
}

function Write-Log($Profile, $Tier, $Message) {
    $line = "[$(Get-Date -Format 'HH:mm:ss')] $Message"
    Write-Host $line
    Add-Content -Path (Join-Path $OutDir "profile-$Profile-$Tier.log") -Value $line
}

function Test-Port8080 {
    $ranges = netsh interface ipv4 show excludedportrange protocol=tcp 2>$null
    if ($ranges -match "8061\s+8160") {
        Write-Warning "Windows may block port 8080 (excluded range 8061-8160). Use 8090 or restart Docker/WINnat."
    }
}

function Wait-Healthy {
    param([string]$Profile, [string]$Tier, [int]$MaxSec)
    $deadline = (Get-Date).AddSeconds($MaxSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $code = curl.exe -s -o NUL -w "%{http_code}" http://localhost:8080/health
            if ($code -eq "200") {
                Write-Log $Profile $Tier "health OK"
                return $true
            }
        } catch { }
        Start-Sleep -Seconds 5
    }
    Write-Log $Profile $Tier "health TIMEOUT after ${MaxSec}s"
    cmd /c "docker compose ps 2>&1" | Out-File (Join-Path $OutDir "profile-$Profile-compose-ps.txt")
    return $false
}

function Capture-NginxUpstreamErrors {
    param([string]$Profile, [string]$Tier)
    $path = Join-Path $OutDir "profile-$Profile-nginx-upstream.txt"
    cmd /c "docker logs peakload-nginx 2>&1" |
        Select-String "no live upstreams|upstream timed out|connect\(\) failed" |
        Out-File $path
    $count = (Get-Content $path -ErrorAction SilentlyContinue | Measure-Object).Count
    Write-Log $Profile $Tier "nginx upstream errors logged: $count lines -> $path"
    return $count
}

function Capture-PrometheusSnapshot {
    param([string]$Profile, [string]$Tier)
    $path = Join-Path $OutDir "profile-$Profile-prometheus.txt"
    @(
        "=== http status (60m) ==="
        curl.exe -s 'http://localhost:9090/api/v1/query?query=sum%20by%20(http_response_status_code)%20(increase(http_requests_total%5B60m%5D))'
        ""
        "=== intake pending max ==="
        curl.exe -s 'http://localhost:9090/api/v1/query?query=max%20(transactions_intake_pending)'
        ""
        "=== e2e timeout counter ==="
        curl.exe -s 'http://localhost:9090/api/v1/query?query=sum%20(increase(transactions_e2e_timeout%5B60m%5D))'
    ) | Out-File $path
}

function Invoke-K6 {
    param([string]$Profile, [string]$Tier)
    $log = Join-Path $OutDir "profile-$Profile-k6-$Tier.log"
    $env:K6_ENV = "profile-$Profile"
    $env:RUN_ID = "$Stamp-$Profile"

    if ($Tier -eq "smoke") {
        $cmd = "k6 run --quiet k6/setup-probe.js"
    } elseif ($Tier -eq "gate") {
        $cmd = "k6 run --quiet -e K6_GATE=1 k6/load-test-1m-sample-e2e.js"
    } else {
        $cmd = "k6 run --quiet k6/load-test-1m-sample-e2e.js"
    }

    Write-Log $Profile $Tier "running: $cmd"
    # Tee-Object passes the k6 output through the pipeline; without the
    # trailing Out-Null it becomes part of the function's return value
    # and corrupts summary.csv. The log file still gets the full output.
    cmd /c "$cmd 2>&1" | Tee-Object -FilePath $log | Out-Null
    return $LASTEXITCODE
}

# ─── Main ───────────────────────────────────────────────────
Test-Port8080

$summaryRows = @("profile,tier,cold_boot,health,k6_exit,nginx_upstream_errors,notes")
$prevTopology = $null

foreach ($p in $Profiles) {
    $p = $p.ToUpper()
    if (-not $ProfileEnv.ContainsKey($p)) {
        Write-Warning "Unknown profile $p — skip"
        continue
    }

    $envFile = $ProfileEnv[$p]
    $topology = $TopologyGroup[$p]
    $needColdBoot = -not $SkipColdBoot -and ($prevTopology -ne $null) -and ($topology -ne $prevTopology)

    Write-Log $p $Tier "=== Profile $p ($envFile, topology=$topology) ==="

    # Down runs BEFORE the .env swap: compose only sees services in the
    # active COMPOSE_PROFILES, so the teardown must use the .env that
    # describes the stack that is actually running. Copying the new
    # profile first leaves the old profile's extra-shard containers and
    # volumes behind as orphans (e.g. A->B kept shard1 up through the
    # whole B run).
    if ($needColdBoot -or (-not $SkipColdBoot -and $null -eq $prevTopology)) {
        Write-Log $p $Tier "docker compose down -v --remove-orphans (topology/bootstrap, old .env)"
        cmd /c "docker compose down -v --remove-orphans 2>&1" | Out-Null
    } else {
        Write-Log $p $Tier "docker compose down --remove-orphans (keep volumes, old .env)"
        cmd /c "docker compose down --remove-orphans 2>&1" | Out-Null
    }

    Copy-Item $envFile ".env" -Force
    Write-Log $p $Tier "copied $envFile -> .env"

    Write-Log $p $Tier "docker compose up -d --wait"
    cmd /c "docker compose up -d --wait 2>&1" | Out-File (Join-Path $OutDir "profile-$p-compose-up.log")

    $healthOk = Wait-Healthy -Profile $p -Tier $Tier -MaxSec $HealthWaitSec
    if (-not $healthOk) {
        $summaryRows += "$p,$Tier,$(-not $SkipColdBoot),FAIL,SKIP,,-"
        $prevTopology = $topology
        continue
    }

    $k6Exit = 0
    if ($Tier -ne "smoke") {
        $k6Exit = Invoke-K6 -Profile $p -Tier $Tier
    } else {
        $k6Exit = Invoke-K6 -Profile $p -Tier "smoke"
    }

    $nginxErrs = Capture-NginxUpstreamErrors -Profile $p -Tier $Tier
    Capture-PrometheusSnapshot -Profile $p -Tier $Tier

    $notes = if ($k6Exit -eq 0) { "PASS" } else { "K6_FAIL" }
    $summaryRows += "$p,$Tier,$(-not $SkipColdBoot),OK,$k6Exit,$nginxErrs,$notes"
    $prevTopology = $topology
}

$summaryPath = Join-Path $OutDir "summary.csv"
$summaryRows | Set-Content $summaryPath -Encoding UTF8

Write-Host ""
Write-Host "Done. Results: $OutDir"
Write-Host "Summary:     $summaryPath"
Get-Content $summaryPath
