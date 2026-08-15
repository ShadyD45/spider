# Compose-stack fleet benchmark (tracker + 3 workers + Prometheus + Grafana).
param(
    [int]$SizeMB = 500,
    [int]$ChunkMB = 4,
    [string]$File = "tmp/origin/payload.bin",
    [switch]$SkipStack
)

$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root

Remove-Item Env:GOOS, Env:GOARCH, Env:CGO_ENABLED, Env:GOFLAGS -ErrorAction SilentlyContinue

$Workers = @("worker-1", "worker-2", "worker-3")
$ChunkBytes = $ChunkMB * 1024 * 1024
$WantBytes = [int64]$SizeMB * 1024 * 1024
$OriginDir = Split-Path $File -Parent
if (-not $OriginDir) { $OriginDir = "." }

if (Get-Command podman-compose -ErrorAction SilentlyContinue) {
    $Compose = @("podman-compose", "-f", "podman-compose.yml")
} elseif (Get-Command docker -ErrorAction SilentlyContinue) {
    $Compose = @("docker", "compose", "-f", "docker-compose.yml")
} else {
    throw "podman-compose or docker compose required"
}

function Invoke-Compose {
    param([string[]]$CmdArgs)
    $all = @($Compose + $CmdArgs)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $all[0] @($all[1..($all.Length - 1)]) 2>&1 | ForEach-Object { Write-Host $_ }
    $ErrorActionPreference = $prev
    if ($LASTEXITCODE -ne 0) { throw "compose failed: $CmdArgs" }
}

function Ensure-Payload {
    New-Item -ItemType Directory -Force -Path $OriginDir, tmp/bench, tmp/work/cache | Out-Null
    go run ./scripts/ensure-payload/main.go $File $WantBytes
}

function Get-PodmanVMIP {
    $line = (wsl -d podman-machine-default -- ip -4 -o addr show eth0 scope global 2>$null | Select-Object -First 1)
    if ($line -match 'inet (\d+\.\d+\.\d+\.\d+)') { return $Matches[1] }
    return $null
}

function Get-StackHost {
    foreach ($h in @("127.0.0.1")) {
        try {
            $null = Invoke-WebRequest -Uri "http://${h}:9091/healthz" -UseBasicParsing -TimeoutSec 2
            return $h
        } catch {}
    }
    $ip = Get-PodmanVMIP
    if ($ip) {
        try {
            $null = Invoke-WebRequest -Uri "http://${ip}:9091/healthz" -UseBasicParsing -TimeoutSec 3
            Write-Host "--- Podman VM IP for stack HTTP: $ip ---"
            return $ip
        } catch {}
    }
    throw "tracker not ready on :9091 (localhost and Podman VM IP unreachable)"
}

function Wait-Stack {
    Write-Host "--- waiting for tracker /healthz ---"
    for ($i = 0; $i -lt 90; $i++) {
        try {
            $script:StackHost = Get-StackHost
            Write-Host "--- tracker ready at $($script:StackHost):9091 ---"
            return
        } catch { Start-Sleep -Seconds 2 }
    }
    throw "tracker not ready on :9091"
}

function Get-PromSum($Query) {
    if (-not $script:StackHost) { $script:StackHost = Get-StackHost }
    try {
        $u = "http://$($script:StackHost):9090/api/v1/query?query=$([uri]::EscapeDataString($Query))"
        $r = Invoke-RestMethod -Uri $u -TimeoutSec 5
        if ($r.data.result.Count -gt 0) { return [double]$r.data.result[0].value[1] }
    } catch {}
    return 0
}

function Reset-Workers {
    Write-Host "--- reset worker caches and bench dest ---"
    foreach ($w in $Workers) {
        Invoke-Compose @("exec", "-T", $w, "sh", "-c", "rm -rf /var/lib/spider/chunks/* /data/bench/dest-baseline /data/bench/dest-mesh 2>/dev/null; mkdir -p /data/bench")
    }
    Write-Host "--- restart workers (refresh tracker chunk index) ---"
    Invoke-Compose @("restart", "worker-1", "worker-2", "worker-3")
    Start-Sleep -Seconds 8
}

function Write-Manifest {
    Write-Host "--- publish manifest -> tmp/bench/manifest.json (worker-1) ---"
    Invoke-Compose @(
        "exec", "-T", "worker-1", "spiderctl", "publish",
        "--source=/bench/origin",
        "--name=bench-model",
        "--version=1.0",
        "--chunk-size=$ChunkBytes",
        "--output=/data/bench/manifest.json",
        "--tracker=central-tracker:50051",
        "--cache-dir=/var/lib/spider",
        "--node-id=worker-1"
    )
}

function Parse-SyncLog($LogPath) {
    $line = Select-String -Path $LogPath -Pattern '^reused=' | Select-Object -Last 1
    if (-not $line) { return @{ Origin = 0; Peer = 0 } }
    $t = $line.Line
    $o = if ($t -match 'origin_bytes=(\d+)') { [int64]$Matches[1] } else { 0 }
    $p = if ($t -match 'peer_bytes=(\d+)') { [int64]$Matches[1] } else { 0 }
    return @{ Origin = $o; Peer = $p }
}

function Invoke-ParallelSync {
    param([string]$Dest, [string]$Origin = "")
    $logs = @()
    $jobs = @()
    $composeExe = $Compose[0]
    $composeBase = @($Compose[1..($Compose.Length - 1)])
    foreach ($w in $Workers) {
        $log = Join-Path $env:TEMP "spider-compose-bench-$w-$(Split-Path $Dest -Leaf).log"
        $logs += $log
        $jobs += Start-Job -ArgumentList $Root, $composeExe, $composeBase, $w, $Dest, $Origin, $log -ScriptBlock {
            param($root, $exe, $baseArgs, $worker, $dest, $origin, $logPath)
            Set-Location $root
            $cmd = [string[]]@($baseArgs + @(
                "exec", "-T", $worker, "spiderctl", "sync",
                "--manifest=/data/bench/manifest.json",
                "--dest=$dest",
                "--daemon=127.0.0.1:50052"
            ))
            if ($origin) { $cmd += "--origin=$origin" }
            & $exe @cmd *> $logPath 2>&1
        }
    }
    Wait-Job $jobs | Out-Null
    $jobs | ForEach-Object { Receive-Job $_ -ErrorAction SilentlyContinue | Out-Null; Remove-Job $_ -Force }
    return $logs
}

function Cleanup-Dest {
    Write-Host "--- cleanup worker materialized dirs ---"
    foreach ($w in $Workers) {
        Invoke-Compose @("exec", "-T", $w, "sh", "-c", "rm -rf /data/bench/dest-baseline /data/bench/dest-mesh")
    }
}

function Sum-Metrics($Logs) {
    $origin = [int64]0
    $peer = [int64]0
    foreach ($l in $Logs) {
        $m = Parse-SyncLog $l
        $origin += $m.Origin
        $peer += $m.Peer
    }
    return @{ Origin = $origin; Peer = $peer }
}

Write-Host "================================================================="
Write-Host "     SPIDER - COMPOSE STACK BENCHMARK ($($SizeMB) MB x 3 workers) "
Write-Host "================================================================="

Ensure-Payload

if (-not $SkipStack) {
    & (Join-Path $PSScriptRoot "build-image.ps1")
    Invoke-Compose @("up", "-d")
}

Wait-Stack
Write-Manifest
Reset-Workers

$originBefore = Get-PromSum "sum(spider_origin_bytes_downloaded_total)"
$peerBefore = Get-PromSum "sum(spider_peer_bytes_transferred_total)"

Write-Host "`n=== Scenario 1: Direct origin (each worker reads /bench/origin) ==="
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$baseLogs = Invoke-ParallelSync -Dest "/data/bench/dest-baseline" -Origin "/bench/origin"
$sw.Stop()
$base = Sum-Metrics $baseLogs
$baseSec = $sw.Elapsed.TotalSeconds
$artifactMB = $SizeMB * $Workers.Count

Reset-Workers

Write-Host "`n=== Scenario 2: P2P mesh (seed on worker-1, fan-out to fleet) ==="
Invoke-Compose @(
    "exec", "-T", "worker-1", "spiderctl", "publish",
    "--source=/bench/origin",
    "--name=bench-model",
    "--version=1.0",
    "--chunk-size=$ChunkBytes",
    "--output=/data/bench/manifest.json",
    "--tracker=central-tracker:50051",
    "--cache-dir=/var/lib/spider",
    "--node-id=worker-1"
)

$sw.Restart()
$meshLogs = Invoke-ParallelSync -Dest "/data/bench/dest-mesh"
$sw.Stop()
$mesh = Sum-Metrics $meshLogs
$meshSec = $sw.Elapsed.TotalSeconds

$originAfter = Get-PromSum "sum(spider_origin_bytes_downloaded_total)"
$peerAfter = Get-PromSum "sum(spider_peer_bytes_transferred_total)"

Cleanup-Dest

$saved = if ($base.Origin -gt 0) { ($base.Origin - $mesh.Origin) / $base.Origin * 100 } else { 0 }
$speed = if ($meshSec -gt 0) { $baseSec / $meshSec } else { 0 }
$baseTp = if ($baseSec -gt 0) { $artifactMB / $baseSec } else { 0 }
$meshTp = if ($meshSec -gt 0) { $artifactMB / $meshSec } else { 0 }

Write-Host ""
Write-Host "METRIC                    DIRECT ORIGIN (BASELINE)   SPIDER P2P MESH   IMPROVEMENT"
Write-Host "------                    ------------------------   ---------------   -----------"
Write-Host ("Duration                  {0:N1}s                      {1:N1}s               {2:N2}x speedup" -f $baseSec, $meshSec, $speed)
Write-Host ("Fleet Throughput          {0:N2} MB/s                  {1:N2} MB/s           -" -f $baseTp, $meshTp)
Write-Host ("Origin Data Transferred   {0:N2} MB                  {1:N2} MB           {2:N1}% bandwidth saved" -f ($base.Origin/1MB), ($mesh.Origin/1MB), $saved)
Write-Host ("Peer Data Transferred     0.00 MB                  {0:N2} MB           -" -f ($mesh.Peer/1MB))

Write-Host ""
Write-Host ("Prometheus totals (delta this run): origin_downloaded={0} peer_transferred={1}" -f ($originAfter - $originBefore), ($peerAfter - $peerBefore))
Write-Host ""
Write-Host "Grafana:    http://localhost:3000/d/spider/spider-mesh  (admin / admin)"
Write-Host "Prometheus: http://localhost:9090"
Write-Host "Stack left running - do not compose down until you have screenshots."
Write-Host "================================================================="
