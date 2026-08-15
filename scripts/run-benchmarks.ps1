param(
  [string]$File = "tmp/origin/payload.bin"
)

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "        SPIDER ARTIFACT MESH — BENCHMARK SUITE RUNNER           " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

Remove-Item Env:GOOS, Env:GOARCH, Env:CGO_ENABLED, Env:GOFLAGS -ErrorAction SilentlyContinue

Write-Host "`n--- 1. Go microbenchmarks ---" -ForegroundColor Yellow
go test -count=1 -bench="." -benchmem ./pkg/chunk ./pkg/cache

Write-Host "`n--- 2. Compose fleet benchmark (500 MB x 3 workers, feeds Grafana) ---" -ForegroundColor Yellow
& (Join-Path $PSScriptRoot "run-compose-benchmark.ps1") -File $File

Write-Host "`n--- Optional: in-process loopback (fast, no Grafana) ---" -ForegroundColor Yellow
Write-Host "  .\bin\spiderctl.exe benchmark --file=$File --size=500 --workers=6 --chunk-size=4"

Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host "                 BENCHMARK SUITE COMPLETED                      " -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
