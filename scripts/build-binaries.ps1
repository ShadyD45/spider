# Cross-compile Linux static binaries into dist/linux/ for the container image.
$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent
$Out = Join-Path $Root "dist\linux"
New-Item -ItemType Directory -Force -Path $Out | Out-Null

$env:CGO_ENABLED = "0"
$env:GOOS = "linux"
if (-not $env:GOARCH) { $env:GOARCH = "amd64" }
$env:GOFLAGS = "-trimpath"

Set-Location $Root

function Build-One($Name, $Pkg) {
    Write-Host "  $Name <- $Pkg"
    go build -ldflags="-s -w" -o (Join-Path $Out $Name) $Pkg
}

Write-Host "--- Go build (linux/$env:GOARCH) -> dist/linux ---"
Build-One tracker ./cmd/tracker
Build-One spiderd ./cmd/spiderd
Build-One spiderctl ./cmd/spiderctl

Write-Host "Done: $Out"

Remove-Item Env:GOOS -ErrorAction SilentlyContinue
Remove-Item Env:GOARCH -ErrorAction SilentlyContinue
Remove-Item Env:CGO_ENABLED -ErrorAction SilentlyContinue
Remove-Item Env:GOFLAGS -ErrorAction SilentlyContinue
