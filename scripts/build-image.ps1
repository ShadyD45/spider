# Build Linux binaries, then assemble the runtime container image (no compile in Containerfile).
param(
    [string]$Image = "localhost/spider:local"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root

. (Join-Path $PSScriptRoot "windows-podman.ps1")
Initialize-SpiderPodmanEnv

& (Join-Path $PSScriptRoot "build-binaries.ps1")

$container = $null
if (Get-Command podman -ErrorAction SilentlyContinue) { $container = "podman" }
elseif (Get-Command docker -ErrorAction SilentlyContinue) { $container = "docker" }
else { throw "podman or docker required" }

Write-Host "--- $container build -t $Image ---"
& $container build -t $Image -f Containerfile .

Write-Host "Image ready: $Image"
Write-Host "Start stack: podman-compose -f podman-compose.yml up -d"
