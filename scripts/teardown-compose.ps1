# Stop the compose stack, remove volumes (keep images), optionally stop the Podman VM.
param(
    [switch]$KeepMachine
)

$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root

. (Join-Path $PSScriptRoot "windows-podman.ps1")

$stopMachine = -not $KeepMachine
Stop-SpiderComposeStack -StopMachine:$stopMachine

Write-Host "Done. Images were not removed; run build-image.ps1 or compose up to reuse them."
