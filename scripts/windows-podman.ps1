# Shared Podman / podman-compose setup for Windows (and cross-platform compose discovery).
$ErrorActionPreference = "Stop"

function Add-PythonScriptsToPath {
    $dirs = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($root in @($env:LOCALAPPDATA, $env:APPDATA, $env:USERPROFILE)) {
        if (-not $root) { continue }
        foreach ($pattern in @(
            "$root\Python\Python*\Scripts",
            "$root\Python\pythoncore-*\Scripts",
            "$root\AppData\Local\Programs\Python\Python*\Scripts"
        )) {
            Get-ChildItem -Path $pattern -ErrorAction SilentlyContinue | ForEach-Object {
                [void]$dirs.Add($_.FullName)
            }
        }
    }
    foreach ($dir in $dirs) {
        if ($env:PATH -notlike "*$dir*") {
            $env:PATH = "$dir;$env:PATH"
        }
    }
}

function Test-PodmanReady {
    if (-not (Get-Command podman -ErrorAction SilentlyContinue)) { return $false }
    $null = podman info --format "{{.Host.Arch}}" 2>$null
    return $LASTEXITCODE -eq 0
}

function Ensure-PodmanMachine {
    if (-not (Get-Command podman -ErrorAction SilentlyContinue)) {
        throw "podman not found in PATH. Install Podman Desktop or add podman to PATH."
    }
    if (Test-PodmanReady) { return }

    if ($IsWindows -or $env:OS -match "Windows") {
        Write-Host "--- podman not ready; starting podman machine ---"
        $prev = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        podman machine start 2>&1 | ForEach-Object { Write-Host $_ }
        $ErrorActionPreference = $prev
        if ($LASTEXITCODE -ne 0) { throw "podman machine start failed (exit $LASTEXITCODE)" }

        for ($i = 0; $i -lt 45; $i++) {
            if (Test-PodmanReady) {
                Write-Host "--- podman machine ready ---"
                return
            }
            Start-Sleep -Seconds 2
        }
        throw "podman did not become ready after machine start"
    }

    throw "podman info failed and automatic machine start is only implemented on Windows"
}

function Initialize-SpiderPodmanEnv {
    Add-PythonScriptsToPath
    Ensure-PodmanMachine
}

function Get-SpiderComposeCommand {
    param(
        [string]$PodmanFile = "podman-compose.yml",
        [string]$DockerFile = "docker-compose.yml"
    )

    Add-PythonScriptsToPath

    $pc = Get-Command podman-compose -ErrorAction SilentlyContinue
    if ($pc) {
        return @($pc.Source, "-f", $PodmanFile)
    }

    foreach ($py in @("python", "python3", "py")) {
        if (-not (Get-Command $py -ErrorAction SilentlyContinue)) { continue }
        & $py -c "import podman_compose" 2>$null
        if ($LASTEXITCODE -eq 0) {
            return @((Get-Command $py).Source, "-m", "podman_compose", "-f", $PodmanFile)
        }
    }

    if (Get-Command podman -ErrorAction SilentlyContinue) {
        $null = podman compose version 2>$null
        if ($LASTEXITCODE -eq 0) {
            return @((Get-Command podman).Source, "compose", "-f", $PodmanFile)
        }
    }

    if (Get-Command docker -ErrorAction SilentlyContinue) {
        $null = docker compose version 2>$null
        if ($LASTEXITCODE -eq 0) {
            return @((Get-Command docker).Source, "compose", "-f", $DockerFile)
        }
    }

    throw @"
podman-compose or docker compose required.
Install: pip install podman-compose
Or ensure podman-compose is on PATH (Python Scripts folder is added automatically).
"@
}

function Invoke-SpiderCompose {
    param(
        [Parameter(Mandatory)][string[]]$Compose,
        [Parameter(Mandatory)][string[]]$CmdArgs
    )
    $all = @($Compose + $CmdArgs)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $all[0] @($all[1..($all.Length - 1)]) 2>&1 | ForEach-Object { Write-Host $_ }
    $ErrorActionPreference = $prev
    if ($LASTEXITCODE -ne 0) { throw "compose failed: $($CmdArgs -join ' ')" }
}

function Stop-SpiderComposeStack {
    param(
        [switch]$StopMachine,
        [string]$PodmanFile = "podman-compose.yml"
    )

    Initialize-SpiderPodmanEnv
    $compose = Get-SpiderComposeCommand -PodmanFile $PodmanFile
    Write-Host "--- compose down (containers + volumes; images kept) ---"
    Invoke-SpiderCompose -Compose $compose -CmdArgs @("down", "-v")

    if ($StopMachine -and (Get-Command podman -ErrorAction SilentlyContinue)) {
        if ($IsWindows -or $env:OS -match "Windows") {
            Write-Host "--- stopping podman machine ---"
            $prev = $ErrorActionPreference
            $ErrorActionPreference = "Continue"
            podman machine stop 2>&1 | ForEach-Object { Write-Host $_ }
            $ErrorActionPreference = $prev
        }
    }
}
