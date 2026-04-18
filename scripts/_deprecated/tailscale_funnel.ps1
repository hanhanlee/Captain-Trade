param(
    [Parameter(Position = 0)]
    [ValidateSet("start", "stop", "status", "reset")]
    [string]$Action = "status",

    [string]$Target = "http://127.0.0.1:8080",

    [ValidateSet(443, 8443, 10000)]
    [int]$HttpsPort = 443
)

$ErrorActionPreference = "Stop"

function Find-Tailscale {
    $cmd = Get-Command tailscale -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }

    $defaultPath = "C:\Program Files\Tailscale\tailscale.exe"
    if (Test-Path $defaultPath) {
        return $defaultPath
    }

    throw "tailscale CLI not found. Install Tailscale for Windows or add tailscale.exe to PATH."
}

function Invoke-Tailscale {
    param([string[]]$Arguments)
    $exe = Find-Tailscale
    & $exe @Arguments
}

Write-Host "Tailscale Funnel action: $Action" -ForegroundColor Cyan
Write-Host "Target: $Target" -ForegroundColor DarkGray
Write-Host "HTTPS port: $HttpsPort" -ForegroundColor DarkGray

switch ($Action) {
    "start" {
        Write-Host "Starting Funnel and forwarding public HTTPS traffic to $Target ..." -ForegroundColor Yellow
        Invoke-Tailscale @("funnel", "--bg", "--https=$HttpsPort", $Target)
        Write-Host ""
        Invoke-Tailscale @("funnel", "status")
    }
    "stop" {
        Write-Host "Stopping Funnel ..." -ForegroundColor Yellow
        Invoke-Tailscale @("funnel", "--https=$HttpsPort", $Target, "off")
        Write-Host ""
        Invoke-Tailscale @("funnel", "status")
    }
    "status" {
        Invoke-Tailscale @("status")
        Write-Host ""
        Invoke-Tailscale @("funnel", "status")
    }
    "reset" {
        Write-Host "Resetting Funnel configuration ..." -ForegroundColor Yellow
        Invoke-Tailscale @("funnel", "reset")
        Write-Host ""
        Invoke-Tailscale @("funnel", "status")
    }
}
