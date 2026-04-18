param(
    [Parameter(Position = 0)]
    [ValidateSet(
        "help",
        "status",
        "online",
        "offline",
        "restart",
        "start-streamlit",
        "stop-streamlit",
        "restart-streamlit",
        "start-auth",
        "stop-auth",
        "restart-auth",
        "test-auth",
        "start-funnel",
        "stop-funnel",
        "funnel-status",
        "add-user",
        "reset-user",
        "list-users",
        "logs"
    )]
    [string]$Action = "status",

    [string]$User = "srock",
    [string]$Password = "",
    [int]$StreamlitPort = 8501,
    [int]$AuthPort = 8080,
    [int]$HttpsPort = 443,
    [switch]$NoFunnel
)

$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$AppPy = Join-Path $Root "app.py"
$RuntimeDir = Join-Path $Root "runtime"
$SecretsDir = Join-Path $Root "secrets"
$AuthScript = Join-Path $Root "scripts\caddy_auth_proxy.ps1"
$FunnelScript = Join-Path $Root "scripts\tailscale_funnel.ps1"
$CaddyExe = Join-Path $Root "tools\caddy\caddy.exe"
$Caddyfile = Join-Path $SecretsDir "Caddyfile"
$CredentialFile = Join-Path $SecretsDir "basic_auth_credentials.txt"
$UsersFile = Join-Path $SecretsDir "basic_auth_users.json"
$StreamlitPidFile = Join-Path $RuntimeDir "streamlit.pid"
$StreamlitOutLogFile = Join-Path $RuntimeDir "streamlit.out.log"
$StreamlitErrLogFile = Join-Path $RuntimeDir "streamlit.err.log"
$CaddyPidFile = Join-Path $RuntimeDir "caddy.pid"
$CaddyOutLogFile = Join-Path $RuntimeDir "caddy.out.log"
$CaddyErrLogFile = Join-Path $RuntimeDir "caddy.err.log"
$ManagerOpsLogFile = Join-Path $RuntimeDir "manager_ops.log"
$PythonExe = "python"

function Ensure-Runtime {
    New-Item -ItemType Directory -Force $RuntimeDir | Out-Null
}

function Write-Ok {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-WarnLine {
    param([string]$Message)
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

function Write-Fail {
    param([string]$Message)
    Write-Host "[FAIL] $Message" -ForegroundColor Red
}

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "== $Message ==" -ForegroundColor Cyan
}

function Get-ListenerPid {
    param([int]$Port)
    $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($conn) {
        return [int]$conn.OwningProcess
    }
    return $null
}

function Get-ProcessLabel {
    param([int]$Port)
    $pidValue = Get-ListenerPid $Port
    if (!$pidValue) {
        return "stopped"
    }
    $process = Get-Process -Id $pidValue -ErrorAction SilentlyContinue
    if ($process) {
        return "running pid=$pidValue process=$($process.ProcessName)"
    }
    return "running pid=$pidValue"
}

function Wait-PortOpen {
    param([int]$Port, [int]$TimeoutSeconds = 10)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Get-ListenerPid $Port) {
            return $true
        }
        Start-Sleep -Milliseconds 300
    }
    return $false
}

function Wait-PortClosed {
    param([int]$Port, [int]$TimeoutSeconds = 10)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (!(Get-ListenerPid $Port)) {
            return $true
        }
        Start-Sleep -Milliseconds 300
    }
    return $false
}

function Invoke-Capture {
    param([scriptblock]$Block)
    try {
        $output = & $Block 2>&1 | Out-String
        if (!$output.Trim()) {
            return "OK"
        }
        return $output.Trim()
    } catch {
        return $_.Exception.Message
    }
}

function Find-Tailscale {
    $cmd = Get-Command tailscale -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }
    $defaultPath = "C:\Program Files\Tailscale\tailscale.exe"
    if (Test-Path $defaultPath) {
        return $defaultPath
    }
    return $null
}

function Start-Streamlit {
    Ensure-Runtime
    $existingPid = Get-ListenerPid $StreamlitPort
    if ($existingPid) {
        Write-Ok "Streamlit already listens on port $StreamlitPort. PID: $existingPid"
        return
    }
    if (!(Test-Path $AppPy)) {
        throw "app.py not found: $AppPy"
    }

    $args = "-m streamlit run `"$AppPy`" --server.address 0.0.0.0 --server.port $StreamlitPort --server.headless true"
    $proc = Start-Process -FilePath $PythonExe `
        -ArgumentList $args `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $StreamlitOutLogFile `
        -RedirectStandardError $StreamlitErrLogFile `
        -WindowStyle Hidden `
        -PassThru
    Set-Content -Encoding ASCII $StreamlitPidFile $proc.Id

    if (Wait-PortOpen $StreamlitPort 60) {
        $listenPid = Get-ListenerPid $StreamlitPort
        Write-Ok "Streamlit started on port $StreamlitPort. PID: $listenPid"
        return
    }

    throw "Streamlit did not listen on port $StreamlitPort within 60 seconds. Check $StreamlitErrLogFile"
}

function Stop-Port {
    param([int]$Port, [string]$Name)
    $pidValue = Get-ListenerPid $Port
    if (!$pidValue) {
        Write-Ok "$Name is already stopped. Port $Port is free."
        return
    }
    Stop-Process -Id $pidValue -Force -ErrorAction SilentlyContinue
    if (Wait-PortClosed $Port 10) {
        Write-Ok "$Name stopped. PID was $pidValue."
        return
    }
    throw "$Name stop requested, but port $Port is still busy."
}

function Stop-Streamlit {
    Stop-Port $StreamlitPort "Streamlit"
    Remove-Item $StreamlitPidFile -Force -ErrorAction SilentlyContinue
}

function Restart-Streamlit {
    Stop-Streamlit
    Start-Streamlit
}

function Ensure-CaddyConfig {
    if (!(Test-Path $CaddyExe)) {
        throw "Caddy not found: $CaddyExe"
    }
    if (!(Test-Path $Caddyfile)) {
        Write-WarnLine "Caddyfile is missing. Creating default Basic Auth user."
        $output = Invoke-Capture {
            powershell -NoProfile -ExecutionPolicy Bypass -File $AuthScript setup -User $User -Password $Password
        }
        Write-Host $output
    }
}

function Start-AuthProxy {
    Ensure-Runtime
    Ensure-CaddyConfig
    $existingPid = Get-ListenerPid $AuthPort
    if ($existingPid) {
        Write-Ok "Auth proxy already listens on port $AuthPort. PID: $existingPid"
        return
    }

    if (Test-Path $CaddyPidFile) {
        $pidValue = Get-Content $CaddyPidFile -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($pidValue) {
            Stop-Process -Id ([int]$pidValue) -Force -ErrorAction SilentlyContinue
        }
        Remove-Item $CaddyPidFile -Force -ErrorAction SilentlyContinue
        Wait-PortClosed $AuthPort 5 | Out-Null
    }

    Push-Location $Root
    try {
        & $CaddyExe start --config $Caddyfile --adapter caddyfile
    } finally {
        Pop-Location
    }

    if (Wait-PortOpen $AuthPort 25) {
        $listenPid = Get-ListenerPid $AuthPort
        Set-Content -Encoding ASCII $CaddyPidFile $listenPid
        Write-Ok "Auth proxy started on port $AuthPort. PID: $listenPid"
        return
    }

    throw "Auth proxy did not listen on port $AuthPort within 25 seconds."
}

function Stop-AuthProxy {
    $pidValue = Get-ListenerPid $AuthPort
    if ($pidValue) {
        Stop-Process -Id $pidValue -Force -ErrorAction SilentlyContinue
        if (!(Wait-PortClosed $AuthPort 10)) {
            throw "Auth proxy stop requested, but port $AuthPort is still busy."
        }
        Write-Ok "Auth proxy stopped. PID was $pidValue."
    } else {
        Write-Ok "Auth proxy is already stopped. Port $AuthPort is free."
    }

    if (Test-Path $CaddyPidFile) {
        $filePid = Get-Content $CaddyPidFile -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($filePid) {
            Stop-Process -Id ([int]$filePid) -Force -ErrorAction SilentlyContinue
        }
        Remove-Item $CaddyPidFile -Force -ErrorAction SilentlyContinue
    }
}

function Restart-AuthProxy {
    Stop-AuthProxy
    Start-AuthProxy
}

function Test-AuthProxy {
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$AuthPort/" -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
        Write-WarnLine "Protected URL returned HTTP $($resp.StatusCode) without auth. Check Caddy config."
    } catch {
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode.value__ -eq 401) {
            Write-Ok "Basic Auth is active. Unauthenticated request returned 401."
            return
        }
        throw "Auth test failed: $($_.Exception.Message)"
    }
}

function Invoke-Tailscale {
    param([string[]]$Arguments, [switch]$AllowAlreadyStopped)
    $exe = Find-Tailscale
    if (!$exe) {
        throw "tailscale CLI not found. Install Tailscale for Windows or add tailscale.exe to PATH."
    }
    $output = Invoke-Capture {
        & $exe @Arguments
    }
    if ($AllowAlreadyStopped -and $output -match "handler does not exist") {
        Write-Ok "Tailscale Funnel is already stopped."
        return
    }
    Write-Host $output
}

function Start-Funnel {
    if ($NoFunnel) {
        Write-WarnLine "NoFunnel was specified. Skipping Funnel start."
        return
    }
    Invoke-Tailscale @("funnel", "--bg", "--https=$HttpsPort", "http://127.0.0.1:$AuthPort")
    Write-Ok "Funnel start requested for http://127.0.0.1:$AuthPort"
}

function Stop-Funnel {
    if ($NoFunnel) {
        Write-WarnLine "NoFunnel was specified. Skipping Funnel stop."
        return
    }
    Invoke-Tailscale @("funnel", "--https=$HttpsPort", "http://127.0.0.1:$AuthPort", "off") -AllowAlreadyStopped
}

function Show-FunnelStatus {
    Invoke-Tailscale @("status")
    Write-Host ""
    Invoke-Tailscale @("funnel", "status")
}

function Add-OrResetUser {
    param([ValidateSet("add-user", "reset-user")][string]$Mode)
    Ensure-CaddyConfig
    $args = @($Mode, "-User", $User)
    if ($Password) {
        $args += @("-Password", $Password)
    }
    $output = Invoke-Capture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $AuthScript @args
    }
    Write-Host $output
    if (Get-ListenerPid $AuthPort) {
        Write-WarnLine "Auth proxy is running. Restarting it to apply credential changes."
        Restart-AuthProxy
    }
}

function List-Users {
    $output = Invoke-Capture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $AuthScript list-users
    }
    Write-Host $output
}

function Show-Status {
    Write-Host ""
    Write-Host "Srock status" -ForegroundColor Cyan
    Write-Host "------------" -ForegroundColor Cyan
    Write-Host ("Streamlit : {0}" -f (Get-ProcessLabel $StreamlitPort))
    Write-Host ("Auth proxy: {0}" -f (Get-ProcessLabel $AuthPort))
    if (Test-Path $CredentialFile) {
        Write-Host "Credentials: configured"
    } else {
        Write-Host "Credentials: missing"
    }
    if (Test-Path $UsersFile) {
        Write-Host "Users file : $UsersFile"
    }
    Write-Host "Local app  : http://127.0.0.1:$StreamlitPort"
    Write-Host "Protected  : http://127.0.0.1:$AuthPort"
    $tailscaleExe = Find-Tailscale
    if ($tailscaleExe) {
        Write-Host "Tailscale  : installed"
    } else {
        Write-Host "Tailscale  : not found"
    }
    Write-Host ""
}

function Show-Logs {
    Write-Host ""
    Write-Host "Recent logs" -ForegroundColor Cyan
    Write-Host "-----------" -ForegroundColor Cyan
    foreach ($file in @($StreamlitErrLogFile, $CaddyErrLogFile, $ManagerOpsLogFile)) {
        Write-Host ""
        Write-Host $file -ForegroundColor DarkGray
        if (Test-Path $file) {
            Get-Content $file | Select-Object -Last 30
        } else {
            Write-Host "(missing)"
        }
    }
}

function Show-Help {
    Write-Host @"
Srock CLI

Usage:
  .\scripts\srockctl.ps1 status
  .\scripts\srockctl.ps1 online
  .\scripts\srockctl.ps1 offline
  .\scripts\srockctl.ps1 restart

Service:
  start-streamlit      Start Streamlit on port $StreamlitPort
  stop-streamlit       Stop Streamlit
  restart-streamlit    Restart Streamlit

Security gateway:
  start-auth           Start Caddy Basic Auth proxy on port $AuthPort
  stop-auth            Stop Caddy Basic Auth proxy
  restart-auth         Restart Caddy Basic Auth proxy
  test-auth            Verify unauthenticated requests return 401

External access:
  start-funnel         Start Tailscale Funnel to http://127.0.0.1:$AuthPort
  stop-funnel          Stop Tailscale Funnel
  funnel-status        Show Tailscale and Funnel status

Accounts:
  add-user -User USER -Password PASS
  reset-user -User USER -Password PASS
  list-users

Options:
  -NoFunnel            Skip Funnel operations for online/offline/restart
  -HttpsPort 443       Funnel HTTPS port: 443, 8443, or 10000
"@
}

try {
    switch ($Action) {
        "help" { Show-Help }
        "status" { Show-Status }
        "online" {
            Write-Step "Start Streamlit"
            Start-Streamlit
            Write-Step "Start Auth Proxy"
            Start-AuthProxy
            Write-Step "Test Basic Auth"
            Test-AuthProxy
            Write-Step "Start Funnel"
            Start-Funnel
            Write-Step "Status"
            Show-Status
        }
        "offline" {
            Write-Step "Stop Funnel"
            Stop-Funnel
            Write-Step "Stop Auth Proxy"
            Stop-AuthProxy
            Write-Step "Stop Streamlit"
            Stop-Streamlit
            Write-Step "Status"
            Show-Status
        }
        "restart" {
            Write-Step "Offline"
            Stop-Funnel
            Stop-AuthProxy
            Stop-Streamlit
            Write-Step "Online"
            Start-Streamlit
            Start-AuthProxy
            Test-AuthProxy
            Start-Funnel
            Write-Step "Status"
            Show-Status
        }
        "start-streamlit" { Start-Streamlit }
        "stop-streamlit" { Stop-Streamlit }
        "restart-streamlit" { Restart-Streamlit }
        "start-auth" { Start-AuthProxy }
        "stop-auth" { Stop-AuthProxy }
        "restart-auth" { Restart-AuthProxy }
        "test-auth" { Test-AuthProxy }
        "start-funnel" { Start-Funnel }
        "stop-funnel" { Stop-Funnel }
        "funnel-status" { Show-FunnelStatus }
        "add-user" { Add-OrResetUser "add-user" }
        "reset-user" { Add-OrResetUser "reset-user" }
        "list-users" { List-Users }
        "logs" { Show-Logs }
    }
    exit 0
} catch {
    Write-Fail $_.Exception.Message
    exit 1
}
