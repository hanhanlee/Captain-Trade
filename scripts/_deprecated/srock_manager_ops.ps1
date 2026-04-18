param(
    [Parameter(Position = 0)]
    [ValidateSet("online", "offline", "restart-online")]
    [string]$Operation = "online"
)

$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$AppPy = Join-Path $Root "app.py"
$RuntimeDir = Join-Path $Root "runtime"
$AuthScript = Join-Path $Root "scripts\caddy_auth_proxy.ps1"
$FunnelScript = Join-Path $Root "scripts\tailscale_funnel.ps1"
$CaddyExe = Join-Path $Root "tools\caddy\caddy.exe"
$Caddyfile = Join-Path $Root "secrets\Caddyfile"
$OpsLogFile = Join-Path $RuntimeDir "manager_ops.log"
$OpsPidFile = Join-Path $RuntimeDir "manager_ops.pid"
$StreamlitPidFile = Join-Path $RuntimeDir "streamlit.pid"
$StreamlitOutLogFile = Join-Path $RuntimeDir "streamlit.out.log"
$StreamlitErrLogFile = Join-Path $RuntimeDir "streamlit.err.log"
$CaddyPidFile = Join-Path $RuntimeDir "caddy.pid"
$CaddyOutLogFile = Join-Path $RuntimeDir "caddy.out.log"
$CaddyErrLogFile = Join-Path $RuntimeDir "caddy.err.log"
$StreamlitPort = 8501
$AuthPort = 8080
$PythonExe = "python"

function Ensure-Runtime {
    New-Item -ItemType Directory -Force $RuntimeDir | Out-Null
}

function Write-OpLog {
    param([string]$Message)
    Ensure-Runtime
    $stamp = Get-Date -Format "HH:mm:ss"
    Add-Content -Path $OpsLogFile -Encoding UTF8 -Value "[$stamp] $Message"
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

function Invoke-CommandCapture {
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

function Start-StockTool {
    Ensure-Runtime
    if (Get-ListenerPid $StreamlitPort) {
        return "Streamlit already listens on port $StreamlitPort."
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
        return "Started Streamlit. PID: $listenPid"
    }
    return "Streamlit start requested, but port $StreamlitPort did not open within 60 seconds. Check runtime/streamlit.err.log."
}

function Stop-PortProcess {
    param([int]$Port)
    $pidValue = Get-ListenerPid $Port
    if (!$pidValue) {
        return "No process is listening on port $Port."
    }
    Stop-Process -Id $pidValue -Force
    if (Wait-PortClosed $Port 10) {
        return "Stopped PID $pidValue on port $Port."
    }
    return "Stop requested for PID $pidValue on port $Port, but port $Port is still busy."
}

function Start-AuthProxy {
    if (Get-ListenerPid $AuthPort) {
        return "Caddy auth proxy already listens on port $AuthPort."
    }
    if (!(Test-Path $CaddyExe)) {
        return "Caddy not found: $CaddyExe"
    }
    if (!(Test-Path $Caddyfile)) {
        Write-OpLog "Caddyfile not found. Running auth setup first."
        $setupOutput = Invoke-CommandCapture {
            powershell -NoProfile -ExecutionPolicy Bypass -File $AuthScript setup
        }
        Write-OpLog $setupOutput
    }

    $stalePid = if (Test-Path $CaddyPidFile) { Get-Content $CaddyPidFile -ErrorAction SilentlyContinue | Select-Object -First 1 } else { $null }
    if ($stalePid) {
        $staleProc = Get-Process -Id ([int]$stalePid) -ErrorAction SilentlyContinue
        if ($staleProc -and !$staleProc.HasExited) {
            Stop-Process -Id $staleProc.Id -Force -ErrorAction SilentlyContinue
            Wait-PortClosed $AuthPort 5 | Out-Null
        }
        Remove-Item $CaddyPidFile -Force -ErrorAction SilentlyContinue
    }

    Push-Location $Root
    try {
        & $CaddyExe start --config $Caddyfile --adapter caddyfile | Out-Null
    } finally {
        Pop-Location
    }

    if (Wait-PortOpen $AuthPort 25) {
        $listenPid = Get-ListenerPid $AuthPort
        Set-Content -Encoding ASCII $CaddyPidFile $listenPid
        return "Caddy auth proxy started. PID: $listenPid"
    }

    return "Caddy auth proxy failed to listen on port $AuthPort within 25 seconds."
}

function Stop-AuthProxy {
    $messages = New-Object System.Collections.Generic.List[string]
    $listenPid = Get-ListenerPid $AuthPort
    if ($listenPid) {
        Stop-Process -Id $listenPid -Force -ErrorAction SilentlyContinue
        Wait-PortClosed $AuthPort 10 | Out-Null
        $messages.Add("Stopped Caddy listener PID $listenPid.")
    } else {
        $messages.Add("Caddy auth proxy is not listening on port $AuthPort.")
    }

    if (Test-Path $CaddyPidFile) {
        $pidValue = Get-Content $CaddyPidFile -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($pidValue) {
            Stop-Process -Id ([int]$pidValue) -Force -ErrorAction SilentlyContinue
        }
        Remove-Item $CaddyPidFile -Force -ErrorAction SilentlyContinue
    }
    return ($messages -join "`r`n")
}

function Start-Funnel {
    return Invoke-CommandCapture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $FunnelScript start -Target "http://127.0.0.1:$AuthPort"
    }
}

function Stop-Funnel {
    $output = Invoke-CommandCapture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $FunnelScript stop -Target "http://127.0.0.1:$AuthPort"
    }
    if ($output -match "handler does not exist") {
        return "Tailscale Funnel was already stopped."
    }
    return $output
}

function Test-ProtectedEndpoint {
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$AuthPort/" -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
        return "Unexpected: protected endpoint returned HTTP $($resp.StatusCode) without auth."
    } catch {
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode.value__ -eq 401) {
            return "OK: Basic Auth is active. Unauthenticated request returned 401."
        }
        return "Endpoint test failed: $($_.Exception.Message)"
    }
}

function Invoke-Online {
    Write-OpLog "Step 1/4 Start Streamlit"
    Write-OpLog (Start-StockTool)
    Write-OpLog "Step 2/4 Start Basic Auth proxy"
    Write-OpLog (Start-AuthProxy)
    Write-OpLog "Step 3/4 Verify Basic Auth"
    Write-OpLog (Test-ProtectedEndpoint)
    Write-OpLog "Step 4/4 Start Tailscale Funnel"
    Write-OpLog (Start-Funnel)
}

function Invoke-Offline {
    Write-OpLog "Step 1/3 Stop Tailscale Funnel"
    Write-OpLog (Stop-Funnel)
    Write-OpLog "Step 2/3 Stop Basic Auth proxy"
    Write-OpLog (Stop-AuthProxy)
    Write-OpLog "Step 3/3 Stop Streamlit"
    Write-OpLog (Stop-PortProcess $StreamlitPort)
}

Ensure-Runtime
Set-Content -Encoding ASCII $OpsPidFile $PID

try {
    Write-OpLog "One-click operation started: $Operation"
    switch ($Operation) {
        "online" { Invoke-Online }
        "offline" { Invoke-Offline }
        "restart-online" {
            Write-OpLog "Restart requested for an already-online system."
            Invoke-Offline
            Invoke-Online
        }
    }
    Write-OpLog "One-click operation finished: $Operation"
} catch {
    Write-OpLog "ERROR: $($_.Exception.Message)"
    exit 1
} finally {
    Remove-Item $OpsPidFile -Force -ErrorAction SilentlyContinue
}
