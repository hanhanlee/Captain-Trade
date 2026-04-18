param(
    [Parameter(Position = 0)]
    [ValidateSet("setup", "add-user", "reset-user", "list-users", "start", "run", "stop", "status", "restart")]
    [string]$Action = "status",

    [string]$User = "srock",
    [string]$Password = "",
    [string]$Listen = "127.0.0.1:8080",
    [string]$Upstream = "127.0.0.1:8501"
)

$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$CaddyExe = Join-Path $Root "tools\caddy\caddy.exe"
$SecretsDir = Join-Path $Root "secrets"
$RuntimeDir = Join-Path $Root "runtime"
$Caddyfile = Join-Path $SecretsDir "Caddyfile"
$CredentialFile = Join-Path $SecretsDir "basic_auth_credentials.txt"
$UsersFile = Join-Path $SecretsDir "basic_auth_users.json"
$PidFile = Join-Path $RuntimeDir "caddy.pid"
$OutLogFile = Join-Path $RuntimeDir "caddy.out.log"
$ErrLogFile = Join-Path $RuntimeDir "caddy.err.log"

function Assert-Caddy {
    if (!(Test-Path $CaddyExe)) {
        throw "Caddy not found: $CaddyExe"
    }
}

function New-RandomPassword {
    $bytes = New-Object byte[] 24
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $rng.GetBytes($bytes)
    } finally {
        $rng.Dispose()
    }
    return [Convert]::ToBase64String($bytes).TrimEnd("=")
}

function Get-ProxyProcess {
    if (!(Test-Path $PidFile)) {
        return $null
    }
    $pidValue = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (!$pidValue) {
        return $null
    }
    return Get-Process -Id ([int]$pidValue) -ErrorAction SilentlyContinue
}

function Read-AuthUsers {
    $users = [ordered]@{}
    if (Test-Path $UsersFile) {
        $items = Get-Content $UsersFile -Raw | ConvertFrom-Json
        foreach ($item in @($items)) {
            if ($item.user -and $item.hash) {
                $users[$item.user] = $item.hash
            }
        }
    } elseif (Test-Path $Caddyfile) {
        $inAuthBlock = $false
        foreach ($line in (Get-Content $Caddyfile)) {
            if ($line -match "^\s*basic_auth\s*\{\s*$") {
                $inAuthBlock = $true
                continue
            }
            if ($inAuthBlock -and $line -match "^\s*\}\s*$") {
                break
            }
            if ($inAuthBlock -and $line -match "^\s*(\S+)\s+(\S+)\s*$") {
                $users[$matches[1]] = $matches[2]
            }
        }
    }
    return $users
}

function Write-AuthUsers {
    param([System.Collections.IDictionary]$Users)
    $items = foreach ($key in $Users.Keys) {
        [pscustomobject]@{
            user = $key
            hash = $Users[$key]
            updated_at = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
        }
    }
    $items | ConvertTo-Json -Depth 3 | Set-Content -Encoding UTF8 $UsersFile
}

function Write-Caddyfile {
    param([System.Collections.IDictionary]$Users)
    if ($Users.Count -eq 0) {
        throw "At least one Basic Auth user is required."
    }

    $authLines = foreach ($key in $Users.Keys) {
        "        $key $($Users[$key])"
    }
    $authBlock = $authLines -join "`r`n"

    @"
{
    auto_https off
    admin 127.0.0.1:2019
    persist_config off
    storage file_system ./runtime/caddy_data
}

http://$Listen {
    bind 127.0.0.1
    encode gzip

    basic_auth {
$authBlock
    }

    reverse_proxy $Upstream
}
"@ | Set-Content -Encoding UTF8 $Caddyfile
}

function Write-LatestCredential {
    param([string]$UserName, [string]$PlainPassword)
    @"
Basic Auth latest credential

URL:      http://$Listen
Username: $UserName
Password: $PlainPassword

Changed:  $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")

User list: $UsersFile
Keep this file local. It is ignored by git.
"@ | Set-Content -Encoding UTF8 $CredentialFile
}

function Invoke-Setup {
    Assert-Caddy
    New-Item -ItemType Directory -Force $SecretsDir | Out-Null
    New-Item -ItemType Directory -Force $RuntimeDir | Out-Null

    $password = if ($Password) { $Password } else { New-RandomPassword }
    $hash = & $CaddyExe hash-password --plaintext $password
    if ($LASTEXITCODE -ne 0 -or !$hash) {
        throw "Failed to hash password with Caddy."
    }

    $users = [ordered]@{}
    $users[$User] = $hash
    Write-AuthUsers $users
    Write-Caddyfile $users
    Write-LatestCredential $User $password

    Write-Host "Created $Caddyfile" -ForegroundColor Green
    Write-Host "Created $CredentialFile" -ForegroundColor Green
    Write-Host "Created $UsersFile" -ForegroundColor Green
    Write-Host ""
    Write-Host "Username: $User" -ForegroundColor Cyan
    Write-Host "Password: $password" -ForegroundColor Cyan
}

function Invoke-UpsertUser {
    param([string]$Mode)
    Assert-Caddy
    New-Item -ItemType Directory -Force $SecretsDir | Out-Null
    New-Item -ItemType Directory -Force $RuntimeDir | Out-Null

    if (!$User.Trim()) {
        throw "User is required."
    }

    $password = if ($Password) { $Password } else { New-RandomPassword }
    $hash = & $CaddyExe hash-password --plaintext $password
    if ($LASTEXITCODE -ne 0 -or !$hash) {
        throw "Failed to hash password with Caddy."
    }

    $users = Read-AuthUsers
    if ($users.Count -eq 0 -and (Test-Path $Caddyfile)) {
        Write-Host "Existing Caddyfile found but user registry is missing. The registry will be recreated with this user." -ForegroundColor Yellow
    }

    $exists = $users.Contains($User)
    $users[$User] = $hash
    Write-AuthUsers $users
    Write-Caddyfile $users
    Write-LatestCredential $User $password

    $verb = if ($exists) { "updated" } elseif ($Mode -eq "reset-user") { "created" } else { "added" }
    Write-Host "User $User $verb." -ForegroundColor Green
    Write-Host "Restart the auth proxy if it is already running." -ForegroundColor Yellow
    Write-Host "Username: $User" -ForegroundColor Cyan
    Write-Host "Password: $password" -ForegroundColor Cyan
}

function Invoke-ListUsers {
    $users = Read-AuthUsers
    if ($users.Count -eq 0) {
        Write-Host "No Basic Auth users configured." -ForegroundColor Yellow
        return
    }
    Write-Host "Configured Basic Auth users:" -ForegroundColor Green
    foreach ($key in $users.Keys) {
        Write-Host "- $key"
    }
}

function Invoke-Start {
    Assert-Caddy
    if (!(Test-Path $Caddyfile)) {
        Write-Host "Caddyfile not found. Running setup first..." -ForegroundColor Yellow
        Invoke-Setup
    }

    $existing = Get-ProxyProcess
    if ($existing) {
        Write-Host "Caddy auth proxy is already running. PID: $($existing.Id)" -ForegroundColor Green
        return
    }

    New-Item -ItemType Directory -Force $RuntimeDir | Out-Null
    $args = "run --config `"$Caddyfile`" --adapter caddyfile"
    $proc = Start-Process -FilePath $CaddyExe `
        -ArgumentList $args `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $OutLogFile `
        -RedirectStandardError $ErrLogFile `
        -WindowStyle Hidden `
        -PassThru
    Start-Sleep -Seconds 1
    $listenPort = [int](($Listen -split ":")[-1])
    $conn = Get-NetTCPConnection -LocalPort $listenPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($conn) {
        Set-Content -Encoding ASCII $PidFile $conn.OwningProcess
    } else {
        Set-Content -Encoding ASCII $PidFile $proc.Id
    }
    $pidLabel = if ($conn) { $conn.OwningProcess } else { "unknown" }
    Write-Host "Caddy auth proxy started. PID: $pidLabel" -ForegroundColor Green
    Write-Host "Local protected URL: http://$Listen" -ForegroundColor Cyan
}

function Invoke-Run {
    Assert-Caddy
    if (!(Test-Path $Caddyfile)) {
        Write-Host "Caddyfile not found. Running setup first..." -ForegroundColor Yellow
        Invoke-Setup
    }
    Write-Host "Running Caddy auth proxy in the foreground. Press Ctrl+C to stop." -ForegroundColor Yellow
    Push-Location $Root
    try {
        & $CaddyExe run --config $Caddyfile --adapter caddyfile
    } finally {
        Pop-Location
    }
}

function Invoke-Stop {
    $proc = Get-ProxyProcess
    if (!$proc) {
        Write-Host "Caddy auth proxy is not running." -ForegroundColor Yellow
        if (Test-Path $PidFile) {
            Remove-Item $PidFile -Force
        }
        return
    }
    Stop-Process -Id $proc.Id -Force
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    Write-Host "Caddy auth proxy stopped." -ForegroundColor Green
}

function Invoke-Status {
    $proc = Get-ProxyProcess
    if ($proc) {
        Write-Host "Caddy auth proxy running. PID: $($proc.Id)" -ForegroundColor Green
    } else {
        Write-Host "Caddy auth proxy not running." -ForegroundColor Yellow
    }
    Write-Host "Listen:   http://$Listen"
    Write-Host "Upstream: http://$Upstream"
    if (Test-Path $CredentialFile) {
        Write-Host "Credentials file: $CredentialFile"
    }
    if (Test-Path $UsersFile) {
        Write-Host "Users file: $UsersFile"
    }
    if (Test-Path $OutLogFile) {
        Write-Host "Stdout log: $OutLogFile"
    }
    if (Test-Path $ErrLogFile) {
        Write-Host "Stderr log: $ErrLogFile"
    }
}

switch ($Action) {
    "setup" { Invoke-Setup }
    "add-user" { Invoke-UpsertUser "add-user" }
    "reset-user" { Invoke-UpsertUser "reset-user" }
    "list-users" { Invoke-ListUsers }
    "start" { Invoke-Start }
    "run" { Invoke-Run }
    "stop" { Invoke-Stop }
    "status" { Invoke-Status }
    "restart" {
        Invoke-Stop
        Invoke-Start
    }
}
