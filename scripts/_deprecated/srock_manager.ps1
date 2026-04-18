param(
    [switch]$ValidateOnly
)

Add-Type -AssemblyName PresentationFramework
Add-Type -AssemblyName PresentationCore
Add-Type -AssemblyName WindowsBase

$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$AppPy = Join-Path $Root "app.py"
$PythonExe = "python"
$StreamlitPort = 8501
$AuthPort = 8080
$AuthScript = Join-Path $Root "scripts\caddy_auth_proxy.ps1"
$FunnelScript = Join-Path $Root "scripts\tailscale_funnel.ps1"
$OpsScript = Join-Path $Root "scripts\srock_manager_ops.ps1"
$CaddyExe = Join-Path $Root "tools\caddy\caddy.exe"
$Caddyfile = Join-Path $Root "secrets\Caddyfile"
$CredentialFile = Join-Path $Root "secrets\basic_auth_credentials.txt"
$RuntimeDir = Join-Path $Root "runtime"
$StreamlitPidFile = Join-Path $RuntimeDir "streamlit.pid"
$StreamlitOutLogFile = Join-Path $RuntimeDir "streamlit.out.log"
$StreamlitErrLogFile = Join-Path $RuntimeDir "streamlit.err.log"
$CaddyPidFile = Join-Path $RuntimeDir "caddy.pid"
$CaddyOutLogFile = Join-Path $RuntimeDir "caddy.out.log"
$CaddyErrLogFile = Join-Path $RuntimeDir "caddy.err.log"
$OpsLogFile = Join-Path $RuntimeDir "manager_ops.log"
$OpsPidFile = Join-Path $RuntimeDir "manager_ops.pid"
$script:OpsLogLinesRead = 0

function Ensure-Runtime {
    New-Item -ItemType Directory -Force $RuntimeDir | Out-Null
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
        return "Stopped"
    }
    try {
        $p = Get-Process -Id $pidValue -ErrorAction Stop
        return "PID $pidValue 繚 $($p.ProcessName)"
    } catch {
        return "PID $pidValue"
    }
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

function Restart-StockTool {
    $msg1 = Stop-PortProcess $StreamlitPort
    Start-Sleep -Seconds 1
    $msg2 = Start-StockTool
    return "$msg1`r`n$msg2"
}

function Start-AuthProxy {
    Ensure-Runtime
    if (Get-ListenerPid $AuthPort) {
        return "Caddy auth proxy already listens on port $AuthPort."
    }
    if (!(Test-Path $CaddyExe)) {
        return "Caddy not found: $CaddyExe"
    }
    if (!(Test-Path $Caddyfile)) {
        $setupOutput = Invoke-CommandCapture {
            powershell -NoProfile -ExecutionPolicy Bypass -File $AuthScript setup
        }
        Write-Log $setupOutput
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

function Restart-AuthProxy {
    return Invoke-CommandCapture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $AuthScript restart
    }
}

function Start-Funnel {
    return Invoke-CommandCapture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $FunnelScript start -Target "http://127.0.0.1:$AuthPort"
    }
}

function Stop-Funnel {
    return Invoke-CommandCapture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $FunnelScript stop -Target "http://127.0.0.1:$AuthPort"
    }
}

function Get-FunnelStatus {
    return Invoke-CommandCapture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $FunnelScript status -Target "http://127.0.0.1:$AuthPort"
    }
}

function Set-Credentials {
    param([string]$Action, [string]$User, [string]$Password)
    if (!$User.Trim()) {
        return "Username is required."
    }
    if (!$Password) {
        return "Password is required."
    }
    $wasRunning = [bool](Get-ListenerPid $AuthPort)
    $result = Invoke-CommandCapture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $AuthScript $Action -User $User -Password $Password
    }
    if ($wasRunning) {
        $restart = Restart-AuthProxy
        return "$result`r`n$restart"
    }
    return $result
}

function Read-Credentials {
    if (!(Test-Path $CredentialFile)) {
        return "Credentials file not found. Create a login first."
    }
    return Get-Content $CredentialFile -Raw
}

function Get-CredentialUsers {
    return Invoke-CommandCapture {
        powershell -NoProfile -ExecutionPolicy Bypass -File $AuthScript list-users
    }
}

function Open-Url {
    param([string]$Url)
    Start-Process $Url
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

function Test-SystemOnline {
    return ([bool](Get-ListenerPid $StreamlitPort) -and [bool](Get-ListenerPid $AuthPort))
}

function Test-SystemOffline {
    return (-not [bool](Get-ListenerPid $StreamlitPort) -and -not [bool](Get-ListenerPid $AuthPort))
}

function Get-RunningOperationProcess {
    if (!(Test-Path $OpsPidFile)) {
        return $null
    }
    $pidValue = Get-Content $OpsPidFile -ErrorAction SilentlyContinue | Select-Object -First 1
    if (!$pidValue) {
        return $null
    }
    return Get-Process -Id ([int]$pidValue) -ErrorAction SilentlyContinue
}

function Start-ManagerOperation {
    param([ValidateSet("online", "offline", "restart-online")][string]$Operation)
    Ensure-Runtime
    $existing = Get-RunningOperationProcess
    if ($existing) {
        return "Another one-click operation is already running. PID: $($existing.Id)"
    }
    if (!(Test-Path $OpsScript)) {
        return "Operation script not found: $OpsScript"
    }
    $args = "-NoProfile -ExecutionPolicy Bypass -File `"$OpsScript`" $Operation"
    $proc = Start-Process -FilePath "powershell" `
        -ArgumentList $args `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -PassThru
    return "Started one-click '$Operation' sequence in background. PID: $($proc.Id)"
}

function Pull-OperationLog {
    if (!(Test-Path $OpsLogFile) -or !$controls -or !$controls.LogBox) {
        return
    }
    $lines = @(Get-Content $OpsLogFile -ErrorAction SilentlyContinue)
    if (!$lines) {
        return
    }
    if ($script:OpsLogLinesRead -gt $lines.Count) {
        $script:OpsLogLinesRead = 0
    }
    if ($lines.Count -le $script:OpsLogLinesRead) {
        return
    }
    $newLines = $lines[$script:OpsLogLinesRead..($lines.Count - 1)]
    foreach ($line in $newLines) {
        $controls.LogBox.AppendText("$line`r`n")
    }
    $controls.LogBox.ScrollToEnd()
    $script:OpsLogLinesRead = $lines.Count
}

function Invoke-OneClickOnline {
    if (Test-SystemOnline) {
        $answer = [System.Windows.MessageBox]::Show(
            "System is already online.`n`nRestart the full system?",
            "System already online",
            [System.Windows.MessageBoxButton]::YesNo,
            [System.Windows.MessageBoxImage]::Question
        )
        if ($answer -ne [System.Windows.MessageBoxResult]::Yes) {
            return "One-click online cancelled. System was already online."
        }
        return Start-ManagerOperation "restart-online"
    }
    return Start-ManagerOperation "online"
}

function Invoke-OneClickOffline {
    if (Test-SystemOffline) {
        [System.Windows.MessageBox]::Show(
            "System is already offline.",
            "System already offline",
            [System.Windows.MessageBoxButton]::OK,
            [System.Windows.MessageBoxImage]::Information
        ) | Out-Null
        return "System is already offline."
    }
    return Start-ManagerOperation "offline"
}

$xaml = @"
<Window
    xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml"
    Title="Srock Control Center"
    Width="1120"
    Height="840"
    MinWidth="980"
    MinHeight="720"
    WindowStartupLocation="CenterScreen"
    Background="#09090B"
    FontFamily="Segoe UI">
    <Window.Resources>
        <SolidColorBrush x:Key="PanelBrush" Color="#111114"/>
        <SolidColorBrush x:Key="PanelBrush2" Color="#151519"/>
        <SolidColorBrush x:Key="LineBrush" Color="#2A2A30"/>
        <SolidColorBrush x:Key="TextMain" Color="#F4F4F5"/>
        <SolidColorBrush x:Key="TextMuted" Color="#A1A1AA"/>
        <SolidColorBrush x:Key="Green" Color="#22C55E"/>
        <SolidColorBrush x:Key="Red" Color="#F87171"/>
        <SolidColorBrush x:Key="Amber" Color="#F59E0B"/>
        <Style TargetType="Button">
            <Setter Property="Height" Value="42"/>
            <Setter Property="Padding" Value="16,0"/>
            <Setter Property="Margin" Value="0,0,10,10"/>
            <Setter Property="Foreground" Value="#F4F4F5"/>
            <Setter Property="Background" Value="#1D1D22"/>
            <Setter Property="BorderBrush" Value="#34343B"/>
            <Setter Property="BorderThickness" Value="1"/>
            <Setter Property="FontWeight" Value="SemiBold"/>
            <Setter Property="Cursor" Value="Hand"/>
            <Setter Property="Template">
                <Setter.Value>
                    <ControlTemplate TargetType="Button">
                        <Border
                            x:Name="Root"
                            Background="{TemplateBinding Background}"
                            BorderBrush="{TemplateBinding BorderBrush}"
                            BorderThickness="{TemplateBinding BorderThickness}"
                            CornerRadius="8">
                            <ContentPresenter HorizontalAlignment="Center" VerticalAlignment="Center"/>
                        </Border>
                        <ControlTemplate.Triggers>
                            <Trigger Property="IsMouseOver" Value="True">
                                <Setter TargetName="Root" Property="Background" Value="#27272F"/>
                                <Setter TargetName="Root" Property="BorderBrush" Value="#52525B"/>
                            </Trigger>
                            <Trigger Property="IsPressed" Value="True">
                                <Setter TargetName="Root" Property="Background" Value="#0F0F12"/>
                            </Trigger>
                            <Trigger Property="IsEnabled" Value="False">
                                <Setter Property="Opacity" Value="0.45"/>
                            </Trigger>
                        </ControlTemplate.Triggers>
                    </ControlTemplate>
                </Setter.Value>
            </Setter>
        </Style>
        <Style x:Key="AccentButton" TargetType="Button" BasedOn="{StaticResource {x:Type Button}}">
            <Setter Property="Background" Value="#0F2A1A"/>
            <Setter Property="BorderBrush" Value="#166534"/>
        </Style>
        <Style x:Key="DangerButton" TargetType="Button" BasedOn="{StaticResource {x:Type Button}}">
            <Setter Property="Background" Value="#2A1111"/>
            <Setter Property="BorderBrush" Value="#7F1D1D"/>
        </Style>
        <Style x:Key="HeroButton" TargetType="Button" BasedOn="{StaticResource {x:Type Button}}">
            <Setter Property="Height" Value="54"/>
            <Setter Property="Padding" Value="22,0"/>
            <Setter Property="FontSize" Value="15"/>
            <Setter Property="Background" Value="#0F2A1A"/>
            <Setter Property="BorderBrush" Value="#15803D"/>
        </Style>
        <Style x:Key="HeroDangerButton" TargetType="Button" BasedOn="{StaticResource {x:Type Button}}">
            <Setter Property="Height" Value="54"/>
            <Setter Property="Padding" Value="22,0"/>
            <Setter Property="FontSize" Value="15"/>
            <Setter Property="Background" Value="#2A1111"/>
            <Setter Property="BorderBrush" Value="#B91C1C"/>
        </Style>
        <Style TargetType="TextBox">
            <Setter Property="Height" Value="38"/>
            <Setter Property="Padding" Value="12,8"/>
            <Setter Property="Foreground" Value="#F4F4F5"/>
            <Setter Property="Background" Value="#0E0E12"/>
            <Setter Property="BorderBrush" Value="#32323A"/>
            <Setter Property="CaretBrush" Value="#F4F4F5"/>
        </Style>
        <Style TargetType="PasswordBox">
            <Setter Property="Height" Value="38"/>
            <Setter Property="Padding" Value="12,8"/>
            <Setter Property="Foreground" Value="#F4F4F5"/>
            <Setter Property="Background" Value="#0E0E12"/>
            <Setter Property="BorderBrush" Value="#32323A"/>
            <Setter Property="CaretBrush" Value="#F4F4F5"/>
        </Style>
    </Window.Resources>

    <Grid>
        <Grid.Background>
            <RadialGradientBrush Center="0.1,0.0" RadiusX="0.9" RadiusY="0.9">
                <GradientStop Color="#1A1A1F" Offset="0"/>
                <GradientStop Color="#09090B" Offset="0.62"/>
                <GradientStop Color="#050506" Offset="1"/>
            </RadialGradientBrush>
        </Grid.Background>

        <Grid Margin="34">
            <Grid.RowDefinitions>
                <RowDefinition Height="Auto"/>
                <RowDefinition Height="Auto"/>
                <RowDefinition Height="*"/>
                <RowDefinition Height="150"/>
            </Grid.RowDefinitions>

            <Grid Grid.Row="0" Margin="0,0,0,26">
                <Grid.ColumnDefinitions>
                    <ColumnDefinition Width="*"/>
                    <ColumnDefinition Width="Auto"/>
                    <ColumnDefinition Width="Auto"/>
                    <ColumnDefinition Width="Auto"/>
                </Grid.ColumnDefinitions>
                <StackPanel>
                    <TextBlock Text="SROCK CONTROL CENTER" Foreground="#FAFAFA" FontSize="30" FontWeight="Black" FontFamily="Consolas"/>
                    <TextBlock Text="Local service, secure proxy, and public access management" Foreground="#A1A1AA" FontSize="14" Margin="2,8,0,0"/>
                </StackPanel>
                <Button x:Name="StartAllButton" Grid.Column="1" Content="One-Click Online" Style="{StaticResource HeroButton}" Width="180" Margin="0,0,12,0"/>
                <Button x:Name="StopAllButton" Grid.Column="2" Content="One-Click Offline" Style="{StaticResource HeroDangerButton}" Width="180" Margin="0,0,18,0"/>
                <Border Grid.Column="3" Padding="14,8" CornerRadius="18" BorderBrush="#2A2A30" BorderThickness="1" Background="#101014">
                    <StackPanel Orientation="Horizontal">
                        <Ellipse Width="8" Height="8" Fill="#22C55E" Margin="0,0,8,0"/>
                        <TextBlock x:Name="ClockText" Text="READY" Foreground="#D4D4D8" FontFamily="Consolas"/>
                    </StackPanel>
                </Border>
            </Grid>

            <Grid Grid.Row="1" Margin="0,0,0,24">
                <Grid.ColumnDefinitions>
                    <ColumnDefinition Width="*"/>
                    <ColumnDefinition Width="*"/>
                    <ColumnDefinition Width="*"/>
                </Grid.ColumnDefinitions>

                <Border Grid.Column="0" Background="{StaticResource PanelBrush}" BorderBrush="{StaticResource LineBrush}" BorderThickness="1" CornerRadius="14" Padding="20" Margin="0,0,14,0">
                    <StackPanel>
                        <TextBlock Text="STREAMLIT" Foreground="#A1A1AA" FontFamily="Consolas" FontWeight="Bold"/>
                        <TextBlock x:Name="StreamlitStatus" Text="CHECKING" Foreground="#F59E0B" FontSize="28" FontFamily="Consolas" FontWeight="Black" Margin="0,10,0,2"/>
                        <TextBlock x:Name="StreamlitDetail" Text="Port 8501" Foreground="#A1A1AA"/>
                    </StackPanel>
                </Border>

                <Border Grid.Column="1" Background="{StaticResource PanelBrush}" BorderBrush="{StaticResource LineBrush}" BorderThickness="1" CornerRadius="14" Padding="20" Margin="0,0,14,0">
                    <StackPanel>
                        <TextBlock Text="AUTH PROXY" Foreground="#A1A1AA" FontFamily="Consolas" FontWeight="Bold"/>
                        <TextBlock x:Name="AuthStatus" Text="CHECKING" Foreground="#F59E0B" FontSize="28" FontFamily="Consolas" FontWeight="Black" Margin="0,10,0,2"/>
                        <TextBlock x:Name="AuthDetail" Text="Port 8080" Foreground="#A1A1AA"/>
                    </StackPanel>
                </Border>

                <Border Grid.Column="2" Background="{StaticResource PanelBrush}" BorderBrush="{StaticResource LineBrush}" BorderThickness="1" CornerRadius="14" Padding="20">
                    <StackPanel>
                        <TextBlock Text="LOGIN" Foreground="#A1A1AA" FontFamily="Consolas" FontWeight="Bold"/>
                        <TextBlock x:Name="CredStatus" Text="CHECKING" Foreground="#F59E0B" FontSize="28" FontFamily="Consolas" FontWeight="Black" Margin="0,10,0,2"/>
                        <TextBlock x:Name="CredDetail" Text="Basic Auth" Foreground="#A1A1AA"/>
                    </StackPanel>
                </Border>
            </Grid>

            <ScrollViewer Grid.Row="2" VerticalScrollBarVisibility="Auto" HorizontalScrollBarVisibility="Disabled" Padding="0,0,8,0">
            <Grid>
                <Grid.ColumnDefinitions>
                    <ColumnDefinition Width="1.15*"/>
                    <ColumnDefinition Width="0.85*"/>
                </Grid.ColumnDefinitions>

                <Border Grid.Column="0" Background="{StaticResource PanelBrush2}" BorderBrush="{StaticResource LineBrush}" BorderThickness="1" CornerRadius="16" Padding="22" Margin="0,0,18,0">
                    <StackPanel>
                        <TextBlock Text="Streamlit Service" Foreground="#F4F4F5" FontSize="20" FontWeight="Bold"/>
                        <TextBlock Text="Start or restart the Streamlit process. Open Browser only opens the local URL." Foreground="#A1A1AA" Margin="0,6,0,20"/>

                        <WrapPanel>
                            <Button x:Name="StartStockButton" Content="Start Streamlit" Style="{StaticResource AccentButton}" Width="160"/>
                            <Button x:Name="RestartStockButton" Content="Restart Streamlit" Width="170"/>
                            <Button x:Name="OpenStreamlitButton" Content="Open Browser" Width="150"/>
                        </WrapPanel>

                        <Separator Background="#2A2A30" Margin="0,18,0,22"/>

                        <TextBlock Text="Secure Gateway" Foreground="#F4F4F5" FontSize="20" FontWeight="Bold"/>
                        <TextBlock Text="Public traffic should enter through the protected proxy, not Streamlit directly." Foreground="#A1A1AA" Margin="0,6,0,20"/>

                        <WrapPanel>
                            <Button x:Name="StartAuthButton" Content="Start Auth Proxy" Style="{StaticResource AccentButton}" Width="160"/>
                            <Button x:Name="StopAuthButton" Content="Stop Auth Proxy" Style="{StaticResource DangerButton}" Width="160"/>
                            <Button x:Name="RestartAuthButton" Content="Restart Auth Proxy" Width="170"/>
                            <Button x:Name="OpenProtectedButton" Content="Open Protected URL" Width="180"/>
                            <Button x:Name="TestAuthButton" Content="Test Basic Auth" Width="160"/>
                        </WrapPanel>

                        <Separator Background="#2A2A30" Margin="0,18,0,22"/>

                        <TextBlock Text="External Access" Foreground="#F4F4F5" FontSize="20" FontWeight="Bold"/>
                        <TextBlock Text="Tailscale Funnel targets the protected proxy on port 8080." Foreground="#A1A1AA" Margin="0,6,0,20"/>

                        <WrapPanel>
                            <Button x:Name="StartFunnelButton" Content="Start Funnel" Style="{StaticResource AccentButton}" Width="140"/>
                            <Button x:Name="StopFunnelButton" Content="Stop Funnel" Style="{StaticResource DangerButton}" Width="140"/>
                            <Button x:Name="FunnelStatusButton" Content="Funnel Status" Width="150"/>
                            <Button x:Name="StopPublicButton" Content="Stop All Public Access" Style="{StaticResource DangerButton}" Width="200"/>
                        </WrapPanel>
                    </StackPanel>
                </Border>

                <Border Grid.Column="1" Background="{StaticResource PanelBrush2}" BorderBrush="{StaticResource LineBrush}" BorderThickness="1" CornerRadius="16" Padding="22">
                    <StackPanel>
                        <TextBlock Text="Account Manager" Foreground="#F4F4F5" FontSize="20" FontWeight="Bold"/>
                        <TextBlock Text="Add a user or reset a user's Basic Auth password." Foreground="#A1A1AA" Margin="0,6,0,20"/>

                        <TextBlock Text="Username" Foreground="#A1A1AA" Margin="0,0,0,6"/>
                        <TextBox x:Name="UserInput" Text="srock" Margin="0,0,0,14"/>

                        <TextBlock Text="Password" Foreground="#A1A1AA" Margin="0,0,0,6"/>
                        <PasswordBox x:Name="PassInput" Margin="0,0,0,18"/>

                        <WrapPanel>
                            <Button x:Name="AddLoginButton" Content="Add Login" Style="{StaticResource AccentButton}" Width="130"/>
                            <Button x:Name="ResetLoginButton" Content="Reset Login" Width="130"/>
                        </WrapPanel>

                        <WrapPanel Margin="0,10,0,0">
                            <Button x:Name="ListUsersButton" Content="List Users" Width="120"/>
                            <Button x:Name="ShowLastLoginButton" Content="Show Last Login" Width="150"/>
                        </WrapPanel>

                        <Separator Background="#2A2A30" Margin="0,22,0,22"/>

                        <TextBlock Text="Utilities" Foreground="#F4F4F5" FontSize="20" FontWeight="Bold"/>
                        <WrapPanel Margin="0,18,0,0">
                            <Button x:Name="OpenProjectButton" Content="Project Folder" Width="140"/>
                            <Button x:Name="OpenLogsButton" Content="Runtime Logs" Width="140"/>
                            <Button x:Name="RefreshButton" Content="Refresh" Width="110"/>
                        </WrapPanel>
                    </StackPanel>
                </Border>
            </Grid>
            </ScrollViewer>

            <Border Grid.Row="3" Background="#070709" BorderBrush="#24242A" BorderThickness="1" CornerRadius="14" Padding="16" Margin="0,24,0,0">
                <Grid>
                    <Grid.RowDefinitions>
                        <RowDefinition Height="Auto"/>
                        <RowDefinition Height="*"/>
                    </Grid.RowDefinitions>
                    <TextBlock Text="Activity Log" Foreground="#A1A1AA" FontFamily="Consolas" FontWeight="Bold" Margin="0,0,0,10"/>
                    <TextBox x:Name="LogBox" Grid.Row="1" Background="#070709" BorderThickness="0" Foreground="#D4D4D8" FontFamily="Consolas" FontSize="12" IsReadOnly="True" TextWrapping="Wrap" VerticalScrollBarVisibility="Auto" AcceptsReturn="True"/>
                </Grid>
            </Border>
        </Grid>
    </Grid>
</Window>
"@

$reader = New-Object System.Xml.XmlNodeReader ([xml]$xaml)
$window = [Windows.Markup.XamlReader]::Load($reader)

function Find-Control {
    param([string]$Name)
    return $window.FindName($Name)
}

$controls = @{}
@(
    "ClockText",
    "StreamlitStatus", "StreamlitDetail",
    "AuthStatus", "AuthDetail",
    "CredStatus", "CredDetail",
    "LogBox", "UserInput", "PassInput",
    "StartStockButton", "RestartStockButton", "OpenStreamlitButton",
    "StartAllButton", "StopAllButton",
    "StartAuthButton", "StopAuthButton", "RestartAuthButton", "OpenProtectedButton", "TestAuthButton",
    "StartFunnelButton", "StopFunnelButton", "FunnelStatusButton", "StopPublicButton",
    "AddLoginButton", "ResetLoginButton", "ListUsersButton", "ShowLastLoginButton",
    "OpenProjectButton", "OpenLogsButton", "RefreshButton"
) | ForEach-Object {
    $controls[$_] = Find-Control $_
}

function New-Brush {
    param([string]$Hex)
    return [System.Windows.Media.BrushConverter]::new().ConvertFromString($Hex)
}

$BrushGreen = New-Brush "#22C55E"
$BrushRed = New-Brush "#F87171"
$BrushAmber = New-Brush "#F59E0B"
$BrushMuted = New-Brush "#A1A1AA"

if (Test-Path $OpsLogFile) {
    $script:OpsLogLinesRead = @(Get-Content $OpsLogFile -ErrorAction SilentlyContinue).Count
}

function Write-Log {
    param([string]$Message)
    $stamp = Get-Date -Format "HH:mm:ss"
    $controls.LogBox.AppendText("[$stamp] $Message`r`n")
    $controls.LogBox.ScrollToEnd()
}

function Set-StatusLabel {
    param(
        [object]$StatusControl,
        [object]$DetailControl,
        [bool]$Running,
        [string]$Detail
    )
    if ($Running) {
        $StatusControl.Text = "RUNNING"
        $StatusControl.Foreground = $BrushGreen
    } else {
        $StatusControl.Text = "STOPPED"
        $StatusControl.Foreground = $BrushRed
    }
    $DetailControl.Text = $Detail
}

function Refresh-Status {
    Pull-OperationLog
    $streamlitPid = Get-ListenerPid $StreamlitPort
    $authPid = Get-ListenerPid $AuthPort
    Set-StatusLabel $controls.StreamlitStatus $controls.StreamlitDetail ([bool]$streamlitPid) (Get-ProcessLabel $StreamlitPort)
    Set-StatusLabel $controls.AuthStatus $controls.AuthDetail ([bool]$authPid) (Get-ProcessLabel $AuthPort)

    if (Test-Path $CredentialFile) {
        $controls.CredStatus.Text = "CONFIGURED"
        $controls.CredStatus.Foreground = $BrushGreen
        $controls.CredDetail.Text = "Credentials file exists"
    } else {
        $controls.CredStatus.Text = "MISSING"
        $controls.CredStatus.Foreground = $BrushAmber
        $controls.CredDetail.Text = "Create a login before public access"
    }
    $controls.ClockText.Text = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
}

function Invoke-UiAction {
    param([scriptblock]$Action)
    try {
        $result = & $Action
        if ($result) {
            Write-Log $result
        }
    } catch {
        Write-Log "ERROR: $($_.Exception.Message)"
    }
    Refresh-Status
}

function Add-Handler {
    param([string]$Name, [scriptblock]$Action)
    $clickAction = $Action
    $controls[$Name].Add_Click({
        Invoke-UiAction $clickAction
    }.GetNewClosure())
}

Add-Handler "StartStockButton" { Start-StockTool }
Add-Handler "RestartStockButton" { Restart-StockTool }
Add-Handler "OpenStreamlitButton" { Open-Url "http://127.0.0.1:$StreamlitPort"; "Opened Streamlit." }
Add-Handler "StartAllButton" { Invoke-OneClickOnline }
Add-Handler "StopAllButton" { Invoke-OneClickOffline }

Add-Handler "StartAuthButton" { Start-AuthProxy }
Add-Handler "StopAuthButton" { Stop-AuthProxy }
Add-Handler "RestartAuthButton" { Restart-AuthProxy }
Add-Handler "OpenProtectedButton" { Open-Url "http://127.0.0.1:$AuthPort"; "Opened protected URL." }
Add-Handler "TestAuthButton" { Test-ProtectedEndpoint }

Add-Handler "StartFunnelButton" { Start-Funnel }
Add-Handler "StopFunnelButton" { Stop-Funnel }
Add-Handler "FunnelStatusButton" { Get-FunnelStatus }
Add-Handler "StopPublicButton" {
    $a = Stop-Funnel
    $b = Stop-AuthProxy
    "$a`r`n$b"
}

Add-Handler "AddLoginButton" {
    Set-Credentials "add-user" $controls.UserInput.Text $controls.PassInput.Password
}
Add-Handler "ResetLoginButton" {
    Set-Credentials "reset-user" $controls.UserInput.Text $controls.PassInput.Password
}
Add-Handler "ListUsersButton" { Get-CredentialUsers }
Add-Handler "ShowLastLoginButton" { Read-Credentials }

Add-Handler "OpenProjectButton" { Start-Process $Root; "Opened project folder." }
Add-Handler "OpenLogsButton" { Ensure-Runtime; Start-Process $RuntimeDir; "Opened runtime logs folder." }
Add-Handler "RefreshButton" { Refresh-Status; "Status refreshed." }

$timer = New-Object System.Windows.Threading.DispatcherTimer
$timer.Interval = [TimeSpan]::FromSeconds(5)
$timer.Add_Tick({ Refresh-Status })
$timer.Start()

Refresh-Status
Write-Log "Manager ready."
if ($ValidateOnly) {
    Write-Host "srock_manager WPF validation ok"
    return
}
[void]$window.ShowDialog()
