param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$ConfigB64
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$script:CurrentPhase = 'member_web_start'
$script:Base = $null
$script:MemberProcess = $null
$script:ForcedTermination = $false
$script:AllPassed = $false
$script:CleanupPassed = $false
$script:GracefulDrainProven = $false
$script:DestinationCreated = $false
$script:LastApiStatus = 0
$script:OwnedConsole = $false
$script:ConsoleHandler = $null
$script:ConsoleOwnerPid = 0
$script:ConsoleChildPid = 0
$script:LastStopErrorClass = 'none'
$script:LastConsoleExtraTrusted = $false
$script:TraceStartedTick = [Diagnostics.Stopwatch]::GetTimestamp()
$script:ProcessStreamTasks = @{}

function Emit-Diagnostic {
    param([Parameter(Mandatory = $true)][ValidateSet(
        'config_ok', 'binary_done', 'self_test_done', 'web_start_enter',
        'console_prepare', 'console_detached', 'console_allocated',
        'console_handler_installed', 'console_verified', 'web_process_started',
        'console_ready', 'pipes_ready', 'web_http_ready', 'web_start_return',
        'member_login_enter', 'member_login_done', 'preview_enter',
        'validate_enter', 'join_enter', 'file_hash_enter', 'shutdown_enter',
        'reopen_enter', 'cleanup_enter', 'owner_web_ready', 'owner_login_enter',
        'owner_login_done', 'owner_create_enter', 'owner_create_done',
        'owner_issue_enter', 'owner_issue_done', 'console_release_enter',
        'console_release_done', 'member_web_start_enter', 'member_stop_enter',
        'owner_attach_enter', 'owner_stop_enter', 'member_reopen_enter',
        'reopen_login_enter', 'reopen_login_done', 'reopen_membership_enter',
        'reopen_membership_done', 'reopen_checks',
        'keepalive_enter', 'keepalive_done',
        'artifact_download_enter', 'artifact_download_done', 'artifact_hash_done',
        'ctrlc_sent', 'exit_wait_enter', 'exit_observed', 'streams_drained',
        'console_released', 'stop_ctrlc_failed', 'stop_exit_timeout',
        'stop_stream_timeout', 'stop_release_failed', 'stop_done',
        'console_test_enter', 'console_test_not_owned', 'console_test_missing',
        'console_test_exited', 'console_test_ready', 'console_test_mismatch',
        'console_test_extra_trusted', 'console_test_extra_unknown', 'console_test_error'
    )][string]$Stage,
        [int]$Count = -1
    )
    # Write directly to the remoting stdout stream.  Emitting a success-stream
    # object from a helper would become part of that helper's return value and
    # could change boolean/process assignments in the caller.
    $elapsedMs = [int64](([Diagnostics.Stopwatch]::GetTimestamp() - $script:TraceStartedTick) * 1000 / [Diagnostics.Stopwatch]::Frequency)
    $line = "FTRACE|stage=$Stage"
    if ($Count -ge 0) { $line += "|count=$Count" }
    $line += "|elapsed_ms=$elapsedMs"
    [Console]::Out.WriteLine($line)
}

function Emit-Phase {
    param(
        [Parameter(Mandatory = $true)][string]$Phase,
        [Parameter(Mandatory = $true)][bool]$Ok,
        [string]$Hash = '',
        [long]$Size = -1,
        [bool]$Forced = $false,
        [ValidateSet('ctrl_c', 'none')][string]$Signal = '',
        [string]$ErrorClass = ''
    )
    $value = if ($Ok) { 'true' } else { 'false' }
    $line = "FROLE|phase=$Phase|ok=$value"
    if ($Hash -match '^[0-9a-f]{64}$') { $line += "|hash=$Hash" }
    if ($Size -ge 0) { $line += "|size=$Size" }
    if ($Forced) { $line += '|forced=true' }
    if ($Signal -match '^(ctrl_c|none)$') { $line += "|signal=$Signal" }
    if (-not $Ok -and $ErrorClass -match '^[a-z0-9_]+$') { $line += "|error_class=$ErrorClass" }
    Write-Output $line
}

function Decode-Config {
    param([string]$Encoded)
    $compressedStream = $null
    $gzipStream = $null
    $decodedStream = $null
    try {
        $bytes = [Convert]::FromBase64String($Encoded)
        $compressedStream = [IO.MemoryStream]::new($bytes)
        $gzipStream = [IO.Compression.GzipStream]::new(
            $compressedStream,
            [IO.Compression.CompressionMode]::Decompress
        )
        $decodedStream = [IO.MemoryStream]::new()
        $gzipStream.CopyTo($decodedStream)
        $json = [Text.Encoding]::UTF8.GetString($decodedStream.ToArray())
        $value = $json | ConvertFrom-Json
        if ($null -eq $value) { throw 'config' }
        Emit-Diagnostic 'config_ok'
        return $value
    } catch {
        throw 'config'
    } finally {
        if ($null -ne $gzipStream) { $gzipStream.Dispose() }
        if ($null -ne $compressedStream) { $compressedStream.Dispose() }
        if ($null -ne $decodedStream) { $decodedStream.Dispose() }
    }
}

function Assert-Config {
    param($Value)
    $required = @('artifact_url', 'artifact_sha256', 'artifact_size', 'owner_base_uri', 'share_key', 'destination_root', 'expected_file_hash', 'expected_file_name', 'expected_permission')
    foreach ($name in $required) {
        $item = [string]$Value.$name
        if ([string]::IsNullOrWhiteSpace($item)) { throw 'config' }
    }
    if ([string]$Value.artifact_sha256 -notmatch '^[0-9a-f]{64}$') { throw 'config' }
    if ([string]$Value.artifact_size -notmatch '^[1-9][0-9]*$') { throw 'config' }
    if ([string]$Value.expected_file_hash -notmatch '^[0-9a-f]{64}$') { throw 'config' }
    if ([string]$Value.destination_root -notmatch '^[A-Za-z]:\\[^\x00\r\n]+$') { throw 'config' }
    if ([string]$Value.expected_file_name -notmatch '^[A-Za-z0-9._-]{1,128}$') { throw 'config' }
    if ([string]$Value.expected_permission -notmatch '^(read_only|read_write)$') { throw 'config' }
    if ($null -eq $Value.keepalive_seconds) { $Value | Add-Member -NotePropertyName keepalive_seconds -NotePropertyValue 0 }
    if ([string]$Value.keepalive_seconds -notmatch '^[0-9]{1,3}$' -or [int]$Value.keepalive_seconds -gt 900) { throw 'config' }
    if ([string]$Value.owner_base_uri -notmatch '^https?://[^\s/]+(?::[0-9]{1,5})?$') { throw 'config' }
    if ([string]$Value.artifact_url -notmatch '^https?://[^\s/]+(?::[0-9]{1,5})?/qsync-f-[A-Za-z0-9_-]+$') { throw 'config' }
}

function Get-FreeTcpPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try { $listener.Start(); return [int]$listener.LocalEndpoint.Port }
    finally { $listener.Stop() }
}

function Get-PrivateProfile {
    param([string]$Root)
    $profile = Join-Path $Root 'profile'
    New-Item -ItemType Directory -Path $profile -Force:$false | Out-Null
    foreach ($name in @('home', 'tmp', 'config', 'cache', 'state', 'data')) {
        New-Item -ItemType Directory -Path (Join-Path $profile $name) -Force:$false | Out-Null
    }
    return $profile
}

function Initialize-OwnedConsoleApi {
    if ($null -ne ('QsyncOwnedConsole' -as [type])) { return $true }
    try {
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

public static class QsyncOwnedConsole
{
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool AllocConsole();

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool FreeConsole();

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool GenerateConsoleCtrlEvent(uint controlEvent, uint processGroupId);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetConsoleCtrlHandler(HandlerRoutine handler, bool add);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint GetConsoleProcessList([Out] uint[] processList, uint processCount);

    [UnmanagedFunctionPointer(CallingConvention.Winapi)]
    private delegate bool HandlerRoutine(uint controlType);

    private static readonly HandlerRoutine OwnerHandler = HandleOwnerControl;

    private static bool HandleOwnerControl(uint controlType)
    {
        // Consume only CTRL+C. Other console events keep their normal
        // process behavior, and this static callback never enters a
        // PowerShell runspace from a native signal thread.
        return controlType == 0;
    }

    public static bool InstallOwnerHandler()
    {
        return SetConsoleCtrlHandler(OwnerHandler, true);
    }

    public static bool RemoveOwnerHandler()
    {
        return SetConsoleCtrlHandler(OwnerHandler, false);
    }

    public static uint[] GetAttachedProcessIds()
    {
        uint[] initial = new uint[1];
        uint count = GetConsoleProcessList(initial, (uint)initial.Length);
        if (count == 0) return new uint[0];
        uint[] ids = new uint[(int)count];
        uint actual = GetConsoleProcessList(ids, count);
        if (actual == 0) return new uint[0];
        if (actual < ids.Length) Array.Resize(ref ids, (int)actual);
        return ids;
    }
}
'@
        return $true
    } catch { return $false }
}

function Get-OwnedConsolePids {
    if (-not (Initialize-OwnedConsoleApi)) { return @() }
    try { return @([QsyncOwnedConsole]::GetAttachedProcessIds()) } catch { return @() }
}

function Ensure-OwnedConsole {
    if ($script:OwnedConsole) { return $true }
    if (-not (Initialize-OwnedConsoleApi)) { return $false }
    try {
        Emit-Diagnostic 'console_prepare'
        # WinRM hosts commonly attach PowerShell to a shared helper console.
        # Detach this process before allocating the test-owned console so a
        # later CTRL+C cannot reach unrelated WinRM processes.
        if ((Get-OwnedConsolePids).Count -ne 0) {
            if (-not [QsyncOwnedConsole]::FreeConsole()) { return $false }
            if ((Get-OwnedConsolePids).Count -ne 0) { return $false }
            Emit-Diagnostic 'console_detached'
        }
        if (-not [QsyncOwnedConsole]::AllocConsole()) { return $false }
        Emit-Diagnostic 'console_allocated'
        if (-not [QsyncOwnedConsole]::InstallOwnerHandler()) {
            [QsyncOwnedConsole]::FreeConsole() | Out-Null
            return $false
        }
        Emit-Diagnostic 'console_handler_installed'
        $ownerPid = [uint32]$PID
        $ids = Get-OwnedConsolePids
        if ($ids.Count -ne 1 -or -not ($ids -contains $ownerPid)) {
            [QsyncOwnedConsole]::RemoveOwnerHandler() | Out-Null
            [QsyncOwnedConsole]::FreeConsole() | Out-Null
            return $false
        }
        Emit-Diagnostic 'console_verified'
        $script:ConsoleHandler = $true
        $script:ConsoleOwnerPid = $ownerPid
        $script:ConsoleChildPid = 0
        $script:OwnedConsole = $true
        return $true
    } catch { return $false }
}

function Release-OwnedConsole {
    if (-not $script:OwnedConsole) { return $true }
    $ok = $true
    try {
        if ($null -ne $script:ConsoleHandler -and -not [QsyncOwnedConsole]::RemoveOwnerHandler()) {
            $ok = $false
        }
    } catch { $ok = $false }
    $freed = $false
    try { $freed = [QsyncOwnedConsole]::FreeConsole() } catch { $freed = $false }
    if ($freed) {
        $script:OwnedConsole = $false
        $script:ConsoleHandler = $null
        $script:ConsoleOwnerPid = 0
        $script:ConsoleChildPid = 0
    } else {
        $ok = $false
    }
    return $ok
}

function Test-OwnedConsoleProcess {
    param($Process)
    $script:LastConsoleExtraTrusted = $false
    Emit-Diagnostic 'console_test_enter'
    if (-not $script:OwnedConsole) {
        $script:LastStopErrorClass = 'console_control_failed'
        Emit-Diagnostic 'console_test_not_owned'
        return $false
    }
    if ($null -eq $Process) {
        $script:LastStopErrorClass = 'console_control_failed'
        Emit-Diagnostic 'console_test_missing'
        return $false
    }
    try {
        if ($Process.HasExited) {
            $script:LastStopErrorClass = 'process_exited'
            Emit-Diagnostic 'console_test_exited'
            return $false
        }
        $ids = Get-OwnedConsolePids
        $ownerPid = [uint32]$script:ConsoleOwnerPid
        $childPid = [uint32]$Process.Id
        if ($ids.Count -eq 2 -and ($ids -contains $ownerPid) -and ($ids -contains $childPid)) {
            Emit-Diagnostic 'console_test_ready' -Count $ids.Count
            return $true
        }
        $extraIds = @($ids | Where-Object { $_ -ne $ownerPid -and $_ -ne $childPid })
        if ($extraIds.Count -gt 0) {
            $allTrustedTestOwned = $true
            $systemRoot = [Environment]::GetEnvironmentVariable('SystemRoot')
            $systemPowerShell = Join-Path $systemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
            foreach ($extraId in $extraIds) {
                try {
                    $extraProcess = Get-Process -Id ([int]$extraId) -ErrorAction Stop
                    $imagePath = [string]$extraProcess.MainModule.FileName
                    $processInfo = Get-CimInstance -ClassName Win32_Process -Filter ("ProcessId = {0}" -f [int]$extraId) -ErrorAction Stop
                    $parentId = [int]$processInfo.ParentProcessId
                    if (-not [String]::Equals($imagePath, $systemPowerShell, [StringComparison]::OrdinalIgnoreCase) -or
                        ($parentId -ne [int]$ownerPid -and $parentId -ne [int]$childPid)) {
                        $allTrustedTestOwned = $false
                    }
                } catch {
                    $allTrustedTestOwned = $false
                }
            }
            if ($allTrustedTestOwned) {
                $script:LastConsoleExtraTrusted = $true
                Emit-Diagnostic 'console_test_extra_trusted' -Count $extraIds.Count
            } else {
                Emit-Diagnostic 'console_test_extra_unknown' -Count $extraIds.Count
            }
        }
        $script:LastStopErrorClass = 'console_control_failed'
        Emit-Diagnostic 'console_test_mismatch' -Count $ids.Count
        return $false
    } catch {
        $script:LastStopErrorClass = 'console_control_failed'
        Emit-Diagnostic 'console_test_error'
        return $false
    }
}

function Send-OwnedCtrlC {
    param($Process)
    if (Test-OwnedConsoleProcess $Process) {
        try {
            # CTRL_C_EVENT with process group 0 is broadcast to the dedicated
            # console.  The owner handler ignores it; only the checked child
            # is the other process attached to that console.
            return [QsyncOwnedConsole]::GenerateConsoleCtrlEvent(0, 0)
        } catch { return $false }
    }
    # A short-lived helper, or a process that cannot be classified during a
    # transient process-list race, may remain on the console after the child
    # is ready.  Resample for a short fixed budget.  An unknown process is
    # never included in a broadcast; only Test-OwnedConsoleProcess returning
    # true permits the signal.
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        Start-Sleep -Milliseconds 50
        if ($Process.HasExited) { return $false }
        if (Test-OwnedConsoleProcess $Process) {
            try { return [QsyncOwnedConsole]::GenerateConsoleCtrlEvent(0, 0) } catch { return $false }
        }
    }
    return $false
}

function New-ProcessInfo {
    param(
        [string]$Executable,
        [string]$Arguments,
        [string]$WorkingDirectory,
        [string]$Profile,
        [switch]$UseOwnedConsole
    )
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $Executable
    $info.Arguments = $Arguments
    $info.WorkingDirectory = $WorkingDirectory
    $info.UseShellExecute = $false
    $info.CreateNoWindow = -not $UseOwnedConsole.IsPresent
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    $info.EnvironmentVariables.Clear()
    foreach ($name in @('PATH', 'SystemRoot', 'WINDIR', 'ComSpec', 'PATHEXT')) {
        $value = [Environment]::GetEnvironmentVariable($name)
        if (-not [string]::IsNullOrWhiteSpace($value)) { $info.EnvironmentVariables[$name] = $value }
    }
    $qsyncProfileHome = Join-Path $Profile 'home'
    $tmp = Join-Path $Profile 'tmp'
    $info.EnvironmentVariables['HOME'] = $qsyncProfileHome
    $info.EnvironmentVariables['USERPROFILE'] = $qsyncProfileHome
    $info.EnvironmentVariables['XDG_CONFIG_HOME'] = (Join-Path $Profile 'config')
    $info.EnvironmentVariables['XDG_CACHE_HOME'] = (Join-Path $Profile 'cache')
    $info.EnvironmentVariables['XDG_STATE_HOME'] = (Join-Path $Profile 'state')
    $info.EnvironmentVariables['XDG_DATA_HOME'] = (Join-Path $Profile 'data')
    $info.EnvironmentVariables['TEMP'] = $tmp
    $info.EnvironmentVariables['TMP'] = $tmp
    $info.EnvironmentVariables['RUST_BACKTRACE'] = '0'
    $info.EnvironmentVariables['NO_COLOR'] = '1'
    return $info
}

function Start-DiscardProcessStreams {
    param([Parameter(Mandatory = $true)]$Process)
    try {
        $stdoutTask = $Process.StandardOutput.BaseStream.CopyToAsync([IO.Stream]::Null)
        $stderrTask = $Process.StandardError.BaseStream.CopyToAsync([IO.Stream]::Null)
        $script:ProcessStreamTasks[[int]$Process.Id] = @($stdoutTask, $stderrTask)
        return $true
    } catch { return $false }
}

function Wait-DiscardProcessStreams {
    param(
        [Parameter(Mandatory = $true)]$Process,
        [int]$TimeoutMilliseconds = 5000
    )
    $key = [int]$Process.Id
    if (-not $script:ProcessStreamTasks.ContainsKey($key)) { return $false }
    $ok = $true
    $deadline = [Diagnostics.Stopwatch]::GetTimestamp() + [int64]($TimeoutMilliseconds * [Diagnostics.Stopwatch]::Frequency / 1000)
    foreach ($task in @($script:ProcessStreamTasks[$key])) {
        try {
            $remaining = [int](($deadline - [Diagnostics.Stopwatch]::GetTimestamp()) * 1000 / [Diagnostics.Stopwatch]::Frequency)
            if ($remaining -lt 0 -or -not $task.Wait($remaining)) { $ok = $false }
        } catch { $ok = $false }
    }
    if ($ok) { $script:ProcessStreamTasks.Remove($key) | Out-Null }
    return $ok
}

function Invoke-BinarySelfTest {
    param([string]$Executable, [string]$Profile)
    $info = New-ProcessInfo $Executable 'self-test' $script:Base $Profile
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $info
    if (-not $process.Start()) { return $false }
    # Never inherit child streams into the WinRM transcript.  Copy both
    # streams to Stream.Null with .NET Tasks; PowerShell scriptblock event
    # handlers can execute on a callback thread without a runspace.
    if (-not (Start-DiscardProcessStreams $process)) {
        $script:ForcedTermination = $true
        try { $process.Kill() } catch { }
        try { $process.WaitForExit(5000) } catch { }
        return $false
    }
    if (-not $process.WaitForExit(60000)) {
        $script:ForcedTermination = $true
        try { $process.Kill() } catch { }
        try { $process.WaitForExit(5000) } catch { }
        Wait-DiscardProcessStreams $process 5000 | Out-Null
        return $false
    }
    return (Wait-DiscardProcessStreams $process 5000) -and $process.ExitCode -eq 0
}

function Start-WebProcess {
    param([string]$Executable, [string]$DataDirectory, [int]$Port, [string]$Profile)
    Emit-Diagnostic 'web_start_enter'
    if (-not (Ensure-OwnedConsole)) { return $null }
    $arguments = ('web --bind "127.0.0.1:{0}" --data-dir "{1}"' -f $Port, $DataDirectory)
    $info = New-ProcessInfo $Executable $arguments $script:Base $Profile -UseOwnedConsole
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $info
    if (-not $process.Start()) {
        Release-OwnedConsole | Out-Null
        return $null
    }
    # Publish the owned handle before readiness checks.  A failed Kill/Wait
    # must remain reachable by the outer cleanup path; otherwise an unknown
    # child could be detached and the run would falsely look cleaned up.
    $script:MemberProcess = $process
    if (-not (Start-DiscardProcessStreams $process)) {
        $script:ForcedTermination = $true
        try { $process.Kill() } catch { }
        try { $process.WaitForExit(5000) } catch { }
        return $null
    }
    Emit-Diagnostic 'web_process_started'
    $consoleReady = $false
    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        if (Test-OwnedConsoleProcess $process) {
            $consoleReady = $true
            break
        }
        if ($process.HasExited) { break }
        Start-Sleep -Milliseconds 50
    }
    if (-not $consoleReady) {
        $stopped = $false
        try {
            if ($process.HasExited) { $stopped = $true }
            else { $script:ForcedTermination = $true; $process.Kill(); $stopped = $process.WaitForExit(5000) }
        } catch { $stopped = $false }
        if ($stopped) {
            $stopped = Wait-DiscardProcessStreams $process 5000
            $script:MemberProcess = $null
            Release-OwnedConsole | Out-Null
        }
        return $null
    }
    Emit-Diagnostic 'console_ready'
    $script:ConsoleChildPid = [uint32]$process.Id
    # The discard tasks started above continue draining both streams while the
    # long-running web process is alive.
    Emit-Diagnostic 'pipes_ready'
    for ($attempt = 0; $attempt -lt 150; $attempt++) {
        if ($process.HasExited) {
            if (-not (Wait-DiscardProcessStreams $process 5000)) { return $null }
            $script:MemberProcess = $null
            Release-OwnedConsole | Out-Null
            return $null
        }
        if (Test-Path -LiteralPath (Join-Path $DataDirectory 'admin-token')) {
            try {
                $null = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/" -TimeoutSec 2
                Emit-Diagnostic 'web_http_ready'
                Emit-Diagnostic 'web_start_return'
                return $process
            } catch { }
        }
        Start-Sleep -Milliseconds 200
    }
    $stopped = $false
    try {
        if ($process.HasExited) { $stopped = $true }
        else { $script:ForcedTermination = $true; $process.Kill(); $stopped = $process.WaitForExit(5000) }
    } catch { $stopped = $false }
    if ($stopped) {
        $stopped = Wait-DiscardProcessStreams $process 5000
        $script:MemberProcess = $null
        Release-OwnedConsole | Out-Null
    }
    return $null
}

function Stop-WebProcess {
    param($Process)
    if ($null -eq $Process) { return $true }
    $script:LastStopErrorClass = 'none'
    try {
        if ($Process.HasExited) {
            $script:LastStopErrorClass = 'process_exited'
            $streamsDrained = Wait-DiscardProcessStreams $Process 5000
            if (-not $streamsDrained) {
                $script:LastStopErrorClass = 'cleanup_incomplete'
                Emit-Diagnostic 'stop_stream_timeout'
            } else {
                Emit-Diagnostic 'streams_drained'
            }
            $released = Release-OwnedConsole
            if (-not $released) {
                $script:LastStopErrorClass = 'cleanup_incomplete'
                Emit-Diagnostic 'stop_release_failed'
            }
            $script:GracefulDrainProven = $false
            Emit-Diagnostic 'stop_done'
            return $false
        }
        if (-not (Send-OwnedCtrlC $Process)) {
            if ($script:LastStopErrorClass -eq 'none') { $script:LastStopErrorClass = 'console_control_failed' }
            Emit-Diagnostic 'stop_ctrlc_failed'
            $script:GracefulDrainProven = $false
            Emit-Diagnostic 'stop_done'
            return $false
        }
        Emit-Diagnostic 'ctrlc_sent'
        Emit-Diagnostic 'exit_wait_enter'
        if (-not $Process.WaitForExit(30000)) {
            $script:LastStopErrorClass = 'timeout'
            Emit-Diagnostic 'stop_exit_timeout'
            $script:ForcedTermination = $true
            try { $Process.Kill() } catch { }
            if (-not $Process.WaitForExit(5000)) {
                $script:GracefulDrainProven = $false
                Emit-Diagnostic 'stop_done'
                return $false
            }
            if (-not (Wait-DiscardProcessStreams $Process 5000)) {
                $script:LastStopErrorClass = 'cleanup_incomplete'
                Emit-Diagnostic 'stop_stream_timeout'
            }
            if (-not (Release-OwnedConsole)) { Emit-Diagnostic 'stop_release_failed' }
            $script:GracefulDrainProven = $false
            Emit-Diagnostic 'stop_done'
            return $false
        }
        Emit-Diagnostic 'exit_observed'
        $exitCode = $Process.ExitCode
        if ($exitCode -ne 0) { $script:LastStopErrorClass = 'process_exited' }
        $streamsDrained = Wait-DiscardProcessStreams $Process 5000
        if ($streamsDrained) { Emit-Diagnostic 'streams_drained' } else {
            $script:LastStopErrorClass = 'cleanup_incomplete'
            Emit-Diagnostic 'stop_stream_timeout'
        }
        $released = Release-OwnedConsole
        if ($released) { Emit-Diagnostic 'console_released' } else {
            $script:LastStopErrorClass = 'cleanup_incomplete'
            Emit-Diagnostic 'stop_release_failed'
        }
        $graceful = $streamsDrained -and $released -and $exitCode -eq 0
        $script:GracefulDrainProven = $graceful
        Emit-Diagnostic 'stop_done'
        return $graceful
    } catch {
        if ($script:LastStopErrorClass -eq 'none') { $script:LastStopErrorClass = 'console_control_failed' }
        Emit-Diagnostic 'stop_done'
        return $false
    }
}

function New-ApiClient {
    param([string]$BaseUri, [string]$DataDirectory)
    $tokenPath = Join-Path $DataDirectory 'admin-token'
    if (-not (Test-Path -LiteralPath $tokenPath)) { return $null }
    try { $token = (Get-Content -LiteralPath $tokenPath -Raw).Trim() } catch { return $null }
    if ([string]::IsNullOrWhiteSpace($token)) { return $null }
    $session = [Microsoft.PowerShell.Commands.WebRequestSession]::new()
    $body = @{ token = $token } | ConvertTo-Json -Compress
    try {
        $response = Invoke-WebRequest -UseBasicParsing -WebSession $session -Method Post `
            -Uri "$BaseUri/api/v1/session" -Headers @{ Origin = $BaseUri } `
            -ContentType 'application/json' -Body $body -TimeoutSec 30
        $value = $response.Content | ConvertFrom-Json
        if ([int]$response.StatusCode -ne 200 -or [string]::IsNullOrWhiteSpace([string]$value.csrf_token)) { return $null }
        return [pscustomobject]@{ BaseUri = $BaseUri; Session = $session; Csrf = [string]$value.csrf_token }
    } catch { return $null }
}

function Invoke-JsonApi {
    param(
        [Parameter(Mandatory = $true)]$Client,
        [Parameter(Mandatory = $true)][ValidateSet('Get', 'Post')][string]$Method,
        [Parameter(Mandatory = $true)][string]$Path,
        [string]$Body,
        [switch]$Mutation
    )
    $headers = @{ Accept = 'application/json' }
    if ($Mutation) {
        $headers['Origin'] = $Client.BaseUri
        $headers['x-deltaweave-csrf'] = $Client.Csrf
    }
    try {
        $params = @{
            UseBasicParsing = $true
            WebSession = $Client.Session
            Method = $Method
            Uri = "$($Client.BaseUri)$Path"
            Headers = $headers
            TimeoutSec = 90
        }
        # PowerShell binds the optional Body parameter to its empty default
        # even when a GET caller omits it.  Sending that default as a GET body
        # can fail before the request reaches the managed membership route.
        # Only mutation requests may carry the JSON body supplied by the
        # caller; the reopen membership GET is deliberately bodyless.
        if ($Method -eq 'Post' -and $PSBoundParameters.ContainsKey('Body') -and $null -ne $Body) {
            $params['ContentType'] = 'application/json'
            $params['Body'] = $Body
        }
        $response = Invoke-WebRequest @params
        $script:LastApiStatus = [int]$response.StatusCode
        if ([string]::IsNullOrWhiteSpace($response.Content)) { return $null }
        return $response.Content | ConvertFrom-Json
    } catch {
        $script:LastApiStatus = 0
        try {
            if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
                $script:LastApiStatus = [int]$_.Exception.Response.StatusCode.value__
            }
        } catch { }
        return $null
    }
}

try {
    $config = Decode-Config $ConfigB64
    Assert-Config $config
    $script:CurrentPhase = 'binary_verification'
    $destination = [string]$config.destination_root
    $destinationParent = Split-Path -Parent $destination
    $baseName = Split-Path -Leaf $destinationParent
    if ($baseName -notmatch '^DeltaWeave-QSync-F-[0-9a-f]{32}$') { throw 'namespace' }
    $script:Base = $destinationParent
    if (Test-Path -LiteralPath $script:Base) { throw 'collision' }
    New-Item -ItemType Directory -Path $script:Base -Force:$false | Out-Null
    if (Test-Path -LiteralPath $destination) { throw 'destination_exists' }
    New-Item -ItemType Directory -Path $destination -Force:$false | Out-Null
    $script:DestinationCreated = $true
    $data = Join-Path $script:Base 'member-data'
    $profile = Get-PrivateProfile $script:Base
    $incoming = Join-Path $script:Base '.incoming.exe'
    Invoke-WebRequest -UseBasicParsing -Uri ([string]$config.artifact_url) -OutFile $incoming -TimeoutSec 120
    $artifactHash = (Get-FileHash -LiteralPath $incoming -Algorithm SHA256).Hash.ToLowerInvariant()
    $artifactSize = (Get-Item -LiteralPath $incoming).Length
    Emit-Phase 'binary_verification' (($artifactHash -eq [string]$config.artifact_sha256) -and ($artifactSize -eq [int64]$config.artifact_size)) $artifactHash $artifactSize
    if ($artifactHash -ne [string]$config.artifact_sha256) { throw 'artifact_hash' }
    $artifact = Join-Path $script:Base 'deltaweave.exe'
    Move-Item -LiteralPath $incoming -Destination $artifact -Force:$false
    Emit-Diagnostic 'binary_done'

    $script:CurrentPhase = 'self_test'
    $selfTestOk = Invoke-BinarySelfTest $artifact $profile
    Emit-Phase 'self_test' $selfTestOk
    if (-not $selfTestOk) { throw 'self_test' }
    Emit-Diagnostic 'self_test_done'

    $script:CurrentPhase = 'member_web_start'
    $memberPort = Get-FreeTcpPort
    $startedProcess = Start-WebProcess $artifact $data $memberPort $profile
    if ($null -ne $startedProcess) { $script:MemberProcess = $startedProcess }
    $memberStarted = $null -ne $script:MemberProcess
    Emit-Phase 'member_web_start' $memberStarted
    if (-not $memberStarted) { throw 'web_start' }
    Emit-Diagnostic 'member_login_enter'
    $member = New-ApiClient "http://127.0.0.1:$memberPort" $data
    $script:CurrentPhase = 'member_login'
    $loggedIn = $null -ne $member
    Emit-Phase 'member_login' $loggedIn
    if (-not $loggedIn) { throw 'login' }
    Emit-Diagnostic 'member_login_done'

    $shareKey = [string]$config.share_key
    $previewBody = @{ request_id = 'qsync-f-winrm-preview'; key = $shareKey } | ConvertTo-Json -Compress
    $script:CurrentPhase = 'local_preview'
    Emit-Diagnostic 'preview_enter'
    $preview = Invoke-JsonApi $member Post '/api/v1/shares/preview' $previewBody -Mutation
    $previewOk = $script:LastApiStatus -eq 200 -and $null -ne $preview -and $preview.signature_valid -eq $true
    Emit-Phase 'local_preview' $previewOk
    if (-not $previewOk) { throw 'preview' }

    $validateBody = @{ request_id = 'qsync-f-winrm-validate'; key = $shareKey } | ConvertTo-Json -Compress
    $script:CurrentPhase = 'online_validate'
    Emit-Diagnostic 'validate_enter'
    $validated = Invoke-JsonApi $member Post '/api/v1/shares/validate' $validateBody -Mutation
    $validateOk = $script:LastApiStatus -eq 200 -and $null -ne $validated -and $validated.signature_valid -eq $true
    Emit-Phase 'online_validate' $validateOk
    if (-not $validateOk) { throw 'validate' }

    $joinBody = @{ request_id = 'qsync-f-winrm-join'; key = $shareKey; destination_root = $destination } | ConvertTo-Json -Compress
    $script:CurrentPhase = 'member_join'
    Emit-Diagnostic 'join_enter'
    $joined = Invoke-JsonApi $member Post '/api/v1/shares/join' $joinBody -Mutation
    $shareId = if ($null -ne $joined) { [string]$joined.share_id } else { '' }
    $joinOk = ($script:LastApiStatus -eq 200 -or $script:LastApiStatus -eq 202) -and $shareId -match '^[0-9a-f]{64}$'
    Emit-Phase 'member_join' $joinOk
    if (-not $joinOk) { throw 'join' }

    $target = Join-Path $destination ([string]$config.expected_file_name)
    $script:CurrentPhase = 'file_hash'
    Emit-Diagnostic 'file_hash_enter'
    $fileHash = ''
    $fileOk = $false
    for ($attempt = 0; $attempt -lt 180; $attempt++) {
        if (Test-Path -LiteralPath $target -PathType Leaf) {
            try {
                $fileHash = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant()
                if ($fileHash -eq [string]$config.expected_file_hash) { $fileOk = $true; break }
            } catch { }
        }
        Start-Sleep -Milliseconds 500
    }
    $fileSize = if ($fileOk) { (Get-Item -LiteralPath $target).Length } else { -1 }
    Emit-Phase 'file_hash' $fileOk $fileHash $fileSize
    if (-not $fileOk) { throw 'file_hash' }

    $keepAliveSeconds = [int]$config.keepalive_seconds
    if ($keepAliveSeconds -gt 0) {
        Emit-Diagnostic 'keepalive_enter' -Count $keepAliveSeconds
        $keepAliveDeadline = [Diagnostics.Stopwatch]::GetTimestamp() + [int64]($keepAliveSeconds * [Diagnostics.Stopwatch]::Frequency)
        while ([Diagnostics.Stopwatch]::GetTimestamp() -lt $keepAliveDeadline) {
            Start-Sleep -Milliseconds 250
        }
        Emit-Diagnostic 'keepalive_done' -Count $keepAliveSeconds
    }

    Emit-Diagnostic 'shutdown_enter'
    $stopped = Stop-WebProcess $script:MemberProcess
    if ($stopped) { $script:MemberProcess = $null }
    $script:CurrentPhase = 'member_reopen_membership'
    Emit-Diagnostic 'reopen_enter'
    if (-not $stopped -or $script:ForcedTermination) { throw 'shutdown' }
    $memberPort = Get-FreeTcpPort
    $restartedProcess = Start-WebProcess $artifact $data $memberPort $profile
    if ($null -ne $restartedProcess) { $script:MemberProcess = $restartedProcess }
    $reopened = $null -ne $script:MemberProcess
    Emit-Diagnostic 'reopen_login_enter'
    $reopenClient = if ($reopened) { New-ApiClient "http://127.0.0.1:$memberPort" $data } else { $null }
    Emit-Diagnostic 'reopen_login_done' -Count $(if ($null -ne $reopenClient) { $script:LastApiStatus } else { 0 })
    Emit-Diagnostic 'reopen_membership_enter'
    $membership = if ($null -ne $reopenClient) { Invoke-JsonApi $reopenClient Get "/api/v1/shares/$shareId" } else { $null }
    Emit-Diagnostic 'reopen_membership_done' -Count $script:LastApiStatus
    $afterHash = ''
    if (Test-Path -LiteralPath $target -PathType Leaf) {
        try { $afterHash = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant() } catch { }
    }
    $reopenLoginOk = $reopened -and $null -ne $reopenClient
    $membershipStatusOk = $script:LastApiStatus -eq 200
    $membershipShareOk = $membershipStatusOk -and $null -ne $membership -and [string]$membership.share_id -eq $shareId
    $membershipRoleOk = $membershipShareOk -and [string]$membership.role -eq 'member'
    $membershipPermissionOk = $membershipRoleOk -and [string]$membership.permission -eq [string]$config.expected_permission
    $fileHashOk = $afterHash -eq [string]$config.expected_file_hash
    $reopenChecks = [int]$reopenLoginOk + (2 * [int]$membershipStatusOk) + (4 * [int]$membershipShareOk) + (8 * [int]$fileHashOk) + (16 * [int]$membershipRoleOk) + (32 * [int]$membershipPermissionOk)
    Emit-Diagnostic 'reopen_checks' -Count $reopenChecks
    $reopenOk = $reopenLoginOk -and $membershipPermissionOk -and $fileHashOk
    $afterSize = if ($reopenOk) { (Get-Item -LiteralPath $target).Length } else { -1 }
    Emit-Phase 'member_reopen_membership' $reopenOk $afterHash $afterSize
    if (-not $reopenOk) { throw 'reopen' }
    $script:AllPassed = $true
} catch {
    Emit-Phase $script:CurrentPhase $false -Forced:$script:ForcedTermination -ErrorClass 'remote_failure'
} finally {
    Emit-Diagnostic 'cleanup_enter'
    $stoppedFinal = Stop-WebProcess $script:MemberProcess
    if ($stoppedFinal) { $script:MemberProcess = $null }
    $clean = $false
    # No managed pause/revoke drain acknowledgement is exposed by this
    # subset.  Retain the run namespace when the acknowledgement is unknown,
    # on a forced stop, or when the process did not stop cleanly.
    if ($stoppedFinal -and $script:AllPassed -and $script:GracefulDrainProven -and -not $script:ForcedTermination -and $null -ne $script:Base) {
        try {
            if ($script:DestinationCreated -and (Test-Path -LiteralPath $config.destination_root)) {
                Remove-Item -LiteralPath $config.destination_root -Recurse -Force -ErrorAction Stop
            }
            Remove-Item -LiteralPath $script:Base -Recurse -Force -ErrorAction Stop
            $clean = $true
        } catch { $clean = $false }
    }
    $script:CleanupPassed = $stoppedFinal -and $clean -and $script:GracefulDrainProven -and -not $script:ForcedTermination
    Emit-Phase 'cleanup' $script:CleanupPassed -Forced:$script:ForcedTermination -Signal $(if ($script:GracefulDrainProven) { 'ctrl_c' } else { 'none' }) -ErrorClass $(if ($script:CleanupPassed) { '' } else { 'cleanup_incomplete' })
}

if (-not $script:AllPassed -or -not $script:CleanupPassed -or $script:ForcedTermination) { exit 1 }
exit 0
