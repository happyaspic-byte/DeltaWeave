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

function Emit-Phase {
    param(
        [Parameter(Mandatory = $true)][string]$Phase,
        [Parameter(Mandatory = $true)][bool]$Ok,
        [string]$Hash = '',
        [long]$Size = -1,
        [bool]$Forced = $false,
        [string]$ErrorClass = ''
    )
    $value = if ($Ok) { 'true' } else { 'false' }
    $line = "FROLE|phase=$Phase|ok=$value"
    if ($Hash -match '^[0-9a-f]{64}$') { $line += "|hash=$Hash" }
    if ($Size -ge 0) { $line += "|size=$Size" }
    if ($Forced) { $line += '|forced=true' }
    if (-not $Ok -and $ErrorClass -match '^[a-z0-9_]+$') { $line += "|error_class=$ErrorClass" }
    Write-Output $line
}

function Decode-Config {
    param([string]$Encoded)
    try {
        $bytes = [Convert]::FromBase64String($Encoded)
        $json = [Text.Encoding]::UTF8.GetString($bytes)
        $value = $json | ConvertFrom-Json
        if ($null -eq $value) { throw 'config' }
        return $value
    } catch {
        throw 'config'
    }
}

function Assert-Config {
    param($Value)
    $required = @('artifact_url', 'artifact_sha256', 'artifact_size', 'owner_base_uri', 'share_key', 'destination_root', 'expected_file_hash', 'expected_file_name')
    foreach ($name in $required) {
        $item = [string]$Value.$name
        if ([string]::IsNullOrWhiteSpace($item)) { throw 'config' }
    }
    if ([string]$Value.artifact_sha256 -notmatch '^[0-9a-f]{64}$') { throw 'config' }
    if ([string]$Value.artifact_size -notmatch '^[1-9][0-9]*$') { throw 'config' }
    if ([string]$Value.expected_file_hash -notmatch '^[0-9a-f]{64}$') { throw 'config' }
    if ([string]$Value.destination_root -notmatch '^[A-Za-z]:\\[^\x00\r\n]+$') { throw 'config' }
    if ([string]$Value.expected_file_name -notmatch '^[A-Za-z0-9._-]{1,128}$') { throw 'config' }
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

function New-ProcessInfo {
    param([string]$Executable, [string]$Arguments, [string]$WorkingDirectory, [string]$Profile)
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $Executable
    $info.Arguments = $Arguments
    $info.WorkingDirectory = $WorkingDirectory
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
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

function Invoke-BinarySelfTest {
    param([string]$Executable, [string]$Profile)
    $info = New-ProcessInfo $Executable 'self-test' $script:Base $Profile
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $info
    if (-not $process.Start()) { return $false }
    # Never inherit the child streams into the WinRM host output.  Discard
    # them asynchronously so a noisy binary cannot block on a full pipe.
    $process.add_OutputDataReceived({ param($sender, $eventArgs) })
    $process.add_ErrorDataReceived({ param($sender, $eventArgs) })
    $process.BeginOutputReadLine()
    $process.BeginErrorReadLine()
    if (-not $process.WaitForExit(60000)) {
        try { $process.Kill() } catch { }
        return $false
    }
    return $process.ExitCode -eq 0
}

function Start-WebProcess {
    param([string]$Executable, [string]$DataDirectory, [int]$Port, [string]$Profile)
    $arguments = ('web --bind "127.0.0.1:{0}" --data-dir "{1}"' -f $Port, $DataDirectory)
    $info = New-ProcessInfo $Executable $arguments $script:Base $Profile
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $info
    if (-not $process.Start()) { return $null }
    # Keep application stdout/stderr out of the remoting transcript and drain
    # both redirected pipes while the long-running web process is alive.
    $process.add_OutputDataReceived({ param($sender, $eventArgs) })
    $process.add_ErrorDataReceived({ param($sender, $eventArgs) })
    $process.BeginOutputReadLine()
    $process.BeginErrorReadLine()
    for ($attempt = 0; $attempt -lt 150; $attempt++) {
        if ($process.HasExited) { return $null }
        if (Test-Path -LiteralPath (Join-Path $DataDirectory 'admin-token')) {
            try {
                $null = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/" -TimeoutSec 2
                return $process
            } catch { }
        }
        Start-Sleep -Milliseconds 200
    }
    try { if (-not $process.HasExited) { $process.Kill(); $process.WaitForExit(5000) } } catch { }
    return $null
}

function Stop-WebProcess {
    param($Process)
    if ($null -eq $Process) { return $true }
    try {
        if (-not $Process.HasExited) {
            $Process.CloseMainWindow() | Out-Null
            if (-not $Process.WaitForExit(30000)) {
                $script:ForcedTermination = $true
                $Process.Kill()
                if (-not $Process.WaitForExit(5000)) { return $false }
            }
        }
        return $Process.HasExited
    } catch { return $false }
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
        if ($null -ne $Body) {
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

    $script:CurrentPhase = 'self_test'
    $selfTestOk = Invoke-BinarySelfTest $artifact $profile
    Emit-Phase 'self_test' $selfTestOk
    if (-not $selfTestOk) { throw 'self_test' }

    $script:CurrentPhase = 'member_web_start'
    $memberPort = Get-FreeTcpPort
    $script:MemberProcess = Start-WebProcess $artifact $data $memberPort $profile
    $memberStarted = $null -ne $script:MemberProcess
    Emit-Phase 'member_web_start' $memberStarted
    if (-not $memberStarted) { throw 'web_start' }
    $member = New-ApiClient "http://127.0.0.1:$memberPort" $data
    $script:CurrentPhase = 'member_login'
    $loggedIn = $null -ne $member
    Emit-Phase 'member_login' $loggedIn
    if (-not $loggedIn) { throw 'login' }

    $shareKey = [string]$config.share_key
    $previewBody = @{ request_id = 'qsync-f-winrm-preview'; key = $shareKey } | ConvertTo-Json -Compress
    $script:CurrentPhase = 'local_preview'
    $preview = Invoke-JsonApi $member Post '/api/v1/shares/preview' $previewBody -Mutation
    $previewOk = $script:LastApiStatus -eq 200 -and $null -ne $preview -and $preview.signature_valid -eq $true
    Emit-Phase 'local_preview' $previewOk
    if (-not $previewOk) { throw 'preview' }

    $validateBody = @{ request_id = 'qsync-f-winrm-validate'; key = $shareKey } | ConvertTo-Json -Compress
    $script:CurrentPhase = 'online_validate'
    $validated = Invoke-JsonApi $member Post '/api/v1/shares/validate' $validateBody -Mutation
    $validateOk = $script:LastApiStatus -eq 200 -and $null -ne $validated -and $validated.signature_valid -eq $true
    Emit-Phase 'online_validate' $validateOk
    if (-not $validateOk) { throw 'validate' }

    $joinBody = @{ request_id = 'qsync-f-winrm-join'; key = $shareKey; destination_root = $destination } | ConvertTo-Json -Compress
    $script:CurrentPhase = 'member_join'
    $joined = Invoke-JsonApi $member Post '/api/v1/shares/join' $joinBody -Mutation
    $shareId = if ($null -ne $joined) { [string]$joined.share_id } else { '' }
    $joinOk = ($script:LastApiStatus -eq 200 -or $script:LastApiStatus -eq 202) -and $shareId -match '^[0-9a-f]{64}$'
    Emit-Phase 'member_join' $joinOk
    if (-not $joinOk) { throw 'join' }

    $target = Join-Path $destination ([string]$config.expected_file_name)
    $script:CurrentPhase = 'file_hash'
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

    $stopped = Stop-WebProcess $script:MemberProcess
    if ($stopped) { $script:MemberProcess = $null }
    $script:CurrentPhase = 'member_reopen_membership'
    if (-not $stopped -or $script:ForcedTermination) { throw 'shutdown' }
    $memberPort = Get-FreeTcpPort
    $script:MemberProcess = Start-WebProcess $artifact $data $memberPort $profile
    $reopened = $null -ne $script:MemberProcess
    $reopenClient = if ($reopened) { New-ApiClient "http://127.0.0.1:$memberPort" $data } else { $null }
    $membership = if ($null -ne $reopenClient) { Invoke-JsonApi $reopenClient Get "/api/v1/shares/$shareId" } else { $null }
    $afterHash = ''
    if (Test-Path -LiteralPath $target -PathType Leaf) {
        try { $afterHash = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant() } catch { }
    }
    $reopenOk = $reopened -and $null -ne $reopenClient -and $null -ne $membership -and $afterHash -eq [string]$config.expected_file_hash
    $afterSize = if ($reopenOk) { (Get-Item -LiteralPath $target).Length } else { -1 }
    Emit-Phase 'member_reopen_membership' $reopenOk $afterHash $afterSize
    if (-not $reopenOk) { throw 'reopen' }
    $script:AllPassed = $true
} catch {
    Emit-Phase $script:CurrentPhase $false -Forced:$script:ForcedTermination -ErrorClass 'remote_failure'
} finally {
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
    Emit-Phase 'cleanup' $script:CleanupPassed -Forced:$script:ForcedTermination -ErrorClass $(if ($script:CleanupPassed) { '' } else { 'cleanup_incomplete' })
}

if (-not $script:AllPassed -or -not $script:CleanupPassed -or $script:ForcedTermination) { exit 1 }
exit 0
