$ErrorActionPreference = "Stop"

$repository = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$runRoot = $null
$daemon = $null
$acpProcess = $null
$harness = $null
$filesystem = $null
$scheduler = $null
$testStartedAt = Get-Date
Push-Location $repository
try {
    cargo build -p agentmod-runtime -p agentmod-harness `
        -p agentmod-filesystem-host -p agentmod-cli -p agentmod-acp
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    $runtime = (Resolve-Path "target\debug\agentmod-runtime.exe").Path
    $harness = (Resolve-Path "target\debug\agentmod-harness.exe").Path
    $cli = (Resolve-Path "target\debug\agentmod.exe").Path
    $acp = (Resolve-Path "target\debug\agentmod-acp.exe").Path
    $filesystem = (
        Resolve-Path "target\debug\agentmod-filesystem-host.exe"
    ).Path
    $scheduler = (Resolve-Path "target\debug\agentmod-scheduler.exe").Path
    $runRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
        "agentmod-acp-permission-e2e-" + [guid]::NewGuid().ToString("N")
    )
    New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
    $env:AGENTMOD_RUNTIME_ENDPOINT = (
        "\\.\pipe\agentmod-acp-permission-e2e-" +
        [guid]::NewGuid().ToString("N")
    )
    $env:AGENTMOD_RUNTIME_AUTH_TOKEN = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    $env:AGENTMOD_HARNESS_PROGRAM = $harness
    $env:AGENTMOD_FILESYSTEM_HOST_PROGRAM = $filesystem
    $env:AGENTMOD_SCHEDULER_POLL_MS = "0"
    $env:AGENTMOD_ACP_PROVIDER_OPTIONS = (
        '{"mock_scenario":"approval_write"}'
    )
    Remove-Item Env:AGENTMOD_PERMISSION_MODE -ErrorAction SilentlyContinue
    $daemon = Start-Process -FilePath $runtime -ArgumentList "serve" `
        -WorkingDirectory $runRoot -WindowStyle Hidden -PassThru
    $ready = $false
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        try {
            & $cli doctor --json 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) {
                $ready = $true
                break
            }
        } catch {}
        Start-Sleep -Milliseconds 100
    }
    if (-not $ready) { throw "runtime did not become ready" }

    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $acp
    $start.WorkingDirectory = $runRoot
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardInput = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $acpProcess = [System.Diagnostics.Process]::new()
    $acpProcess.StartInfo = $start
    if (-not $acpProcess.Start()) { throw "ACP process did not start" }

    function Send-Acp([hashtable]$message) {
        $acpProcess.StandardInput.WriteLine(
            ($message | ConvertTo-Json -Compress -Depth 30)
        )
        $acpProcess.StandardInput.Flush()
    }
    function Read-Acp {
        $line = $acpProcess.StandardOutput.ReadLine()
        if ($null -eq $line) {
            throw "ACP closed: " + $acpProcess.StandardError.ReadToEnd()
        }
        return $line | ConvertFrom-Json
    }

    Send-Acp @{
        jsonrpc = "2.0"; id = 1; method = "initialize"
        params = @{ protocolVersion = 1; clientCapabilities = @{} }
    }
    if ((Read-Acp).id -ne 1) { throw "ACP initialization failed" }

    function Invoke-PermissionScenario(
        [int]$baseId,
        [string]$decision,
        [bool]$cancelled
    ) {
        $workspace = Join-Path $runRoot ("workspace-" + $baseId)
        New-Item -ItemType Directory -Path $workspace -Force | Out-Null
        Send-Acp @{
            jsonrpc = "2.0"; id = $baseId; method = "session/new"
            params = @{ cwd = $workspace; mcpServers = @() }
        }
        $created = Read-Acp
        if ($created.id -ne $baseId) { throw "session creation failed" }
        $sessionId = $created.result.sessionId
        $promptId = $baseId + 1
        Send-Acp @{
            jsonrpc = "2.0"; id = $promptId; method = "session/prompt"
            params = @{
                sessionId = $sessionId
                prompt = @(@{ type = "text"; text = "permission scenario" })
            }
        }
        $sawTool = $false
        $sawPermission = $false
        $stopReason = $null
        for ($frame = 0; $frame -lt 30; $frame++) {
            $message = Read-Acp
            if ($message.method -eq "session/update" -and
                $message.params.update.sessionUpdate -eq "tool_call") {
                $sawTool = $true
            }
            if ($message.method -eq "session/request_permission") {
                $sawPermission = $true
                if ($cancelled) {
                    Send-Acp @{
                        jsonrpc = "2.0"; method = "session/cancel"
                        params = @{ sessionId = $sessionId }
                    }
                    Send-Acp @{
                        jsonrpc = "2.0"; id = $message.id
                        result = @{ outcome = @{ outcome = "cancelled" } }
                    }
                } else {
                    Send-Acp @{
                        jsonrpc = "2.0"; id = $message.id
                        result = @{
                            outcome = @{
                                outcome = "selected"
                                optionId = $decision
                            }
                        }
                    }
                }
            }
            if ($message.id -eq $promptId) {
                $stopReason = $message.result.stopReason
                break
            }
        }
        if (-not $sawTool -or -not $sawPermission) {
            throw "ACP permission flow omitted tool or permission request"
        }
        $expectedStop = if ($cancelled) { "cancelled" } else { "end_turn" }
        if ($stopReason -ne $expectedStop) {
            throw "ACP permission flow returned $stopReason, expected $expectedStop"
        }
        return @{
            SessionId = $sessionId
            Workspace = $workspace
        }
    }

    $allowed = Invoke-PermissionScenario 10 "allow-once" $false
    $allowedFile = Join-Path $allowed.Workspace "approved.txt"
    if (-not (Test-Path -LiteralPath $allowedFile)) {
        $allowedJournal = Join-Path $runRoot (
            "sessions\" + $allowed.SessionId + "\events.jsonl"
        )
        $allowedTypes = @(Get-Content -LiteralPath $allowedJournal |
            ForEach-Object { ($_ | ConvertFrom-Json).event.metadata.event_type })
        throw "approved ACP tool did not execute; events=$($allowedTypes -join ',')"
    }
    $allowedContent = Get-Content -LiteralPath $allowedFile -Raw
    if ($allowedContent.TrimEnd("`r", "`n") -ne "executed once") {
        throw "approved ACP tool wrote unexpected content"
    }

    $denied = Invoke-PermissionScenario 20 "reject-once" $false
    if (Test-Path -LiteralPath (Join-Path $denied.Workspace "approved.txt")) {
        throw "denied ACP tool executed"
    }

    $cancelled = Invoke-PermissionScenario 30 "reject-once" $true
    if (Test-Path -LiteralPath (Join-Path $cancelled.Workspace "approved.txt")) {
        throw "cancelled ACP approval executed"
    }
    $journal = Join-Path $runRoot (
        "sessions\" + $cancelled.SessionId + "\events.jsonl"
    )
    $eventTypes = @(Get-Content -LiteralPath $journal | ForEach-Object {
        ($_ | ConvertFrom-Json).event.metadata.event_type
    })
    if (@($eventTypes | Where-Object { $_ -eq "model.request_started" }).Count -ne 1) {
        throw "approval cancellation started a replacement provider request"
    }
    if (@($eventTypes | Where-Object { $_ -eq "model.request_cancelled" }).Count -ne 1) {
        throw "approval cancellation did not commit exactly one cancellation"
    }
    if ($eventTypes | Where-Object { $_ -eq "model.response_completed" }) {
        throw "approval cancellation committed provider completion"
    }

    $acpProcess.StandardInput.Close()
    if (-not $acpProcess.WaitForExit(5000) -or $acpProcess.ExitCode -ne 0) {
        throw "ACP did not shut down cleanly"
    }
    Write-Output "runtime ACP permission E2E passed"
}
finally {
    if ($null -ne $acpProcess -and -not $acpProcess.HasExited) {
        $acpProcess.Kill()
        $acpProcess.WaitForExit()
    }
    if ($null -ne $daemon -and -not $daemon.HasExited) {
        Stop-Process -Id $daemon.Id -Force
        $daemon.WaitForExit()
    }
    Get-Process -ErrorAction SilentlyContinue | Where-Object {
        $_.StartTime -ge $testStartedAt -and
        $_.Path -in @($harness, $filesystem, $scheduler)
    } | Stop-Process -Force
    Remove-Item Env:AGENTMOD_ACP_PROVIDER_OPTIONS -ErrorAction SilentlyContinue
    Pop-Location
    if ($null -ne $runRoot -and (Test-Path -LiteralPath $runRoot)) {
        $resolved = [System.IO.Path]::GetFullPath($runRoot)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp) -or
            -not (Split-Path $resolved -Leaf).StartsWith(
                "agentmod-acp-permission-e2e-"
            )) {
            throw "refusing to remove non-AgentMod temporary path"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
